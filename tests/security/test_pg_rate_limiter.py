"""Cleanup Item 4: Postgres-backed rate limiter unit tests.

The production path runs psycopg2; tests wire a SQLite connection
via the dialect='sqlite' shim so the full API surface is exercised
without needing a live Postgres service.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.security.pg_rate_limiter import (  # noqa: E402
    PostgresRateLimiter, get_rate_limiter_backend,
    reset_singleton_for_tests,
)


def _make_limiter(tmp_path):
    db_file = tmp_path / 'ratelimit.db'
    # WAL + immediate mode so concurrent threads don't deadlock on
    # the default rollback journal.
    def _connect():
        conn = sqlite3.connect(str(db_file), timeout=10,
                                 isolation_level=None)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=5000')
        return conn
    return PostgresRateLimiter(connect_fn=_connect, dialect='sqlite')


def test_within_limit_allows(tmp_path):
    lim = _make_limiter(tmp_path)
    for i in range(5):
        r = lim.check_and_increment(key='u:1', window_seconds=60, max_count=5)
        assert r['allowed'] is True
        assert r['count'] == i + 1
        assert r['limit'] == 5


def test_over_limit_blocks(tmp_path):
    lim = _make_limiter(tmp_path)
    for _ in range(5):
        lim.check_and_increment(key='u:2', window_seconds=60, max_count=5)
    r = lim.check_and_increment(key='u:2', window_seconds=60, max_count=5)
    assert r['allowed'] is False
    assert r['count'] == 5   # unchanged — insertion skipped


def test_reset_at_computed(tmp_path):
    lim = _make_limiter(tmp_path)
    r = lim.check_and_increment(key='u:3', window_seconds=60, max_count=5)
    assert isinstance(r['reset_at'], datetime)
    # reset_at should be within 60s of "now"
    now = datetime.now(timezone.utc)
    assert abs((r['reset_at'] - now).total_seconds()) <= 120


def test_distinct_keys_isolated(tmp_path):
    lim = _make_limiter(tmp_path)
    for _ in range(5):
        lim.check_and_increment(key='u:a', window_seconds=60, max_count=5)
    # u:a is at limit; u:b independent.
    r_a = lim.check_and_increment(key='u:a', window_seconds=60, max_count=5)
    r_b = lim.check_and_increment(key='u:b', window_seconds=60, max_count=5)
    assert r_a['allowed'] is False
    assert r_b['allowed'] is True


def test_window_expires(tmp_path):
    """A row inserted before the window should not count. We simulate
    the 'older' row by manipulating created_at directly."""
    lim = _make_limiter(tmp_path)
    lim.ensure_schema()
    # Seed a row that's 2 hours old (well outside any 60s window).
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    conn = sqlite3.connect(str(tmp_path / 'ratelimit.db'))
    conn.execute(
        "INSERT INTO rate_limit_events (key, created_at) VALUES (?, ?)",
        ('u:4', old),
    )
    conn.commit()
    conn.close()
    # Window=60s: the stale row shouldn't count.
    r = lim.check_and_increment(key='u:4', window_seconds=60, max_count=1)
    assert r['allowed'] is True
    assert r['count'] == 1  # only the fresh row


def test_cleanup_removes_old_events(tmp_path):
    lim = _make_limiter(tmp_path)
    lim.ensure_schema()
    now = datetime.now(timezone.utc)
    conn = sqlite3.connect(str(tmp_path / 'ratelimit.db'))
    conn.execute(
        "INSERT INTO rate_limit_events (key, created_at) VALUES (?, ?)",
        ('u:old', (now - timedelta(hours=2)).isoformat()),
    )
    conn.execute(
        "INSERT INTO rate_limit_events (key, created_at) VALUES (?, ?)",
        ('u:fresh', now.isoformat()),
    )
    conn.commit()
    conn.close()
    deleted = lim.cleanup_old_events(older_than_seconds=3600)
    assert deleted == 1
    conn = sqlite3.connect(str(tmp_path / 'ratelimit.db'))
    remaining = {r[0] for r in conn.execute(
        "SELECT key FROM rate_limit_events"
    ).fetchall()}
    conn.close()
    assert remaining == {'u:fresh'}


def test_concurrent_requests_correctly_counted(tmp_path):
    """Eight threads, limit=5 → exactly 5 allowed, 3 blocked."""
    lim = _make_limiter(tmp_path)
    lim.ensure_schema()
    results: list[bool] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def hit():
        barrier.wait()
        r = lim.check_and_increment(key='u:race', window_seconds=60,
                                     max_count=5)
        with lock:
            results.append(r['allowed'])

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    allowed = sum(1 for r in results if r)
    blocked = sum(1 for r in results if not r)
    assert allowed == 5
    assert blocked == 3


def test_backend_env_default_is_memory(monkeypatch):
    monkeypatch.delenv('RATE_LIMITER_BACKEND', raising=False)
    reset_singleton_for_tests()
    assert get_rate_limiter_backend() == 'memory'


def test_backend_env_postgres(monkeypatch):
    monkeypatch.setenv('RATE_LIMITER_BACKEND', 'postgres')
    assert get_rate_limiter_backend() == 'postgres'


def test_backend_env_dual(monkeypatch):
    monkeypatch.setenv('RATE_LIMITER_BACKEND', 'dual')
    assert get_rate_limiter_backend() == 'dual'


def test_backend_env_unknown_falls_back_to_memory(monkeypatch):
    monkeypatch.setenv('RATE_LIMITER_BACKEND', 'sqlite3-by-candlelight')
    assert get_rate_limiter_backend() == 'memory'


def test_unknown_dialect_raises():
    with pytest.raises(ValueError):
        PostgresRateLimiter(connect_fn=lambda: None, dialect='mysql')


def test_ensure_schema_idempotent(tmp_path):
    lim = _make_limiter(tmp_path)
    lim.ensure_schema()
    lim.ensure_schema()
    lim.ensure_schema()
    # Table exists, no errors.
    conn = sqlite3.connect(str(tmp_path / 'ratelimit.db'))
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='rate_limit_events'"
    ).fetchone()
    conn.close()
    assert row is not None

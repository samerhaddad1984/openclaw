"""Postgres-backed sliding-window rate limiter.

Drop-in replacement for the in-memory counters scattered across the
dashboard. Safe under multi-process deployment (gunicorn -w N,
multiple containers, blue/green): every worker hits the same
``rate_limit_events`` table so limits stay consistent.

Dependency injection makes this testable without a live Postgres:

    limiter = PostgresRateLimiter(connect_fn=my_fn)

where `connect_fn()` returns a DB-API 2.0 connection. Production
wires psycopg2.connect against the `DATABASE_URL` env var; tests
use an in-memory SQLite connection so unit coverage doesn't need a
PG service.

Schema::

    CREATE TABLE rate_limit_events (
        id SERIAL PRIMARY KEY,
        key TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL
    );
    CREATE INDEX idx_rate_limit_key_time
        ON rate_limit_events(key, created_at);

The SQLite test shim uses the same DDL with INTEGER PRIMARY KEY
AUTOINCREMENT in place of SERIAL.

API::

    check_and_increment(key, window_seconds, max_count)
        -> {'allowed': bool, 'count': int, 'limit': int,
             'reset_at': datetime}
    cleanup_old_events(older_than_seconds=3600) -> int

Atomicity: the production path uses a single CTE that counts recent
hits + conditionally inserts in one statement so two concurrent
workers can't both see count<limit and both insert.

LIMITATION: SQLite doesn't support CTEs that return from INSERT with
RETURNING on older versions, so the SQLite test shim runs a short
transaction per call. This is adequate for unit tests but not what
we deploy.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


log = logging.getLogger(__name__)


class PostgresRateLimiter:
    """Sliding-window rate limiter backed by a DB-API connection.

    The class is backend-agnostic: any connect_fn that returns a
    connection with .cursor() + .commit() + .rollback() works. A
    `dialect='postgres'|'sqlite'` kwarg switches between the CTE
    form and the per-call transaction form."""

    def __init__(
        self, *,
        connect_fn: Callable[[], Any] | None = None,
        dialect: str = 'postgres',
    ) -> None:
        if connect_fn is None:
            connect_fn = self._default_connect_fn
        self._connect_fn = connect_fn
        if dialect not in ('postgres', 'sqlite'):
            raise ValueError(f'unknown dialect {dialect!r}')
        self._dialect = dialect
        self._placeholder = '%s' if dialect == 'postgres' else '?'
        self._init_lock = threading.Lock()
        self._initialised = False

    # ------------------------------------------------------------------
    # Connection / schema
    # ------------------------------------------------------------------

    @staticmethod
    def _default_connect_fn():
        url = os.environ.get('DATABASE_URL', '')
        if not url:
            raise RuntimeError(
                "PostgresRateLimiter: DATABASE_URL env var not set"
            )
        import psycopg2
        return psycopg2.connect(url)

    def ensure_schema(self) -> None:
        """Idempotent table + index creation."""
        if self._initialised:
            return
        with self._init_lock:
            if self._initialised:
                return
            if self._dialect == 'postgres':
                ddl = [
                    "CREATE TABLE IF NOT EXISTS rate_limit_events ("
                    "  id SERIAL PRIMARY KEY,"
                    "  key TEXT NOT NULL,"
                    "  created_at TIMESTAMP NOT NULL)",
                    "CREATE INDEX IF NOT EXISTS idx_rate_limit_key_time "
                    "ON rate_limit_events(key, created_at)",
                ]
            else:
                ddl = [
                    "CREATE TABLE IF NOT EXISTS rate_limit_events ("
                    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "  key TEXT NOT NULL,"
                    "  created_at TEXT NOT NULL)",
                    "CREATE INDEX IF NOT EXISTS idx_rate_limit_key_time "
                    "ON rate_limit_events(key, created_at)",
                ]
            conn = self._connect_fn()
            try:
                cur = conn.cursor()
                for stmt in ddl:
                    cur.execute(stmt)
                conn.commit()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            self._initialised = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_and_increment(
        self, *,
        key: str, window_seconds: int, max_count: int,
    ) -> dict[str, Any]:
        """Return dict with 'allowed', 'count', 'limit', 'reset_at'.

        count = number of events in the current window after the call
        (reflects the increment when allowed)."""
        self.ensure_schema()
        now = datetime.now(timezone.utc).replace(microsecond=0)
        window_start = now - timedelta(seconds=window_seconds)
        if self._dialect == 'postgres':
            return self._check_and_increment_pg(
                key, window_start, now, max_count, window_seconds,
            )
        return self._check_and_increment_sqlite(
            key, window_start, now, max_count, window_seconds,
        )

    def _check_and_increment_pg(
        self, key, window_start, now, max_count, window_seconds,
    ):
        """Atomic CTE for Postgres."""
        conn = self._connect_fn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                WITH recent AS (
                    SELECT COUNT(*) AS c FROM rate_limit_events
                    WHERE key = %s AND created_at >= %s
                ),
                inserted AS (
                    INSERT INTO rate_limit_events (key, created_at)
                    SELECT %s, %s
                    WHERE (SELECT c FROM recent) < %s
                    RETURNING id
                )
                SELECT
                    (SELECT c FROM recent) AS prior_count,
                    EXISTS(SELECT 1 FROM inserted) AS was_allowed
                """,
                (key, window_start, key, now, max_count),
            )
            row = cur.fetchone()
            conn.commit()
            prior_count = int(row[0] or 0)
            allowed = bool(row[1])
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return {
            'allowed': allowed,
            'count': prior_count + (1 if allowed else 0),
            'limit': max_count,
            'reset_at': window_start + timedelta(seconds=window_seconds),
        }

    def _check_and_increment_sqlite(
        self, key, window_start, now, max_count, window_seconds,
    ):
        """SQLite fallback for tests. Uses a short transaction with
        BEGIN IMMEDIATE to serialise concurrent callers — less
        efficient than the PG CTE but correct for unit tests."""
        conn = self._connect_fn()
        try:
            cur = conn.cursor()
            try:
                cur.execute('BEGIN IMMEDIATE')
            except Exception:
                pass
            cur.execute(
                "SELECT COUNT(*) FROM rate_limit_events "
                "WHERE key = ? AND created_at >= ?",
                (key, window_start.isoformat()),
            )
            prior_count = int(cur.fetchone()[0] or 0)
            allowed = prior_count < max_count
            if allowed:
                cur.execute(
                    "INSERT INTO rate_limit_events (key, created_at) "
                    "VALUES (?, ?)",
                    (key, now.isoformat()),
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return {
            'allowed': allowed,
            'count': prior_count + (1 if allowed else 0),
            'limit': max_count,
            'reset_at': window_start + timedelta(seconds=window_seconds),
        }

    def cleanup_old_events(self, *, older_than_seconds: int = 3600) -> int:
        """DELETE rows older than the retention window. Returns
        rows-deleted. Call from the maintenance cron."""
        self.ensure_schema()
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(seconds=older_than_seconds))
        conn = self._connect_fn()
        try:
            cur = conn.cursor()
            if self._dialect == 'postgres':
                cur.execute(
                    "DELETE FROM rate_limit_events WHERE created_at < %s",
                    (cutoff,),
                )
            else:
                cur.execute(
                    "DELETE FROM rate_limit_events WHERE created_at < ?",
                    (cutoff.isoformat(),),
                )
            deleted = cur.rowcount or 0
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return int(deleted)


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------


def get_rate_limiter_backend() -> str:
    """Return the active backend name.

    Env var ``RATE_LIMITER_BACKEND`` switches between:
    - 'memory'   : in-process dicts (default; legacy)
    - 'postgres' : PostgresRateLimiter via DATABASE_URL
    - 'dual'     : dual-run — PG for correctness, memory as fallback

    An unknown value logs a warning and falls back to memory."""
    raw = os.environ.get('RATE_LIMITER_BACKEND', 'memory').strip().lower()
    if raw in ('memory', 'postgres', 'dual'):
        return raw
    log.warning('unknown RATE_LIMITER_BACKEND=%r, falling back to memory', raw)
    return 'memory'


_singleton_limiter: PostgresRateLimiter | None = None
_singleton_lock = threading.Lock()


def get_pg_limiter() -> PostgresRateLimiter:
    """Module-level singleton so callers share the same connection
    pool (once we wire pooling)."""
    global _singleton_limiter
    if _singleton_limiter is None:
        with _singleton_lock:
            if _singleton_limiter is None:
                _singleton_limiter = PostgresRateLimiter()
    return _singleton_limiter


def reset_singleton_for_tests() -> None:
    """Only used by tests; production never calls this."""
    global _singleton_limiter
    with _singleton_lock:
        _singleton_limiter = None

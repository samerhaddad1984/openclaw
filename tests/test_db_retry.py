"""Part 1 Bug C — WAL + retry wrapper tests."""
from __future__ import annotations

import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db.retry import (  # noqa: E402
    DEFAULT_MAX_RETRIES,
    enable_wal_mode,
    retry_on_lock,
)


def test_wal_mode_can_be_enabled(tmp_path):
    db = tmp_path / "wal.db"
    sqlite3.connect(str(db)).close()  # create
    mode = enable_wal_mode(db)
    assert mode == "wal"


def test_wal_mode_idempotent(tmp_path):
    db = tmp_path / "wal.db"
    sqlite3.connect(str(db)).close()
    enable_wal_mode(db)
    mode = enable_wal_mode(db)
    assert mode == "wal"


def test_retry_succeeds_on_recovered_lock():
    """Function fails twice then succeeds; wrapper must absorb the failures."""
    call_state = {"n": 0}

    @retry_on_lock(max_retries=3, initial_backoff=0.001)
    def flaky():
        call_state["n"] += 1
        if call_state["n"] <= 2:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    assert flaky() == "ok"
    assert call_state["n"] == 3


def test_retry_gives_up_after_max():
    @retry_on_lock(max_retries=2, initial_backoff=0.001)
    def always_locked():
        raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError):
        always_locked()


def test_retry_does_not_catch_non_lock_errors():
    @retry_on_lock(max_retries=3, initial_backoff=0.001)
    def bad():
        raise sqlite3.OperationalError("syntax error")

    # Should NOT retry — only lock errors are retried.
    call_state = {"n": 0}
    @retry_on_lock(max_retries=3, initial_backoff=0.001)
    def counting_bad():
        call_state["n"] += 1
        raise sqlite3.OperationalError("syntax error")
    with pytest.raises(sqlite3.OperationalError):
        counting_bad()
    assert call_state["n"] == 1


def test_retry_catches_busy_as_well_as_locked():
    call_state = {"n": 0}

    @retry_on_lock(max_retries=3, initial_backoff=0.001)
    def flaky_busy():
        call_state["n"] += 1
        if call_state["n"] == 1:
            raise sqlite3.OperationalError("database is busy")
        return "ok"

    assert flaky_busy() == "ok"
    assert call_state["n"] == 2


def test_concurrent_writes_with_wal_and_retry(tmp_path):
    """5 threads each insert 20 rows. With WAL + retry, all 100 rows land
    and no thread raises.
    """
    db = tmp_path / "conc.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, who TEXT, n INTEGER)")
    conn.commit()
    conn.close()
    enable_wal_mode(db)

    @retry_on_lock(max_retries=10, initial_backoff=0.01)
    def _insert(who: str, n: int):
        c = sqlite3.connect(str(db), timeout=3)
        c.execute("PRAGMA busy_timeout = 2000")
        c.execute("INSERT INTO t (who, n) VALUES (?, ?)", (who, n))
        c.commit()
        c.close()

    errs = []

    def worker(label):
        for i in range(20):
            try:
                _insert(label, i)
            except Exception as e:
                errs.append((label, str(e)))

    threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()

    conn = sqlite3.connect(str(db))
    count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    conn.close()
    assert count == 100, f"expected 100 rows, got {count}; errs={errs[:5]}"
    assert errs == []


def test_default_max_retries_is_five():
    assert DEFAULT_MAX_RETRIES == 5

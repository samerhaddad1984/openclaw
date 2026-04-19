"""DB retry decorator + WAL bootstrap.

Wraps any function that issues SQLite writes with exponential-backoff
retry on ``OperationalError: database is locked`` / ``database is busy``.

Usage:

    from src.db.retry import retry_on_lock, enable_wal_mode

    # At app bootstrap (once):
    enable_wal_mode(DB_PATH)

    # On every write handler:
    @retry_on_lock()
    def save_document(conn, ...):
        ...
"""
from __future__ import annotations

import functools
import logging
import sqlite3
import time
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 5
DEFAULT_INITIAL_BACKOFF = 0.05
DEFAULT_MAX_BACKOFF = 1.0

_LOCK_MARKERS = ("locked", "busy")


def _is_lock_error(err: Exception) -> bool:
    if not isinstance(err, sqlite3.OperationalError):
        return False
    return any(m in str(err).lower() for m in _LOCK_MARKERS)


def retry_on_lock(
    max_retries: int = DEFAULT_MAX_RETRIES,
    initial_backoff: float = DEFAULT_INITIAL_BACKOFF,
    max_backoff: float = DEFAULT_MAX_BACKOFF,
):
    """Decorator factory. Retries a function up to ``max_retries + 1`` times
    on SQLite database-locked errors, with exponential backoff.
    """
    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if not _is_lock_error(e) or attempt >= max_retries:
                        raise
                    backoff = min(initial_backoff * (2 ** attempt), max_backoff)
                    log.info(
                        "DB locked in %s (attempt %d/%d); sleeping %.3fs",
                        fn.__name__, attempt + 1, max_retries + 1, backoff,
                    )
                    time.sleep(backoff)
                    attempt += 1
        return _wrapper
    return _decorator


def enable_wal_mode(db_path: Path | str) -> str:
    """Switch a SQLite DB to WAL journal mode. Returns the new mode string.
    Idempotent (a WAL-already DB just returns 'wal').
    """
    conn = sqlite3.connect(str(db_path), timeout=5)
    try:
        # busy_timeout is set per-connection; apply 5s so the retry decorator
        # is a secondary backstop rather than the primary coping mechanism.
        conn.execute("PRAGMA busy_timeout = 5000")
        new_mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        conn.commit()
        return str(new_mode)
    finally:
        conn.close()


def attempt_count_for_last_call() -> int:
    """Test hook: last call's total attempt count (1 = first try succeeded)."""
    return getattr(retry_on_lock, "_last_attempts", 1)

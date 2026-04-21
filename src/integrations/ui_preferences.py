"""Per-user UI preference storage.

The dashboard has a handful of filter / sort-order controls that
used to live purely in the URL query-string. If a CPA navigated
away and came back, the filter state was lost. This module stores
the preferences per-user (scoped by firm_code so owner/firm_admin
accounts keep separate state per firm they touch).

Shape: ``user_ui_preferences(user_email, firm_code, preference_key,
preference_value, updated_at)``. `preference_value` is a TEXT
column; callers that store structured data should JSON-encode
before set and parse after get.

Design rules:
- URL overrides stored preference: if the incoming query param is
  present (even empty), it wins AND updates the stored preference
  for next visit.
- Missing query param falls back to the stored preference.
- "Clear filter" deletes the stored preference so the next visit
  sees a neutral default.
- Writes are fire-and-forget: a DB error (table missing on older
  deploy) is logged but doesn't break the page render.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_preferences_schema(db_path: Path | str) -> None:
    """Idempotent schema setup. The dashboard bootstrap also creates
    this table; exposing a standalone helper lets tests seed a DB
    without running the full bootstrap."""
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_ui_preferences (
                user_email TEXT NOT NULL,
                firm_code TEXT NOT NULL,
                preference_key TEXT NOT NULL,
                preference_value TEXT,
                updated_at TEXT,
                PRIMARY KEY (user_email, firm_code, preference_key)
            )
        """)
        conn.commit()


def get_preference(
    db_path: Path | str, *,
    user_email: str, firm_code: str, preference_key: str,
    default: str | None = None,
) -> str | None:
    if not user_email:
        return default
    with _open(db_path) as conn:
        try:
            row = conn.execute(
                "SELECT preference_value FROM user_ui_preferences "
                "WHERE user_email=? AND firm_code=? AND preference_key=?",
                (user_email, firm_code or '', preference_key),
            ).fetchone()
        except sqlite3.OperationalError:
            return default
    if row is None:
        return default
    val = row['preference_value']
    return val if val is not None else default


def set_preference(
    db_path: Path | str, *,
    user_email: str, firm_code: str, preference_key: str,
    preference_value: str,
) -> None:
    if not user_email:
        return
    try:
        with _open(db_path) as conn:
            conn.execute(
                "INSERT INTO user_ui_preferences "
                "(user_email, firm_code, preference_key, "
                " preference_value, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(user_email, firm_code, preference_key) "
                "DO UPDATE SET preference_value=excluded.preference_value, "
                "              updated_at=excluded.updated_at",
                (user_email, firm_code or '', preference_key,
                 preference_value, _iso_now()),
            )
            conn.commit()
    except sqlite3.OperationalError as exc:
        log.warning('ui preference write failed: %s', exc)


def clear_preference(
    db_path: Path | str, *,
    user_email: str, firm_code: str, preference_key: str,
) -> None:
    if not user_email:
        return
    try:
        with _open(db_path) as conn:
            conn.execute(
                "DELETE FROM user_ui_preferences "
                "WHERE user_email=? AND firm_code=? AND preference_key=?",
                (user_email, firm_code or '', preference_key),
            )
            conn.commit()
    except sqlite3.OperationalError as exc:
        log.warning('ui preference delete failed: %s', exc)


def get_all_preferences(
    db_path: Path | str, *,
    user_email: str, firm_code: str,
) -> dict[str, str]:
    """Return every stored preference for this (user, firm) as a dict."""
    if not user_email:
        return {}
    with _open(db_path) as conn:
        try:
            rows = conn.execute(
                "SELECT preference_key, preference_value "
                "FROM user_ui_preferences "
                "WHERE user_email=? AND firm_code=?",
                (user_email, firm_code or ''),
            ).fetchall()
        except sqlite3.OperationalError:
            return {}
    return {r['preference_key']: r['preference_value'] for r in rows}


# ---------------------------------------------------------------------------
# Higher-level helper used by route handlers
# ---------------------------------------------------------------------------


_SENTINEL = object()


def resolve_with_override(
    db_path: Path | str, *,
    user_email: str, firm_code: str, preference_key: str,
    url_value: Any = _SENTINEL,
    default: str | None = None,
    persist_empty: bool = False,
) -> str | None:
    """Decide what value to apply for a preference-backed filter.

    Priority:
    1. `url_value` was explicitly passed (not _SENTINEL). URL wins
       AND updates the stored preference so next visit defaults to
       the same state.
    2. Otherwise, fall back to the stored preference.
    3. Otherwise, use `default`.

    Set ``persist_empty=True`` when an empty URL value (e.g.
    ``?uploader=``) should clear the stored preference instead of
    writing an empty string. This matches the "Clear filter" button
    semantics."""
    if url_value is not _SENTINEL:
        if not url_value and persist_empty:
            clear_preference(
                db_path, user_email=user_email, firm_code=firm_code,
                preference_key=preference_key,
            )
            return default
        # URL provided: persist + return that value.
        stored = url_value if isinstance(url_value, str) else str(url_value)
        set_preference(
            db_path, user_email=user_email, firm_code=firm_code,
            preference_key=preference_key, preference_value=stored,
        )
        return stored
    stored = get_preference(
        db_path, user_email=user_email, firm_code=firm_code,
        preference_key=preference_key,
    )
    if stored is not None:
        return stored
    return default


# Well-known preference keys used by the dashboard. Keep them as
# module-level constants so typos fail at import time.
PREF_QUEUE_UPLOADER = 'queue.uploader_filter'
PREF_QUEUE_STATUS = 'queue.status_filter'
PREF_QUEUE_DATE_RANGE = 'queue.date_range'
PREF_CLIENTS_SORT = 'clients.sort_order'
PREF_REVIEW_PRIORITY = 'review_queue.priority_filter'

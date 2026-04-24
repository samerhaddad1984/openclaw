"""Client archive / deactivation with 7-year retention.

Real scenarios this module handles:

  - Client leaves the firm to work with another CPA.
  - Client's business closes.
  - Client goes dormant for months.

Archiving is the firm-controlled soft-delete path: the client row is
preserved forever (or until retention expiry), portal tokens are
invalidated, new uploads are refused, and the client is hidden from
default listings. Reactivation is supported and fully reversible.

Retention: archived clients hang around for 7 years (regulatory
minimum). After that window, the ``purge_eligible`` helper lists
candidates and ``purge_client`` removes their data when the firm
owner explicitly confirms. We never delete automatically.
"""
from __future__ import annotations

import logging
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


STATUS_ACTIVE = 'active'
STATUS_ARCHIVED = 'archived'

REASON_LEFT_FIRM = 'left_firm'
REASON_BUSINESS_CLOSED = 'business_closed'
REASON_DORMANT = 'dormant'
REASON_OTHER = 'other'
VALID_REASONS = (REASON_LEFT_FIRM, REASON_BUSINESS_CLOSED,
                 REASON_DORMANT, REASON_OTHER)

RETENTION_YEARS = 7


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def ensure_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        # Add archive columns to clients if the table exists.
        tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='clients'"
        ).fetchone()
        if tbl:
            cols = {r['name'] for r in conn.execute(
                "PRAGMA table_info(clients)"
            ).fetchall()}
            for col, decl in (
                ('status', "TEXT DEFAULT 'active'"),
                ('archive_reason', 'TEXT'),
                ('archive_notes', 'TEXT'),
                ('archived_at', 'TEXT'),
                ('archived_by', 'TEXT'),
                ('retention_expires_at', 'TEXT'),
            ):
                if col not in cols:
                    try:
                        conn.execute(
                            f"ALTER TABLE clients ADD COLUMN {col} {decl}"
                        )
                    except sqlite3.OperationalError:
                        pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_archive_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT,
                notes TEXT,
                actor TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_archive_audit_client "
            "ON client_archive_audit(firm_code, client_code, created_at)"
        )
        conn.commit()


def _retention_expiry(base: datetime | None = None) -> str:
    anchor = base or datetime.now(timezone.utc)
    return anchor.replace(tzinfo=timezone.utc).isoformat(timespec='seconds') \
        if anchor.tzinfo is None else anchor.isoformat(timespec='seconds')


def _retention_iso(years: int = RETENTION_YEARS,
                   base: datetime | None = None) -> str:
    anchor = base or datetime.now(timezone.utc)
    try:
        expiry = anchor.replace(year=anchor.year + years)
    except ValueError:  # Feb 29 rollover edge
        expiry = anchor.replace(month=2, day=28, year=anchor.year + years)
    return expiry.isoformat(timespec='seconds')


# ---------------------------------------------------------------------------
# Archive actions
# ---------------------------------------------------------------------------


def has_active_engagements(db_path: Path | str, client_code: str) -> bool:
    """Return True if the client has an open engagement or in-progress
    audit. The rule is enforced when archiving unless the caller
    passes ``force=True``.
    """
    with _open(db_path) as conn:
        # period_close open periods
        tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='period_close_checklists'"
        ).fetchone()
        if tbl:
            row = conn.execute(
                "SELECT 1 FROM period_close_checklists "
                "WHERE client_code=? AND status IN ('open','in_progress') "
                "LIMIT 1", (client_code,)
            ).fetchone()
            if row:
                return True
        # audit working papers
        tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='audit_working_papers'"
        ).fetchone()
        if tbl:
            row = conn.execute(
                "SELECT 1 FROM audit_working_papers "
                "WHERE client_code=? AND status IN ('draft','in_progress') "
                "LIMIT 1", (client_code,)
            ).fetchone()
            if row:
                return True
    return False


def archive_client(
    db_path: Path | str, *, firm_code: str, client_code: str,
    reason: str, actor: str, notes: str = '',
    force: bool = False,
) -> dict:
    """Archive a client.

    Raises on invalid reason. When ``force=False`` and the client has
    open engagements or in-progress audits the call returns
    ``{ok: False, reason: 'has_active_engagements'}``. The caller is
    responsible for closing those first — or passing ``force=True`` to
    override, in which case the reason is recorded.
    """
    if reason not in VALID_REASONS:
        return {'ok': False, 'reason': 'invalid_reason'}
    ensure_schema(db_path)
    if not force and has_active_engagements(db_path, client_code):
        return {'ok': False, 'reason': 'has_active_engagements'}

    now = _iso_now()
    retention = _retention_iso()
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT firm_code, status FROM clients WHERE client_code=?",
            (client_code,),
        ).fetchone()
        if not row:
            return {'ok': False, 'reason': 'unknown_client'}
        if row['firm_code'] != firm_code:
            return {'ok': False, 'reason': 'wrong_firm'}
        if row['status'] == STATUS_ARCHIVED:
            return {'ok': False, 'reason': 'already_archived'}
        conn.execute(
            "UPDATE clients SET status=?, archive_reason=?, "
            "archive_notes=?, archived_at=?, archived_by=?, "
            "retention_expires_at=? WHERE client_code=?",
            (STATUS_ARCHIVED, reason, notes, now, actor, retention,
             client_code),
        )
        # Invalidate portal tokens: rotate the client-level token and
        # wipe any per-user tokens.
        conn.execute(
            "UPDATE clients SET portal_token=NULL WHERE client_code=?",
            (client_code,),
        )
        portal_users_tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='client_portal_users'"
        ).fetchone()
        if portal_users_tbl:
            conn.execute(
                "UPDATE client_portal_users SET token=NULL, status='revoked' "
                "WHERE client_code=?", (client_code,)
            )
        conn.execute(
            "INSERT INTO client_archive_audit "
            "(firm_code, client_code, action, reason, notes, actor) "
            "VALUES (?,?,?,?,?,?)",
            (firm_code, client_code, 'archived', reason,
             notes if not force else f'{notes} (FORCED)', actor),
        )
        conn.commit()
    return {'ok': True, 'status': STATUS_ARCHIVED,
            'retention_expires_at': retention, 'forced': bool(force)}


def reactivate_client(
    db_path: Path | str, *, firm_code: str, client_code: str,
    actor: str, notes: str = '',
) -> dict:
    """Un-archive a client. Data is intact, but tokens stay wiped —
    the firm will need to re-issue them via the existing rotation
    flow so a stale stash of old links can't resume access.
    """
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT firm_code, status FROM clients WHERE client_code=?",
            (client_code,),
        ).fetchone()
        if not row:
            return {'ok': False, 'reason': 'unknown_client'}
        if row['firm_code'] != firm_code:
            return {'ok': False, 'reason': 'wrong_firm'}
        if row['status'] != STATUS_ARCHIVED:
            return {'ok': False, 'reason': 'not_archived'}
        conn.execute(
            "UPDATE clients SET status=?, archive_reason=NULL, "
            "archived_at=NULL, archived_by=NULL, "
            "retention_expires_at=NULL WHERE client_code=?",
            (STATUS_ACTIVE, client_code),
        )
        conn.execute(
            "INSERT INTO client_archive_audit "
            "(firm_code, client_code, action, reason, notes, actor) "
            "VALUES (?,?,?,?,?,?)",
            (firm_code, client_code, 'reactivated', None, notes, actor),
        )
        conn.commit()
    return {'ok': True, 'status': STATUS_ACTIVE}


def can_accept_uploads(db_path: Path | str, client_code: str) -> bool:
    """Gate used by every ingest path. Archived clients reject new
    documents without touching the existing active flag semantics."""
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT status FROM clients WHERE client_code=?",
            (client_code,),
        ).fetchone()
    if not row:
        return False
    return (row['status'] or STATUS_ACTIVE) == STATUS_ACTIVE


# ---------------------------------------------------------------------------
# Listing helpers (used by the admin UI to hide or reveal archived)
# ---------------------------------------------------------------------------


def list_clients(db_path: Path | str, firm_code: str, *,
                 include_archived: bool = False) -> list[dict]:
    ensure_schema(db_path)
    sql = (
        "SELECT client_code, client_name, status, archive_reason, "
        "archived_at, retention_expires_at FROM clients WHERE firm_code=? "
    )
    params: list[Any] = [firm_code]
    if not include_archived:
        sql += "AND COALESCE(status,'active')='active' "
    sql += "ORDER BY client_name"
    with _open(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def list_archived(db_path: Path | str, firm_code: str) -> list[dict]:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT client_code, client_name, archive_reason, archived_at, "
            "archived_by, retention_expires_at FROM clients "
            "WHERE firm_code=? AND status=? ORDER BY archived_at DESC",
            (firm_code, STATUS_ARCHIVED),
        ).fetchall()
    return [dict(r) for r in rows]


def get_audit_trail(db_path: Path | str, firm_code: str,
                    client_code: str) -> list[dict]:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM client_archive_audit WHERE firm_code=? "
            "AND client_code=? ORDER BY created_at DESC",
            (firm_code, client_code),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------


def purge_eligible(db_path: Path | str, firm_code: str) -> list[dict]:
    """List archived clients whose retention window has elapsed."""
    ensure_schema(db_path)
    now = _iso_now()
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT client_code, client_name, archived_at, "
            "retention_expires_at FROM clients "
            "WHERE firm_code=? AND status=? "
            "AND retention_expires_at IS NOT NULL "
            "AND retention_expires_at <= ? "
            "ORDER BY retention_expires_at",
            (firm_code, STATUS_ARCHIVED, now),
        ).fetchall()
    return [dict(r) for r in rows]


def purge_client(
    db_path: Path | str, *, firm_code: str, client_code: str,
    actor: str, confirm_token: str,
) -> dict:
    """Hard-delete a client's data. Called only when the firm owner
    explicitly confirms + the retention window has elapsed. The
    confirm_token must match the one the UI generated for this
    specific client so accidental double-submits can't wipe data.
    """
    ensure_schema(db_path)
    expected = deterministic_confirm_token(firm_code, client_code)
    if not secrets.compare_digest(confirm_token, expected):
        return {'ok': False, 'reason': 'bad_confirm_token'}
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT firm_code, status, retention_expires_at "
            "FROM clients WHERE client_code=?", (client_code,),
        ).fetchone()
        if not row:
            return {'ok': False, 'reason': 'unknown_client'}
        if row['firm_code'] != firm_code:
            return {'ok': False, 'reason': 'wrong_firm'}
        if row['status'] != STATUS_ARCHIVED:
            return {'ok': False, 'reason': 'not_archived'}
        if not row['retention_expires_at']:
            return {'ok': False, 'reason': 'no_retention_set'}
        if row['retention_expires_at'] > _iso_now():
            return {'ok': False, 'reason': 'retention_still_active'}
        conn.execute(
            "INSERT INTO client_archive_audit "
            "(firm_code, client_code, action, reason, notes, actor) "
            "VALUES (?,?,?,?,?,?)",
            (firm_code, client_code, 'purged', 'retention_expired',
             'confirmed by firm owner', actor),
        )
        conn.execute("DELETE FROM clients WHERE client_code=?",
                     (client_code,))
        conn.commit()
    return {'ok': True, 'status': 'purged'}


def deterministic_confirm_token(firm_code: str, client_code: str) -> str:
    """Stable per-client confirm token. Rendered into the purge form
    and verified on POST so a replay against a different client
    aborts at the compare_digest step.
    """
    import hashlib
    raw = f'purge::{firm_code}::{client_code}'.encode()
    return hashlib.sha256(raw).hexdigest()[:32]

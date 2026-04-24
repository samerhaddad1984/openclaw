"""Employee out-of-office + coverage + firm-departure rebalancing.

Three real scenarios this module handles:

  1. Employee on vacation / sick leave — temporary OOO window with
     a coverage colleague. New documents that would have auto-
     routed to them instead go to the coverage employee; existing
     assignments stay untouched (so returning from vacation doesn't
     dump a backlog onto someone else).

  2. Employee leaves the firm permanently — HR action that reassigns
     every open assignment to a replacement. Dashboard access is
     revoked immediately.

  3. Bulk reassignment from admin — deliberate, explicit reshuffle
     (separate from OOO so nobody gets confused about what happened).

The module writes to ``employee_ooo`` and ``employee_ooo_audit``; the
coverage lookup is consulted at auto-assign time (see
``src/integrations/auto_assign.auto_assign_new_document``) but only
for new incoming documents — existing assignments are never touched
unless the admin explicitly calls ``bulk_reassign``.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


OOO_STATUS_ACTIVE = 'active'
OOO_STATUS_ENDED = 'ended'
OOO_STATUS_CANCELLED = 'cancelled'


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def ensure_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS employee_ooo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                employee_email TEXT NOT NULL,
                coverage_email TEXT,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                auto_reply_subject TEXT,
                auto_reply_body TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                created_by TEXT,
                ended_at TEXT,
                ended_by TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ooo_employee "
            "ON employee_ooo(firm_code, employee_email, status)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS employee_ooo_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                employee_email TEXT NOT NULL,
                action TEXT NOT NULL,
                detail TEXT,
                actor TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Coverage permission check
# ---------------------------------------------------------------------------


def coverage_has_client_access(
    db_path: Path | str, *, firm_code: str,
    employee_email: str, coverage_email: str,
) -> bool:
    """True iff the coverage employee already has access to every
    client the original employee is primary / secondary on.

    Owners / firm_admins are always considered to have access.
    """
    if not coverage_email:
        return False
    with _open(db_path) as conn:
        # Check role.
        try:
            u = conn.execute(
                "SELECT role FROM dashboard_users "
                "WHERE LOWER(email)=LOWER(?) AND firm_code=?",
                (coverage_email, firm_code),
            ).fetchone()
            if u and (u['role'] or '').lower() in ('owner', 'firm_admin'):
                return True
        except sqlite3.OperationalError:
            pass
        # Match every client where employee_email is primary or secondary.
        try:
            emp_clients = conn.execute(
                "SELECT client_code FROM clients "
                "WHERE firm_code=? AND ("
                "     LOWER(primary_employee_email)=LOWER(?) "
                "  OR LOWER(secondary_employee_email)=LOWER(?))",
                (firm_code, employee_email, employee_email),
            ).fetchall()
        except sqlite3.OperationalError:
            return False
        cov_clients = conn.execute(
            "SELECT client_code FROM clients "
            "WHERE firm_code=? AND ("
            "     LOWER(primary_employee_email)=LOWER(?) "
            "  OR LOWER(secondary_employee_email)=LOWER(?))",
            (firm_code, coverage_email, coverage_email),
        ).fetchall()
        emp_set = {r['client_code'] for r in emp_clients}
        cov_set = {r['client_code'] for r in cov_clients}
    # Coverage must already own (or share) every client. Unowned
    # clients are handled by the firm pool anyway.
    return emp_set.issubset(cov_set) if emp_set else True


# ---------------------------------------------------------------------------
# Set / end OOO
# ---------------------------------------------------------------------------


def set_ooo(
    db_path: Path | str, *,
    firm_code: str,
    employee_email: str,
    coverage_email: str | None,
    start_date: str,
    end_date: str,
    auto_reply_subject: str = '',
    auto_reply_body: str = '',
    created_by: str = '',
    require_coverage_permission: bool = True,
) -> dict:
    """Open a new OOO window.

    Fails if an active window already exists for this employee (call
    ``end_ooo`` first to update). When
    ``require_coverage_permission`` is True the coverage employee
    must already have access to every client the OOO employee owns,
    otherwise the call returns ``{ok: False,
    reason: 'coverage_missing_client_access'}`` and the admin must
    grant access (or pass ``require_coverage_permission=False`` after
    explicitly deciding).
    """
    if start_date > end_date:
        return {'ok': False, 'reason': 'bad_date_range'}
    ensure_schema(db_path)
    if coverage_email and require_coverage_permission:
        if not coverage_has_client_access(
            db_path, firm_code=firm_code,
            employee_email=employee_email,
            coverage_email=coverage_email,
        ):
            return {'ok': False,
                    'reason': 'coverage_missing_client_access'}
    with _open(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM employee_ooo "
            "WHERE firm_code=? AND employee_email=? AND status='active'",
            (firm_code, employee_email),
        ).fetchone()
        if existing:
            return {'ok': False, 'reason': 'already_active'}
        cur = conn.execute(
            "INSERT INTO employee_ooo "
            "(firm_code, employee_email, coverage_email, start_date, "
            " end_date, auto_reply_subject, auto_reply_body, created_by) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (firm_code, employee_email, coverage_email, start_date,
             end_date, auto_reply_subject, auto_reply_body, created_by),
        )
        conn.execute(
            "INSERT INTO employee_ooo_audit "
            "(firm_code, employee_email, action, detail, actor) "
            "VALUES (?,?,?,?,?)",
            (firm_code, employee_email, 'set',
             f'coverage={coverage_email};{start_date}..{end_date}',
             created_by),
        )
        conn.commit()
        return {'ok': True, 'id': cur.lastrowid}


def end_ooo(
    db_path: Path | str, *,
    firm_code: str, employee_email: str, ended_by: str,
    reason: str = 'ended',
) -> dict:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM employee_ooo "
            "WHERE firm_code=? AND employee_email=? AND status='active' "
            "ORDER BY id DESC LIMIT 1",
            (firm_code, employee_email),
        ).fetchone()
        if not row:
            return {'ok': False, 'reason': 'no_active_ooo'}
        new_status = (OOO_STATUS_CANCELLED
                      if reason == 'cancelled' else OOO_STATUS_ENDED)
        conn.execute(
            "UPDATE employee_ooo SET status=?, ended_at=?, ended_by=? "
            "WHERE id=?",
            (new_status, _iso_now(), ended_by, row['id']),
        )
        conn.execute(
            "INSERT INTO employee_ooo_audit "
            "(firm_code, employee_email, action, detail, actor) "
            "VALUES (?,?,?,?,?)",
            (firm_code, employee_email, new_status, f'id={row["id"]}',
             ended_by),
        )
        conn.commit()
    return {'ok': True, 'id': row['id'], 'status': new_status}


# ---------------------------------------------------------------------------
# Coverage lookup
# ---------------------------------------------------------------------------


def get_coverage_for(
    db_path: Path | str, *, firm_code: str, employee_email: str,
    as_of: str | None = None,
) -> str | None:
    """Return the coverage email for this employee on ``as_of`` date,
    or None when no active OOO window covers it.
    """
    ensure_schema(db_path)
    today = as_of or _today()
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT coverage_email FROM employee_ooo "
            "WHERE firm_code=? AND LOWER(employee_email)=LOWER(?) "
            "AND status='active' "
            "AND start_date <= ? AND end_date >= ?",
            (firm_code, employee_email, today, today),
        ).fetchone()
    return row['coverage_email'] if row and row['coverage_email'] else None


def is_ooo(
    db_path: Path | str, *, firm_code: str, employee_email: str,
    as_of: str | None = None,
) -> bool:
    return get_coverage_for(
        db_path, firm_code=firm_code,
        employee_email=employee_email, as_of=as_of,
    ) is not None


# ---------------------------------------------------------------------------
# Firm departure
# ---------------------------------------------------------------------------


def depart_employee(
    db_path: Path | str, *, firm_code: str,
    employee_email: str, replacement_email: str,
    actor: str,
) -> dict:
    """Permanent removal: rebalance every open workflow assignment +
    every client primary/secondary pointer to the replacement, revoke
    dashboard access. End any active OOO window.
    """
    ensure_schema(db_path)
    with _open(db_path) as conn:
        # End active OOO windows.
        conn.execute(
            "UPDATE employee_ooo SET status='ended', ended_at=?, "
            "ended_by=? WHERE firm_code=? AND employee_email=? "
            "AND status='active'",
            (_iso_now(), actor, firm_code, employee_email),
        )
        # Reassign open document workflows.
        try:
            wf_updated = conn.execute(
                "UPDATE review_workflow SET assigned_to_email=? "
                "WHERE firm_code=? AND LOWER(assigned_to_email)=LOWER(?) "
                "AND status NOT IN ('resolved','closed','completed')",
                (replacement_email, firm_code, employee_email),
            ).rowcount
        except sqlite3.OperationalError:
            wf_updated = 0
        # Update primary/secondary on clients.
        try:
            cp_updated = conn.execute(
                "UPDATE clients SET primary_employee_email=? "
                "WHERE firm_code=? AND LOWER(primary_employee_email)=LOWER(?)",
                (replacement_email, firm_code, employee_email),
            ).rowcount
            cs_updated = conn.execute(
                "UPDATE clients SET secondary_employee_email=? "
                "WHERE firm_code=? AND LOWER(secondary_employee_email)=LOWER(?)",
                (replacement_email, firm_code, employee_email),
            ).rowcount
        except sqlite3.OperationalError:
            cp_updated = cs_updated = 0
        # Revoke dashboard access.
        try:
            conn.execute(
                "UPDATE dashboard_users SET active=0 "
                "WHERE firm_code=? AND LOWER(email)=LOWER(?)",
                (firm_code, employee_email),
            )
        except sqlite3.OperationalError:
            pass
        # Audit.
        conn.execute(
            "INSERT INTO employee_ooo_audit "
            "(firm_code, employee_email, action, detail, actor) "
            "VALUES (?,?,?,?,?)",
            (firm_code, employee_email, 'depart',
             f'replacement={replacement_email};'
             f'wf={wf_updated};primary={cp_updated};secondary={cs_updated}',
             actor),
        )
        conn.commit()
    return {
        'ok': True,
        'workflows_reassigned': wf_updated,
        'primaries_updated': cp_updated,
        'secondaries_updated': cs_updated,
    }


# ---------------------------------------------------------------------------
# Bulk reassignment (explicit admin tool — NOT the OOO path)
# ---------------------------------------------------------------------------


def bulk_reassign(
    db_path: Path | str, *, firm_code: str,
    from_email: str, to_email: str, actor: str,
    include_resolved: bool = False,
) -> dict:
    """Reassign every open workflow from one employee to another.
    Separate entry point so tools that want to touch existing
    assignments (e.g., a returning-from-OOO redistribution) do so
    deliberately rather than piggybacking on the OOO flag.
    """
    ensure_schema(db_path)
    with _open(db_path) as conn:
        try:
            clause = "" if include_resolved else (
                "AND status NOT IN ('resolved','closed','completed') "
            )
            updated = conn.execute(
                "UPDATE review_workflow SET assigned_to_email=? "
                "WHERE firm_code=? AND LOWER(assigned_to_email)=LOWER(?) "
                + clause,
                (to_email, firm_code, from_email),
            ).rowcount
        except sqlite3.OperationalError:
            updated = 0
        conn.execute(
            "INSERT INTO employee_ooo_audit "
            "(firm_code, employee_email, action, detail, actor) "
            "VALUES (?,?,?,?,?)",
            (firm_code, from_email, 'bulk_reassign',
             f'to={to_email};count={updated}', actor),
        )
        conn.commit()
    return {'ok': True, 'reassigned': updated}


# ---------------------------------------------------------------------------
# Auto-reply payload (used by notification_sender and WhatsApp handler)
# ---------------------------------------------------------------------------


def auto_reply_payload(
    db_path: Path | str, *, firm_code: str, employee_email: str,
    lang: str = 'fr',
) -> dict | None:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT coverage_email, end_date, auto_reply_subject, "
            "       auto_reply_body FROM employee_ooo "
            "WHERE firm_code=? AND LOWER(employee_email)=LOWER(?) "
            "AND status='active' "
            "AND start_date <= ? AND end_date >= ? "
            "ORDER BY id DESC LIMIT 1",
            (firm_code, employee_email, _today(), _today()),
        ).fetchone()
    if not row:
        return None
    if row['auto_reply_subject'] and row['auto_reply_body']:
        return {'subject': row['auto_reply_subject'],
                'body': row['auto_reply_body']}
    if lang == 'en':
        return {
            'subject': f'Out of office until {row["end_date"]}',
            'body': (
                f'I am out of office until {row["end_date"]}. '
                f'{row["coverage_email"] or "A colleague"} is '
                'handling my queue and will reply shortly.'
            ),
        }
    return {
        'subject': f'Absence jusqu\'au {row["end_date"]}',
        'body': (
            f'Je suis absent·e jusqu\'au {row["end_date"]}. '
            f'{row["coverage_email"] or "Un·e collègue"} '
            'prend le relais et vous répondra sous peu.'
        ),
    }


def list_active_ooo(db_path: Path | str, firm_code: str) -> list[dict]:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT id, employee_email, coverage_email, start_date, "
            "       end_date, auto_reply_subject FROM employee_ooo "
            "WHERE firm_code=? AND status='active' "
            "AND end_date >= ? ORDER BY start_date",
            (firm_code, _today()),
        ).fetchall()
    return [dict(r) for r in rows]

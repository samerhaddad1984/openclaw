"""Client-level assignment helpers for the hybrid model.

Owner / firm_admin set ``clients.primary_employee_email`` (and an
optional secondary). Every change is captured in
``client_assignment_history`` so the audit trail can answer "who
moved this client to whom and why" months later.

Employees CANNOT call ``update_client_assignment`` — that's enforced
at the route layer in scripts/review_dashboard.py and re-checked
defensively in this module.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


class ClientAssignmentError(Exception):
    """Raised on policy / state violations."""


def get_firm_employees(
    db_path: Path | str, firm_code: str,
    *, include_admins: bool = True,
) -> list[dict[str, str]]:
    """Return active dashboard users belonging to the firm.

    Used to build the primary/secondary dropdowns. Returns
    ``[{'email': ..., 'display_name': ..., 'role': ...}, ...]``
    sorted by display_name.
    """
    if not firm_code:
        return []
    roles = ('employee', 'firm_admin', 'owner') if include_admins \
        else ('employee',)
    placeholders = ','.join('?' for _ in roles)
    with _open(db_path) as conn:
        try:
            rows = conn.execute(
                f"SELECT email, display_name, role "
                f"FROM dashboard_users "
                f"WHERE firm_code = ? AND active = 1 "
                f"  AND role IN ({placeholders}) "
                f"  AND COALESCE(email, '') <> '' "
                f"ORDER BY COALESCE(display_name, email)",
                (firm_code, *roles),
            ).fetchall()
        except sqlite3.OperationalError:
            return []
    return [
        {
            'email': r['email'],
            'display_name': r['display_name'] or r['email'],
            'role': r['role'],
        }
        for r in rows
    ]


def get_client_assignment(
    db_path: Path | str, *, firm_code: str, client_code: str,
) -> dict[str, Any] | None:
    """Return current assignment for a client, or None when missing."""
    with _open(db_path) as conn:
        try:
            row = conn.execute(
                "SELECT client_code, firm_code, "
                "       primary_employee_email, secondary_employee_email, "
                "       assignment_updated_at, assignment_updated_by "
                "FROM clients "
                "WHERE client_code=? AND firm_code=?",
                (client_code, firm_code),
            ).fetchone()
        except sqlite3.OperationalError:
            return None
    return dict(row) if row else None


def update_client_assignment(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    primary_email: Optional[str],
    secondary_email: Optional[str],
    changed_by: str,
    actor_role: str,
    reason: str = "",
) -> dict[str, Any]:
    """Update primary/secondary on a client + write audit row.

    Empty string is normalized to NULL. Validates that primary !=
    secondary (a person can't be both — pick one or split). When the
    assignment doesn't change, the function still returns the current
    state but skips the audit insert. ``actor_role`` must be owner
    or firm_admin.
    """
    if actor_role not in ('owner', 'firm_admin'):
        raise ClientAssignmentError(
            f"role={actor_role!r} cannot reassign clients; needs owner "
            f"or firm_admin"
        )
    primary = (primary_email or '').strip() or None
    secondary = (secondary_email or '').strip() or None
    if primary and secondary and primary.lower() == secondary.lower():
        raise ClientAssignmentError(
            "primary and secondary employees must differ"
        )
    now = _iso_now()
    with _open(db_path) as conn:
        cur = conn.execute(
            "SELECT primary_employee_email, secondary_employee_email "
            "FROM clients WHERE client_code=? AND firm_code=?",
            (client_code, firm_code),
        ).fetchone()
        if cur is None:
            raise ClientAssignmentError(
                f"client {client_code!r} not found in firm {firm_code!r}"
            )
        prev_primary = cur['primary_employee_email']
        prev_secondary = cur['secondary_employee_email']
        if (prev_primary or None) == primary \
                and (prev_secondary or None) == secondary:
            return {
                'client_code': client_code,
                'firm_code': firm_code,
                'primary_employee_email': primary,
                'secondary_employee_email': secondary,
                'changed': False,
            }
        conn.execute(
            "UPDATE clients SET "
            "  primary_employee_email   = ?, "
            "  secondary_employee_email = ?, "
            "  assignment_updated_at    = ?, "
            "  assignment_updated_by    = ? "
            "WHERE client_code=? AND firm_code=?",
            (primary, secondary, now, changed_by, client_code, firm_code),
        )
        action = _classify_action(
            prev_primary, prev_secondary, primary, secondary,
        )
        conn.execute(
            "INSERT INTO client_assignment_history "
            "(firm_code, client_code, action, "
            " previous_primary, new_primary, "
            " previous_secondary, new_secondary, "
            " reason, changed_by, changed_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (firm_code, client_code, action,
             prev_primary, primary,
             prev_secondary, secondary,
             reason or None, changed_by, now),
        )
        conn.commit()
    return {
        'client_code': client_code,
        'firm_code': firm_code,
        'primary_employee_email': primary,
        'secondary_employee_email': secondary,
        'changed': True,
        'action': action,
    }


def _classify_action(
    prev_primary: Optional[str], prev_secondary: Optional[str],
    new_primary: Optional[str], new_secondary: Optional[str],
) -> str:
    had = bool(prev_primary or prev_secondary)
    has = bool(new_primary or new_secondary)
    if not had and has:
        return 'assigned'
    if had and not has:
        return 'unassigned'
    return 'reassigned'


def get_assignment_history(
    db_path: Path | str, *, firm_code: str, client_code: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        try:
            rows = conn.execute(
                "SELECT * FROM client_assignment_history "
                "WHERE firm_code=? AND client_code=? "
                "ORDER BY changed_at DESC, id DESC LIMIT ?",
                (firm_code, client_code, limit),
            ).fetchall()
        except sqlite3.OperationalError:
            return []
    return [dict(r) for r in rows]


def list_clients_with_assignment(
    db_path: Path | str, *, firm_code: str,
    employee_email: Optional[str] = None,
    only_unassigned: bool = False,
) -> list[dict[str, Any]]:
    """List clients in the firm with their current primary/secondary.

    ``employee_email`` filters to clients where they are primary or
    secondary. ``only_unassigned`` filters to clients with neither
    primary nor secondary set.
    """
    sql = (
        "SELECT client_code, client_name, "
        "       primary_employee_email, secondary_employee_email, "
        "       assignment_updated_at, assignment_updated_by "
        "FROM clients WHERE firm_code = ?"
    )
    params: list[Any] = [firm_code]
    if only_unassigned:
        sql += " AND COALESCE(primary_employee_email,'') = '' " \
               " AND COALESCE(secondary_employee_email,'') = ''"
    elif employee_email:
        sql += (" AND (LOWER(primary_employee_email)   = LOWER(?) "
                "  OR  LOWER(secondary_employee_email) = LOWER(?))")
        params.extend([employee_email, employee_email])
    sql += " ORDER BY client_code"
    with _open(db_path) as conn:
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []
    return [dict(r) for r in rows]

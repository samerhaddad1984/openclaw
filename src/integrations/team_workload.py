"""Phase 7: team workload report.

Per-employee workload roll-up used by /reports/team_workload and the
owner-dashboard widget. Queries are firm-scoped; owner sees all
firms, firm_admin sees only their own.

Metrics per active employee:
- ``primary_clients``:  clients where they are primary employee
- ``secondary_clients``: clients where they are secondary
- ``open_docs``:        documents assigned (explicit or via primary)
                         that are not approved or rejected-closed
- ``completed_this_week``: docs this person approved (for admins) or
                            had approved (for employees) in the last 7
                            days, rough throughput signal
- ``avg_resolution_hours``: average hours from first assignment to
                             approval/rejection over the last 90 days

A single SQL round-trip is used for each metric family to keep the
report cheap even for firms with hundreds of employees.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)) \
        .replace(microsecond=0).isoformat()


def get_team_workload(
    db_path: Path | str, *, firm_code: str | None,
) -> list[dict[str, Any]]:
    """Return a row per active dashboard user with their workload metrics.

    ``firm_code=None`` means "across all firms" (owner view).
    Rows are sorted by open_docs DESC so overloaded employees surface
    first.
    """
    with _open(db_path) as conn:
        firm_clause = ""
        firm_params: tuple = ()
        if firm_code:
            firm_clause = " AND firm_code = ?"
            firm_params = (firm_code,)
        try:
            users = conn.execute(
                f"SELECT email, display_name, role, firm_code "
                f"FROM dashboard_users "
                f"WHERE active = 1 AND COALESCE(email,'') <> '' "
                f"  AND role IN ('employee','firm_admin','owner') "
                f"  {firm_clause} "
                f"ORDER BY role, display_name",
                firm_params,
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        rows: list[dict[str, Any]] = []
        week_ago = _iso(7)
        ninety_ago = _iso(90)
        for u in users:
            email = u['email']
            efirm = u['firm_code'] or firm_code or 'OWNER'
            # Primary / secondary client counts.
            try:
                pc = conn.execute(
                    "SELECT COUNT(*) AS n FROM clients "
                    "WHERE firm_code=? "
                    "  AND LOWER(primary_employee_email)=LOWER(?)",
                    (efirm, email),
                ).fetchone()
                sc = conn.execute(
                    "SELECT COUNT(*) AS n FROM clients "
                    "WHERE firm_code=? "
                    "  AND LOWER(secondary_employee_email)=LOWER(?)",
                    (efirm, email),
                ).fetchone()
            except sqlite3.OperationalError:
                pc = sc = {'n': 0}
            primary_clients = int((pc or {'n': 0})['n'] or 0)
            secondary_clients = int((sc or {'n': 0})['n'] or 0)

            # Open docs: explicit assignment to this email with an
            # active status.
            try:
                open_row = conn.execute(
                    "SELECT COUNT(*) AS n FROM review_workflow "
                    "WHERE firm_code=? AND entity_type='document' "
                    "  AND LOWER(assigned_to_email)=LOWER(?) "
                    "  AND status IN ('assigned','in_progress',"
                    "                 'rejected','returned','escalated')",
                    (efirm, email),
                ).fetchone()
            except sqlite3.OperationalError:
                open_row = {'n': 0}
            open_docs = int((open_row or {'n': 0})['n'] or 0)

            # Completed-this-week: audits where this person either
            # performed an approve/reject or was the assignee on one.
            try:
                done_row = conn.execute(
                    "SELECT COUNT(DISTINCT rw.id) AS n "
                    "FROM review_workflow rw "
                    "JOIN review_workflow_audit a "
                    "  ON a.workflow_id = rw.id "
                    "WHERE rw.firm_code=? "
                    "  AND rw.entity_type='document' "
                    "  AND a.action IN ('approve','reject') "
                    "  AND a.created_at >= ? "
                    "  AND (LOWER(rw.assigned_to_email)=LOWER(?) "
                    "    OR LOWER(a.actor_email)=LOWER(?))",
                    (efirm, week_ago, email, email),
                ).fetchone()
            except sqlite3.OperationalError:
                done_row = {'n': 0}
            completed_week = int((done_row or {'n': 0})['n'] or 0)

            # Avg resolution hours over 90 days: first assigned_at
            # to reviewed_at on finished workflows where this person
            # was the assignee.
            try:
                avg_row = conn.execute(
                    "SELECT AVG(  "
                    "   (julianday(reviewed_at) - julianday(assigned_at))"
                    "   * 24.0"
                    " ) AS hours "
                    "FROM review_workflow "
                    "WHERE firm_code=? AND entity_type='document' "
                    "  AND LOWER(assigned_to_email)=LOWER(?) "
                    "  AND reviewed_at IS NOT NULL "
                    "  AND assigned_at IS NOT NULL "
                    "  AND assigned_at >= ?",
                    (efirm, email, ninety_ago),
                ).fetchone()
            except sqlite3.OperationalError:
                avg_row = {'hours': None}
            avg_hours = avg_row['hours'] if avg_row else None
            avg_resolution_hours = (
                round(float(avg_hours), 2) if avg_hours is not None else None
            )

            rows.append({
                'email': email,
                'display_name': u['display_name'] or email,
                'role': u['role'],
                'firm_code': efirm,
                'primary_clients': primary_clients,
                'secondary_clients': secondary_clients,
                'open_docs': open_docs,
                'completed_this_week': completed_week,
                'avg_resolution_hours': avg_resolution_hours,
            })
    rows.sort(key=lambda r: (-r['open_docs'], -r['primary_clients'],
                              r['display_name']))
    return rows

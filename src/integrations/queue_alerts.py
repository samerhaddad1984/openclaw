"""Queue overflow alerts + workload balancing.

When an employee's review queue grows past certain thresholds, the
firm admin needs to know *before* the backlog becomes a crisis.

Thresholds (matches the spec):

  - >= 30 unresolved documents assigned to an employee → yellow
    warning email to the employee.
  - >= 50 unresolved documents → red alert email to the employee
    and firm_admin(s).
  - >= 100 unresolved documents → daily reminder to firm_admin
    about workload imbalance.

Alerts are idempotent within a cool-down window (default 24 h) so a
flapping count doesn't spam the mailbox. The admin dashboard widget
renders a per-employee traffic-light summary; click an employee to
see their queue and optionally bulk-reassign.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


THRESHOLD_YELLOW = 30
THRESHOLD_RED = 50
THRESHOLD_ADMIN_DAILY = 100

LEVEL_GREEN = 'green'
LEVEL_YELLOW = 'yellow'
LEVEL_RED = 'red'
LEVEL_ADMIN = 'admin_daily'

DEFAULT_COOLDOWN_HOURS = 24

UNRESOLVED_STATUSES = (
    'assigned', 'in_review', 'needs_info', 'Queued', 'Processing',
    'Needs Review',
)


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def ensure_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS queue_alert_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                employee_email TEXT NOT NULL,
                level TEXT NOT NULL,
                queue_count INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_queue_alert_recent "
            "ON queue_alert_log(firm_code, employee_email, level, created_at)"
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Counting
# ---------------------------------------------------------------------------


def count_open_for_employee(
    db_path: Path | str, *, firm_code: str, employee_email: str,
) -> int:
    """Count open review_workflow rows assigned to this employee."""
    placeholders = ','.join('?' * len(UNRESOLVED_STATUSES))
    sql = (
        f"SELECT COUNT(*) FROM review_workflow "
        f"WHERE firm_code=? AND LOWER(assigned_to_email)=LOWER(?) "
        f"AND status IN ({placeholders})"
    )
    params = [firm_code, employee_email] + list(UNRESOLVED_STATUSES)
    with _open(db_path) as conn:
        try:
            cnt = conn.execute(sql, params).fetchone()[0]
        except sqlite3.OperationalError:
            return 0
    return int(cnt or 0)


def workload_snapshot(db_path: Path | str, firm_code: str) -> list[dict]:
    """Return {employee_email, queue_count, level} per active
    employee in the firm."""
    with _open(db_path) as conn:
        try:
            rows = conn.execute(
                "SELECT email FROM dashboard_users "
                "WHERE firm_code=? AND COALESCE(active,1)=1 "
                "AND role IN ('employee','firm_admin') "
                "ORDER BY email",
                (firm_code,),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
    out = []
    for r in rows:
        em = r['email']
        cnt = count_open_for_employee(
            db_path, firm_code=firm_code, employee_email=em,
        )
        out.append({
            'employee_email': em,
            'queue_count': cnt,
            'level': level_for_count(cnt),
        })
    return out


def level_for_count(count: int) -> str:
    if count >= THRESHOLD_RED:
        return LEVEL_RED
    if count >= THRESHOLD_YELLOW:
        return LEVEL_YELLOW
    return LEVEL_GREEN


# ---------------------------------------------------------------------------
# Alert state machine
# ---------------------------------------------------------------------------


def recent_alert(
    db_path: Path | str, *, firm_code: str, employee_email: str,
    level: str, cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
    now: datetime | None = None,
) -> bool:
    ensure_schema(db_path)
    anchor = now or datetime.now(timezone.utc)
    cutoff = (anchor - timedelta(hours=cooldown_hours)).isoformat(
        timespec='seconds'
    )
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM queue_alert_log "
            "WHERE firm_code=? AND LOWER(employee_email)=LOWER(?) "
            "AND level=? AND created_at >= ? LIMIT 1",
            (firm_code, employee_email, level, cutoff),
        ).fetchone()
    return row is not None


def log_alert(
    db_path: Path | str, *, firm_code: str, employee_email: str,
    level: str, queue_count: int,
) -> None:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        conn.execute(
            "INSERT INTO queue_alert_log "
            "(firm_code, employee_email, level, queue_count) "
            "VALUES (?,?,?,?)",
            (firm_code, employee_email, level, queue_count),
        )
        conn.commit()


def evaluate_employee(
    db_path: Path | str, *, firm_code: str, employee_email: str,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
    now: datetime | None = None,
) -> dict:
    """Decide which alert (if any) to fire for this employee.

    Returns a payload containing the email recipients, level, and the
    queue count. The caller is responsible for actually dispatching
    the email via ``notification_sender``. This module sticks to the
    state decision so it can be unit-tested without SMTP.
    """
    ensure_schema(db_path)
    cnt = count_open_for_employee(
        db_path, firm_code=firm_code, employee_email=employee_email,
    )
    if cnt >= THRESHOLD_ADMIN_DAILY:
        level = LEVEL_ADMIN
    elif cnt >= THRESHOLD_RED:
        level = LEVEL_RED
    elif cnt >= THRESHOLD_YELLOW:
        level = LEVEL_YELLOW
    else:
        level = LEVEL_GREEN
    if level == LEVEL_GREEN:
        return {'fire': False, 'level': level, 'queue_count': cnt}
    if recent_alert(
        db_path, firm_code=firm_code, employee_email=employee_email,
        level=level, cooldown_hours=cooldown_hours, now=now,
    ):
        return {'fire': False, 'level': level, 'queue_count': cnt,
                'suppressed': 'cooldown'}
    recipients = [employee_email]
    if level in (LEVEL_RED, LEVEL_ADMIN):
        with _open(db_path) as conn:
            try:
                admins = conn.execute(
                    "SELECT email FROM dashboard_users "
                    "WHERE firm_code=? AND COALESCE(active,1)=1 "
                    "AND role IN ('owner','firm_admin')",
                    (firm_code,),
                ).fetchall()
            except sqlite3.OperationalError:
                admins = []
        for a in admins:
            if a['email'] and a['email'] not in recipients:
                recipients.append(a['email'])
    return {
        'fire': True, 'level': level, 'queue_count': cnt,
        'recipients': recipients, 'employee_email': employee_email,
    }


def evaluate_firm(
    db_path: Path | str, firm_code: str,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
    now: datetime | None = None,
) -> list[dict]:
    """Run evaluate_employee for every active employee in the firm.
    Returns all alert decisions (fired or not) so the caller can
    act on ``fire=True`` entries.
    """
    snaps = workload_snapshot(db_path, firm_code)
    out = []
    for s in snaps:
        out.append(evaluate_employee(
            db_path, firm_code=firm_code,
            employee_email=s['employee_email'],
            cooldown_hours=cooldown_hours, now=now,
        ))
    return out


# ---------------------------------------------------------------------------
# Message payloads (used by notification_sender)
# ---------------------------------------------------------------------------


def alert_message(level: str, count: int, employee_email: str,
                  lang: str = 'fr') -> dict:
    if level == LEVEL_YELLOW:
        return {
            'subject': (
                f'Queue warning — {count} pending items'
                if lang == 'en'
                else f'Avertissement file d\'attente — {count} items'
            ),
            'body': (
                (f'Your review queue has {count} unresolved documents.'
                 ' Consider working through the backlog or asking an'
                 ' admin for help.')
                if lang == 'en'
                else (
                    f'Votre file d\'attente compte {count} documents non '
                    'résolus. Pensez à traiter l\'arriéré ou à demander '
                    'de l\'aide à un administrateur.'
                )
            ),
        }
    if level == LEVEL_RED:
        return {
            'subject': (
                f'Queue RED — {count} pending items'
                if lang == 'en'
                else f'File d\'attente ROUGE — {count} items'
            ),
            'body': (
                (f'{employee_email} has {count} unresolved documents. '
                 'Admin: please rebalance the workload.')
                if lang == 'en'
                else (
                    f'{employee_email} a {count} documents non résolus. '
                    'Administrateur : rééquilibrez la charge de travail.'
                )
            ),
        }
    # admin daily
    return {
        'subject': (
            f'Workload imbalance — {employee_email} has {count} items'
            if lang == 'en'
            else (
                f'Déséquilibre de charge — {employee_email} a {count} items'
            )
        ),
        'body': (
            f'{employee_email} has {count} unresolved documents — '
            'this exceeds the 100-item daily reminder threshold.'
            if lang == 'en'
            else (
                f'{employee_email} a {count} documents non résolus — '
                'ce seuil quotidien de 100 items est dépassé.'
            )
        ),
    }

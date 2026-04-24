"""Proactive recurring client reminders.

Real CPA workflow: "Your GST return is due April 30, please send
March bank statement." Today the CPA has to remember to ask every
period. Recurring reminders schedule that ask once and fire it on
every cycle.

Building blocks:

  - ``create_reminder()`` — firm-scoped reminder with a cadence,
    bilingual template, optional target portal user, optional due-
    date offset.
  - ``due_reminders(now)`` — returns reminders whose next-fire date
    has arrived or passed, but are not yet fulfilled for this cycle.
  - ``fire_reminder(id, now)`` — creates a ``client_requests`` row
    (Scope 1.4) for this cycle and advances ``next_fire_date``.
  - ``mark_fulfilled(id, cycle_key)`` — auto-close by the linked
    client_requests row when the client completes the ask.

Cadences:

  - ``once``            — fires once on start_date; auto-archived after.
  - ``weekly``          — every 7 days.
  - ``monthly``         — on a day-of-month (1..28 for safety).
  - ``quarterly``       — on a month-of-quarter + day-of-month.
  - ``annually``        — on month-day.

The fire function is idempotent per (reminder_id, cycle_key) so a
cron that double-ticks (or a restart mid-run) never double-posts.
"""
from __future__ import annotations

import calendar
import json
import logging
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


CADENCE_ONCE = 'once'
CADENCE_WEEKLY = 'weekly'
CADENCE_MONTHLY = 'monthly'
CADENCE_QUARTERLY = 'quarterly'
CADENCE_ANNUALLY = 'annually'
VALID_CADENCES = (CADENCE_ONCE, CADENCE_WEEKLY, CADENCE_MONTHLY,
                  CADENCE_QUARTERLY, CADENCE_ANNUALLY)


STATUS_ACTIVE = 'active'
STATUS_PAUSED = 'paused'
STATUS_ENDED = 'ended'


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso_now(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat(timespec='seconds')


def _today(now: datetime | None = None) -> date:
    return (now or datetime.now(timezone.utc)).date()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def ensure_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recurring_reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                title_fr TEXT NOT NULL,
                title_en TEXT NOT NULL,
                body_fr TEXT,
                body_en TEXT,
                cadence TEXT NOT NULL,
                config_json TEXT,
                target_portal_user_id INTEGER,
                start_date TEXT NOT NULL,
                end_date TEXT,
                next_fire_date TEXT,
                due_offset_days INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                created_by TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recurring_due "
            "ON recurring_reminders(status, next_fire_date)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recurring_reminder_fires (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reminder_id INTEGER NOT NULL,
                cycle_key TEXT NOT NULL,
                fired_at TEXT NOT NULL,
                client_request_id INTEGER,
                fulfilled_at TEXT,
                UNIQUE(reminder_id, cycle_key)
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Cadence helpers
# ---------------------------------------------------------------------------


def _next_date(cadence: str, config: dict, current: date) -> date | None:
    """Advance the schedule by one cycle starting at ``current``.

    ``current`` is inclusive: if the reminder is firing for `current`,
    the next fire should be strictly after.
    """
    if cadence == CADENCE_ONCE:
        return None
    if cadence == CADENCE_WEEKLY:
        return current + timedelta(days=7)
    if cadence == CADENCE_MONTHLY:
        day = int(config.get('day_of_month', current.day))
        day = max(1, min(28, day))
        year, month = current.year, current.month + 1
        if month > 12:
            year, month = year + 1, 1
        return date(year, month, day)
    if cadence == CADENCE_QUARTERLY:
        day = int(config.get('day_of_month', current.day))
        day = max(1, min(28, day))
        year, month = current.year, current.month + 3
        while month > 12:
            year += 1
            month -= 12
        return date(year, month, day)
    if cadence == CADENCE_ANNUALLY:
        month = int(config.get('month', current.month))
        day = int(config.get('day', current.day))
        day = max(1, min(calendar.monthrange(current.year + 1, month)[1],
                         day))
        return date(current.year + 1, month, day)
    return None


def _cycle_key(cadence: str, fire_date: date) -> str:
    """Stable key per cycle — used to de-dupe fire_reminder calls."""
    if cadence == CADENCE_WEEKLY:
        return fire_date.strftime('%G-W%V')
    if cadence == CADENCE_MONTHLY:
        return fire_date.strftime('%Y-%m')
    if cadence == CADENCE_QUARTERLY:
        q = (fire_date.month - 1) // 3 + 1
        return f'{fire_date.year}-Q{q}'
    if cadence == CADENCE_ANNUALLY:
        return fire_date.strftime('%Y')
    return fire_date.strftime('%Y-%m-%d')


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def create_reminder(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    title_fr: str, title_en: str,
    body_fr: str = '', body_en: str = '',
    cadence: str, config: dict | None = None,
    start_date: str, end_date: str | None = None,
    due_offset_days: int = 0,
    target_portal_user_id: int | None = None,
    created_by: str = '',
) -> dict:
    if cadence not in VALID_CADENCES:
        return {'ok': False, 'reason': 'invalid_cadence'}
    if not title_fr.strip() or not title_en.strip():
        return {'ok': False, 'reason': 'title_required_both_languages'}
    ensure_schema(db_path)
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO recurring_reminders "
            "(firm_code, client_code, title_fr, title_en, body_fr, body_en, "
            " cadence, config_json, target_portal_user_id, "
            " start_date, end_date, next_fire_date, due_offset_days, "
            " status, created_by) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (firm_code, client_code, title_fr, title_en, body_fr, body_en,
             cadence, json.dumps(config or {}), target_portal_user_id,
             start_date, end_date, start_date, int(due_offset_days),
             STATUS_ACTIVE, created_by),
        )
        conn.commit()
        return {'ok': True, 'id': cur.lastrowid}


def update_status(
    db_path: Path | str, reminder_id: int, status: str,
) -> dict:
    if status not in (STATUS_ACTIVE, STATUS_PAUSED, STATUS_ENDED):
        return {'ok': False, 'reason': 'invalid_status'}
    ensure_schema(db_path)
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE recurring_reminders SET status=? WHERE id=?",
            (status, reminder_id),
        )
        conn.commit()
    return {'ok': True}


def list_for_client(
    db_path: Path | str, firm_code: str, client_code: str,
) -> list[dict]:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM recurring_reminders "
            "WHERE firm_code=? AND client_code=? "
            "ORDER BY status, next_fire_date",
            (firm_code, client_code),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


def due_reminders(
    db_path: Path | str, *, now: datetime | None = None,
    firm_code: str | None = None,
) -> list[dict]:
    """Active reminders whose next_fire_date is today or earlier."""
    ensure_schema(db_path)
    today = _today(now).isoformat()
    sql = (
        "SELECT * FROM recurring_reminders "
        "WHERE status=? AND next_fire_date IS NOT NULL "
        "AND next_fire_date <= ? "
    )
    params: list[Any] = [STATUS_ACTIVE, today]
    if firm_code is not None:
        sql += "AND firm_code=? "
        params.append(firm_code)
    sql += "ORDER BY next_fire_date"
    with _open(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def fire_reminder(
    db_path: Path | str, reminder_id: int, *,
    now: datetime | None = None,
    post_request: Any = None,
) -> dict:
    """Fire a single reminder.

    Writes a ``recurring_reminder_fires`` row (idempotent per cycle
    via the UNIQUE constraint), optionally calls ``post_request`` to
    create a linked ``client_requests`` row, and advances
    ``next_fire_date``.

    ``post_request`` signature: ``fn(firm_code, client_code, title,
    description, due_date, target_user, created_by) -> request_id``.
    """
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM recurring_reminders WHERE id=?",
            (reminder_id,),
        ).fetchone()
        if not row:
            return {'ok': False, 'reason': 'unknown'}
        if row['status'] != STATUS_ACTIVE:
            return {'ok': False, 'reason': 'inactive'}
        fire_date = _today(now)
        nfd = row['next_fire_date']
        if nfd:
            try:
                nfd_date = date.fromisoformat(nfd)
                if nfd_date < fire_date:
                    fire_date = nfd_date
            except ValueError:
                pass
        cycle = _cycle_key(row['cadence'], fire_date)
        try:
            conn.execute(
                "INSERT INTO recurring_reminder_fires "
                "(reminder_id, cycle_key, fired_at) VALUES (?,?,?)",
                (reminder_id, cycle, _iso_now(now)),
            )
        except sqlite3.IntegrityError:
            return {'ok': False, 'reason': 'already_fired_this_cycle'}
        # Advance next_fire_date based on the cadence.
        try:
            config = json.loads(row['config_json'] or '{}')
        except Exception:
            config = {}
        if row['cadence'] == CADENCE_ONCE:
            conn.execute(
                "UPDATE recurring_reminders SET status=?, next_fire_date=NULL "
                "WHERE id=?", (STATUS_ENDED, reminder_id),
            )
        else:
            nxt = _next_date(row['cadence'], config, fire_date)
            if row['end_date'] and nxt and nxt.isoformat() > row['end_date']:
                conn.execute(
                    "UPDATE recurring_reminders SET status=?, "
                    "next_fire_date=NULL WHERE id=?",
                    (STATUS_ENDED, reminder_id),
                )
            else:
                conn.execute(
                    "UPDATE recurring_reminders SET next_fire_date=? "
                    "WHERE id=?",
                    (nxt.isoformat() if nxt else None, reminder_id),
                )
        conn.commit()
        # Optionally create the linked client_requests row via the
        # caller-supplied helper (Scope 1.4 API).
        req_id = None
        if post_request is not None:
            try:
                due = (fire_date + timedelta(days=row['due_offset_days'])
                       ).isoformat() if row['due_offset_days'] else None
                req_id = post_request(
                    firm_code=row['firm_code'],
                    client_code=row['client_code'],
                    title=row['title_fr'] or row['title_en'],
                    description=row['body_fr'] or row['body_en'] or '',
                    due_date=due,
                    target_user=row['target_portal_user_id'],
                    created_by=f'recurring:{reminder_id}',
                )
                if req_id:
                    conn.execute(
                        "UPDATE recurring_reminder_fires "
                        "SET client_request_id=? "
                        "WHERE reminder_id=? AND cycle_key=?",
                        (req_id, reminder_id, cycle),
                    )
                    conn.commit()
            except Exception:
                log.exception("post_request failed for reminder %s",
                              reminder_id)
    return {'ok': True, 'cycle_key': cycle,
            'client_request_id': req_id,
            'fire_date': fire_date.isoformat()}


def mark_fulfilled(
    db_path: Path | str, reminder_id: int, cycle_key: str,
    now: datetime | None = None,
) -> None:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE recurring_reminder_fires SET fulfilled_at=? "
            "WHERE reminder_id=? AND cycle_key=?",
            (_iso_now(now), reminder_id, cycle_key),
        )
        conn.commit()


def fulfilled_by_request(
    db_path: Path | str, request_id: int,
    now: datetime | None = None,
) -> bool:
    """Wired by Scope 1.4: when a client_requests row gets
    completed, find the linked fire row (if any) and mark it.
    Returns True iff a fire row was matched.
    """
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM recurring_reminder_fires "
            "WHERE client_request_id=? AND fulfilled_at IS NULL",
            (request_id,),
        ).fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE recurring_reminder_fires SET fulfilled_at=? "
            "WHERE id=?", (_iso_now(now), row['id']),
        )
        conn.commit()
    return True


# ---------------------------------------------------------------------------
# Bilingual templates (canned recipes the UI exposes as one-click)
# ---------------------------------------------------------------------------


TEMPLATES = {
    'monthly_bank_statement': {
        'title_fr': 'Relevé bancaire mensuel',
        'title_en': 'Monthly bank statement',
        'body_fr': 'Merci de transmettre votre relevé bancaire du mois '
                   'écoulé.',
        'body_en': 'Please upload last month\'s bank statement.',
        'cadence': CADENCE_MONTHLY,
        'config': {'day_of_month': 15},
    },
    'quarterly_gst_qst': {
        'title_fr': 'TPS / TVQ trimestrielle',
        'title_en': 'Quarterly GST/QST',
        'body_fr': 'Préparation de votre déclaration TPS/TVQ — documents '
                   'requis.',
        'body_en': 'GST/QST filing documents needed for this quarter.',
        'cadence': CADENCE_QUARTERLY,
        'config': {'day_of_month': 15},
    },
    'annual_t4_summary': {
        'title_fr': 'Sommaire T4 annuel',
        'title_en': 'Annual T4 summary',
        'body_fr': 'Veuillez fournir les sommaires T4 avant le 28 février.',
        'body_en': 'Please provide T4 summaries before February 28.',
        'cadence': CADENCE_ANNUALLY,
        'config': {'month': 2, 'day': 28},
    },
    'year_end_docs_request': {
        'title_fr': 'Documents de fin d\'année',
        'title_en': 'Year-end documents request',
        'body_fr': 'Merci de rassembler les documents de fin d\'année.',
        'body_en': 'Please gather year-end documents.',
        'cadence': CADENCE_ANNUALLY,
        'config': {'month': 1, 'day': 15},
    },
}


def create_from_template(
    db_path: Path | str, template_key: str, *,
    firm_code: str, client_code: str, start_date: str,
    end_date: str | None = None, due_offset_days: int = 10,
    target_portal_user_id: int | None = None, created_by: str = '',
) -> dict:
    if template_key not in TEMPLATES:
        return {'ok': False, 'reason': 'unknown_template'}
    t = TEMPLATES[template_key]
    return create_reminder(
        db_path,
        firm_code=firm_code, client_code=client_code,
        title_fr=t['title_fr'], title_en=t['title_en'],
        body_fr=t['body_fr'], body_en=t['body_en'],
        cadence=t['cadence'], config=t['config'],
        start_date=start_date, end_date=end_date,
        due_offset_days=due_offset_days,
        target_portal_user_id=target_portal_user_id,
        created_by=created_by,
    )

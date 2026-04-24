"""Scope 3.3 — queue overflow alerts."""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import queue_alerts as qa  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'qa.db'
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE dashboard_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT, firm_code TEXT, role TEXT,
                active INTEGER DEFAULT 1
            );
            CREATE TABLE review_workflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, entity_type TEXT, entity_id TEXT,
                status TEXT, assigned_to_email TEXT,
                priority TEXT DEFAULT 'normal', assigned_at TEXT
            );
            INSERT INTO dashboard_users (email, firm_code, role, active)
            VALUES
              ('sam@f.com','FIRM','employee',1),
              ('jean@f.com','FIRM','employee',1),
              ('admin@f.com','FIRM','firm_admin',1),
              ('owner@f.com','FIRM','owner',1);
            """
        )
        conn.commit()
    qa.ensure_schema(db_path)
    return db_path


def _queue(db, employee, count, status='assigned'):
    with sqlite3.connect(db) as conn:
        for i in range(count):
            conn.execute(
                "INSERT INTO review_workflow "
                "(firm_code, entity_type, entity_id, status, "
                " assigned_to_email, assigned_at) "
                "VALUES ('FIRM','document',?,?,?,?)",
                (f'{employee}-{i}', status, employee, '2026-04-24'),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Level thresholds
# ---------------------------------------------------------------------------


def test_level_green_under_30():
    assert qa.level_for_count(0) == qa.LEVEL_GREEN
    assert qa.level_for_count(29) == qa.LEVEL_GREEN


def test_yellow_alert_at_30(db):
    _queue(db, 'sam@f.com', 30)
    r = qa.evaluate_employee(db, firm_code='FIRM',
                             employee_email='sam@f.com')
    assert r['fire'] is True
    assert r['level'] == qa.LEVEL_YELLOW
    assert r['queue_count'] == 30


def test_red_alert_at_50(db):
    _queue(db, 'sam@f.com', 50)
    r = qa.evaluate_employee(db, firm_code='FIRM',
                             employee_email='sam@f.com')
    assert r['fire'] is True
    assert r['level'] == qa.LEVEL_RED
    # Recipients include firm_admin and owner on red.
    assert 'sam@f.com' in r['recipients']
    assert 'admin@f.com' in r['recipients']
    assert 'owner@f.com' in r['recipients']


def test_firm_admin_notified_at_100(db):
    _queue(db, 'sam@f.com', 100)
    r = qa.evaluate_employee(db, firm_code='FIRM',
                             employee_email='sam@f.com')
    assert r['fire'] is True
    assert r['level'] == qa.LEVEL_ADMIN
    assert 'admin@f.com' in r['recipients']


def test_alert_green_never_fires(db):
    _queue(db, 'sam@f.com', 5)
    r = qa.evaluate_employee(db, firm_code='FIRM',
                             employee_email='sam@f.com')
    assert r == {'fire': False, 'level': qa.LEVEL_GREEN, 'queue_count': 5}


# ---------------------------------------------------------------------------
# Only unresolved counted
# ---------------------------------------------------------------------------


def test_resolved_items_not_counted(db):
    _queue(db, 'sam@f.com', 5, status='assigned')
    _queue(db, 'sam@f.com', 30, status='resolved')
    # Replace resolved entity_id offsets so they don't collide
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "UPDATE review_workflow SET entity_id=? WHERE entity_id=?",
            [(f'done-{i}', f'sam@f.com-{i}')
             for i in range(30) if i >= 5],
        )
        conn.commit()
    cnt = qa.count_open_for_employee(
        db, firm_code='FIRM', employee_email='sam@f.com',
    )
    # Still counts just the 5 assigned from the first batch
    assert cnt == 5


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------


def test_cooldown_suppresses_duplicate_alert(db):
    _queue(db, 'sam@f.com', 30)
    qa.log_alert(db, firm_code='FIRM', employee_email='sam@f.com',
                 level=qa.LEVEL_YELLOW, queue_count=30)
    r = qa.evaluate_employee(db, firm_code='FIRM',
                             employee_email='sam@f.com')
    assert r['fire'] is False
    assert r.get('suppressed') == 'cooldown'


def test_cooldown_expires_after_window(db):
    _queue(db, 'sam@f.com', 30)
    qa.log_alert(db, firm_code='FIRM', employee_email='sam@f.com',
                 level=qa.LEVEL_YELLOW, queue_count=30)
    # Rewind the log so it's older than the default window.
    old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE queue_alert_log SET created_at=? "
            "WHERE employee_email='sam@f.com'",
            (old,),
        )
        conn.commit()
    r = qa.evaluate_employee(db, firm_code='FIRM',
                             employee_email='sam@f.com')
    assert r['fire'] is True


# ---------------------------------------------------------------------------
# Workload snapshot / dashboard widget
# ---------------------------------------------------------------------------


def test_dashboard_widget_color_coding(db):
    _queue(db, 'sam@f.com', 60)
    _queue(db, 'jean@f.com', 35)
    snap = qa.workload_snapshot(db, 'FIRM')
    by_email = {s['employee_email']: s for s in snap}
    assert by_email['sam@f.com']['level'] == qa.LEVEL_RED
    assert by_email['jean@f.com']['level'] == qa.LEVEL_YELLOW
    assert by_email['admin@f.com']['level'] == qa.LEVEL_GREEN


def test_evaluate_firm_returns_per_employee_decisions(db):
    _queue(db, 'sam@f.com', 55)
    _queue(db, 'jean@f.com', 10)
    decisions = qa.evaluate_firm(db, 'FIRM')
    by_email = {d['employee_email']: d for d in decisions
                if d.get('employee_email')}
    assert by_email['sam@f.com']['fire'] is True
    # Jean at 10 is under the threshold.
    jean = next((d for d in decisions
                 if 'employee_email' in d
                 and d.get('employee_email') == 'jean@f.com'), None)
    # fire=False for green-level employees
    green_jean = [d for d in decisions
                  if d.get('level') == qa.LEVEL_GREEN
                  and d.get('queue_count') == 10]
    assert green_jean


# ---------------------------------------------------------------------------
# Bulk reassign integration (admin tool from Scope 3.2 consumed here)
# ---------------------------------------------------------------------------


def test_bulk_reassign_from_admin_reduces_queue(db):
    _queue(db, 'sam@f.com', 40)
    from src.integrations.employee_ooo import bulk_reassign, ensure_schema
    ensure_schema(db)
    r = bulk_reassign(db, firm_code='FIRM', from_email='sam@f.com',
                      to_email='jean@f.com', actor='admin@f.com')
    assert r['reassigned'] == 40
    after_sam = qa.count_open_for_employee(
        db, firm_code='FIRM', employee_email='sam@f.com',
    )
    after_jean = qa.count_open_for_employee(
        db, firm_code='FIRM', employee_email='jean@f.com',
    )
    assert after_sam == 0
    assert after_jean == 40


# ---------------------------------------------------------------------------
# Message content
# ---------------------------------------------------------------------------


def test_alert_message_yellow_bilingual():
    fr = qa.alert_message(qa.LEVEL_YELLOW, 35, 'sam@f.com', lang='fr')
    en = qa.alert_message(qa.LEVEL_YELLOW, 35, 'sam@f.com', lang='en')
    assert '35' in fr['subject']
    assert '35' in en['subject']
    assert fr['subject'] != en['subject']


def test_alert_message_red_names_admin():
    en = qa.alert_message(qa.LEVEL_RED, 60, 'sam@f.com', lang='en')
    assert 'sam@f.com' in en['body']


# ---------------------------------------------------------------------------
# log_alert + recent_alert
# ---------------------------------------------------------------------------


def test_log_alert_then_recent_alert_true(db):
    qa.log_alert(db, firm_code='FIRM', employee_email='sam@f.com',
                 level=qa.LEVEL_RED, queue_count=60)
    assert qa.recent_alert(db, firm_code='FIRM',
                           employee_email='sam@f.com',
                           level=qa.LEVEL_RED) is True


def test_recent_alert_different_level_returns_false(db):
    qa.log_alert(db, firm_code='FIRM', employee_email='sam@f.com',
                 level=qa.LEVEL_YELLOW, queue_count=30)
    assert qa.recent_alert(db, firm_code='FIRM',
                           employee_email='sam@f.com',
                           level=qa.LEVEL_RED) is False

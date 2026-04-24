"""Scope 3.4 — recurring client reminders."""
from __future__ import annotations

import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import recurring_reminders as rr  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'rr.db'
    rr.ensure_schema(db_path)
    return db_path


# ---------------------------------------------------------------------------
# Cadence advance logic
# ---------------------------------------------------------------------------


def test_next_date_weekly():
    assert rr._next_date('weekly', {}, date(2026, 4, 24)) \
        == date(2026, 5, 1)


def test_next_date_monthly_uses_day_of_month():
    assert rr._next_date('monthly', {'day_of_month': 15},
                         date(2026, 4, 15)) == date(2026, 5, 15)


def test_next_date_monthly_rolls_december():
    assert rr._next_date('monthly', {'day_of_month': 1},
                         date(2026, 12, 1)) == date(2027, 1, 1)


def test_next_date_quarterly_jumps_three_months():
    assert rr._next_date('quarterly', {'day_of_month': 15},
                         date(2026, 1, 15)) == date(2026, 4, 15)


def test_next_date_annually_adds_a_year():
    assert rr._next_date('annually', {'month': 2, 'day': 28},
                         date(2026, 2, 28)) == date(2027, 2, 28)


def test_next_date_once_terminates():
    assert rr._next_date('once', {}, date(2026, 4, 24)) is None


def test_cycle_key_monthly_includes_year_and_month():
    assert rr._cycle_key('monthly', date(2026, 4, 15)) == '2026-04'


def test_cycle_key_quarterly():
    assert rr._cycle_key('quarterly', date(2026, 4, 15)) == '2026-Q2'


def test_cycle_key_weekly_iso_week():
    assert rr._cycle_key('weekly', date(2026, 4, 24)).startswith('2026-W')


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_create_reminder_requires_bilingual_titles(db):
    r = rr.create_reminder(db, firm_code='FIRM', client_code='CLI',
                           title_fr='', title_en='Title',
                           cadence=rr.CADENCE_MONTHLY,
                           start_date='2026-04-24',
                           created_by='o@f.com')
    assert r == {'ok': False, 'reason': 'title_required_both_languages'}


def test_create_reminder_rejects_invalid_cadence(db):
    r = rr.create_reminder(db, firm_code='FIRM', client_code='CLI',
                           title_fr='T', title_en='T',
                           cadence='fortnightly',
                           start_date='2026-04-24',
                           created_by='o@f.com')
    assert r == {'ok': False, 'reason': 'invalid_cadence'}


def test_one_time_reminder_fires_once(db):
    r = rr.create_reminder(db, firm_code='FIRM', client_code='CLI',
                           title_fr='FR', title_en='EN',
                           cadence=rr.CADENCE_ONCE,
                           start_date='2026-04-24',
                           created_by='o@f.com')
    rid = r['id']
    now = datetime(2026, 4, 24, tzinfo=timezone.utc)
    f = rr.fire_reminder(db, rid, now=now)
    assert f['ok'] is True
    # After firing once, the reminder is ended and won't fire again.
    again = rr.fire_reminder(db, rid, now=now)
    assert again == {'ok': False, 'reason': 'inactive'}


def test_recurring_monthly_reminder_advances(db):
    rid = rr.create_reminder(
        db, firm_code='FIRM', client_code='CLI',
        title_fr='Relevé mensuel', title_en='Monthly statement',
        cadence=rr.CADENCE_MONTHLY, config={'day_of_month': 15},
        start_date='2026-04-15',
        created_by='o@f.com',
    )['id']
    now = datetime(2026, 4, 15, tzinfo=timezone.utc)
    rr.fire_reminder(db, rid, now=now)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT next_fire_date FROM recurring_reminders WHERE id=?",
            (rid,),
        ).fetchone()
    assert row['next_fire_date'] == '2026-05-15'


def test_quarterly_reminder_advances_three_months(db):
    rid = rr.create_reminder(
        db, firm_code='FIRM', client_code='CLI',
        title_fr='T', title_en='T',
        cadence=rr.CADENCE_QUARTERLY, config={'day_of_month': 15},
        start_date='2026-01-15',
        created_by='o@f.com',
    )['id']
    rr.fire_reminder(db, rid, now=datetime(2026, 1, 15, tzinfo=timezone.utc))
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT next_fire_date FROM recurring_reminders WHERE id=?",
            (rid,),
        ).fetchone()
    assert row['next_fire_date'] == '2026-04-15'


# ---------------------------------------------------------------------------
# due_reminders scheduler
# ---------------------------------------------------------------------------


def test_due_reminders_returns_active_only(db):
    a = rr.create_reminder(db, firm_code='FIRM', client_code='C1',
                           title_fr='A', title_en='A',
                           cadence=rr.CADENCE_MONTHLY,
                           start_date='2026-04-15',
                           created_by='o@f.com')['id']
    b = rr.create_reminder(db, firm_code='FIRM', client_code='C2',
                           title_fr='B', title_en='B',
                           cadence=rr.CADENCE_MONTHLY,
                           start_date='2026-05-01',
                           created_by='o@f.com')['id']
    rr.update_status(db, b, rr.STATUS_PAUSED)
    due = rr.due_reminders(db, now=datetime(2026, 4, 20,
                                            tzinfo=timezone.utc))
    assert [r['id'] for r in due] == [a]


def test_due_reminders_scoped_by_firm(db):
    rr.create_reminder(db, firm_code='FIRM', client_code='C1',
                       title_fr='A', title_en='A',
                       cadence=rr.CADENCE_MONTHLY,
                       start_date='2026-04-15', created_by='o')
    rr.create_reminder(db, firm_code='OTHER', client_code='X',
                       title_fr='X', title_en='X',
                       cadence=rr.CADENCE_MONTHLY,
                       start_date='2026-04-15', created_by='o')
    due_f = rr.due_reminders(db, now=datetime(2026, 4, 20,
                                              tzinfo=timezone.utc),
                             firm_code='FIRM')
    assert len(due_f) == 1
    assert due_f[0]['firm_code'] == 'FIRM'


# ---------------------------------------------------------------------------
# Fulfillment wiring
# ---------------------------------------------------------------------------


def test_fulfillment_marks_fire_row(db):
    rid = rr.create_reminder(db, firm_code='FIRM', client_code='CLI',
                             title_fr='T', title_en='T',
                             cadence=rr.CADENCE_MONTHLY,
                             start_date='2026-04-15',
                             created_by='o@f.com')['id']
    # Fake post_request: just return a deterministic request id.
    def fake_post(**kwargs):
        return 9999
    rr.fire_reminder(db, rid, now=datetime(2026, 4, 15,
                                           tzinfo=timezone.utc),
                     post_request=fake_post)
    matched = rr.fulfilled_by_request(db, request_id=9999)
    assert matched is True
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT fulfilled_at FROM recurring_reminder_fires "
            "WHERE client_request_id=9999"
        ).fetchone()
    assert row['fulfilled_at'] is not None


def test_fulfillment_with_no_link_returns_false(db):
    assert rr.fulfilled_by_request(db, request_id=9999) is False


# ---------------------------------------------------------------------------
# Idempotency per cycle
# ---------------------------------------------------------------------------


def test_fire_reminder_idempotent_per_cycle(db):
    rid = rr.create_reminder(db, firm_code='FIRM', client_code='CLI',
                             title_fr='T', title_en='T',
                             cadence=rr.CADENCE_MONTHLY,
                             start_date='2026-04-15',
                             created_by='o@f.com')['id']
    now = datetime(2026, 4, 15, tzinfo=timezone.utc)
    rr.fire_reminder(db, rid, now=now)
    # Same cycle via manual restore of next_fire_date
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE recurring_reminders SET next_fire_date='2026-04-15' "
            "WHERE id=?", (rid,),
        )
        conn.commit()
    second = rr.fire_reminder(db, rid, now=now)
    assert second['ok'] is False
    assert second['reason'] == 'already_fired_this_cycle'


# ---------------------------------------------------------------------------
# Templates + bilingual
# ---------------------------------------------------------------------------


def test_bilingual_templates_create_reminder(db):
    r = rr.create_from_template(
        db, 'monthly_bank_statement',
        firm_code='FIRM', client_code='CLI',
        start_date='2026-04-15', created_by='o@f.com',
    )
    assert r['ok'] is True
    reminders = rr.list_for_client(db, 'FIRM', 'CLI')
    assert len(reminders) == 1
    assert reminders[0]['title_fr'] == 'Relevé bancaire mensuel'
    assert reminders[0]['title_en'] == 'Monthly bank statement'


def test_unknown_template_rejected(db):
    r = rr.create_from_template(
        db, 'mystery',
        firm_code='FIRM', client_code='CLI',
        start_date='2026-04-15', created_by='o',
    )
    assert r == {'ok': False, 'reason': 'unknown_template'}


def test_end_date_stops_recurrence(db):
    rid = rr.create_reminder(
        db, firm_code='FIRM', client_code='CLI',
        title_fr='T', title_en='T',
        cadence=rr.CADENCE_MONTHLY, config={'day_of_month': 15},
        start_date='2026-04-15', end_date='2026-05-01',
        created_by='o@f.com',
    )['id']
    rr.fire_reminder(db, rid, now=datetime(2026, 4, 15,
                                           tzinfo=timezone.utc))
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, next_fire_date FROM recurring_reminders "
            "WHERE id=?", (rid,),
        ).fetchone()
    assert row['status'] == rr.STATUS_ENDED


# ---------------------------------------------------------------------------
# Target user
# ---------------------------------------------------------------------------


def test_reminder_target_specific_user(db):
    r = rr.create_reminder(
        db, firm_code='FIRM', client_code='CLI',
        title_fr='T', title_en='T',
        cadence=rr.CADENCE_MONTHLY,
        start_date='2026-04-15',
        target_portal_user_id=42,
        created_by='o@f.com',
    )
    rid = r['id']
    captured = {}
    def fake_post(**kwargs):
        captured.update(kwargs)
        return 1
    rr.fire_reminder(db, rid, now=datetime(2026, 4, 15,
                                           tzinfo=timezone.utc),
                     post_request=fake_post)
    assert captured['target_user'] == 42

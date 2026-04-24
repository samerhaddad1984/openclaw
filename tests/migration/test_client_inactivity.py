"""Scope 3.5 — client inactivity detection."""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import client_inactivity as ci  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'inac.db'
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT, firm_code TEXT,
                status TEXT DEFAULT 'active',
                created_at TEXT
            );
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT UNIQUE, client_code TEXT,
                created_at TEXT
            );
            CREATE TABLE client_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, created_at TEXT
            );
            CREATE TABLE client_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, created_at TEXT, completed_at TEXT
            );
            INSERT INTO clients (client_code, client_name, firm_code,
                                 status, created_at) VALUES
              ('ACTIVE','Active','FIRM','active','2024-01-01T00:00:00+00:00'),
              ('DORM','Dormant','FIRM','active','2023-01-01T00:00:00+00:00'),
              ('ARCH','Archived','FIRM','archived','2023-01-01T00:00:00+00:00'),
              ('FRESH','Fresh no activity','FIRM','active',
               '2026-04-01T00:00:00+00:00');
            """
        )
        conn.commit()
    return db_path


NOW = datetime(2026, 4, 24, tzinfo=timezone.utc)


def _add_doc(db, client, days_ago):
    stamp = (NOW - timedelta(days=days_ago)).isoformat(timespec='seconds')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, created_at) "
            "VALUES (?,?,?)",
            (f'{client}-{days_ago}', client, stamp),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# last_activity_for
# ---------------------------------------------------------------------------


def test_last_activity_uses_most_recent(db):
    _add_doc(db, 'ACTIVE', 10)
    _add_doc(db, 'ACTIVE', 60)
    latest = ci.last_activity_for(db, 'ACTIVE', now=NOW)
    assert latest is not None
    assert (NOW - latest).days == 10


def test_last_activity_falls_back_across_tables(db):
    # No documents but a message 30 days ago.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO client_messages (client_code, created_at) "
            "VALUES ('ACTIVE',?)",
            ((NOW - timedelta(days=30)).isoformat(timespec='seconds'),),
        )
        conn.commit()
    latest = ci.last_activity_for(db, 'ACTIVE', now=NOW)
    assert (NOW - latest).days == 30


def test_last_activity_none_when_no_records(db):
    assert ci.last_activity_for(db, 'FRESH', now=NOW) is None


# ---------------------------------------------------------------------------
# inactivity detection
# ---------------------------------------------------------------------------


def test_inactivity_detection_triggers_at_90_days(db):
    _add_doc(db, 'ACTIVE', 10)   # not inactive
    _add_doc(db, 'DORM', 120)    # inactive > 90
    inactive = ci.inactive_clients(db, firm_code='FIRM', days=90, now=NOW)
    codes = [c['client_code'] for c in inactive]
    assert 'DORM' in codes
    assert 'ACTIVE' not in codes


def test_never_active_clients_surfaced(db):
    # FRESH has no records — created 23 days ago.
    inactive = ci.inactive_clients(db, firm_code='FIRM', days=90, now=NOW)
    fresh_entry = [c for c in inactive if c['client_code'] == 'FRESH']
    # FRESH was created 23 days ago so it's not over threshold on
    # activity *time*, but last_activity_for returns None. We surface
    # never-active clients only when their creation is older than
    # threshold; otherwise they're treated as "fresh and fine".
    # Here created_at = 2026-04-01 → 23 days ago → under threshold.
    # So FRESH should not appear.
    assert fresh_entry == []


def test_never_active_old_clients_surfaced(db):
    # DORM has no documents/messages — created 2023-01-01. Its
    # effective last activity is created_at, which is > 90 days.
    inactive = ci.inactive_clients(db, firm_code='FIRM', days=90, now=NOW)
    dorm = [c for c in inactive if c['client_code'] == 'DORM']
    assert len(dorm) == 1
    assert dorm[0]['days_inactive'] is not None
    assert dorm[0]['days_inactive'] >= 90


def test_archived_clients_excluded(db):
    _add_doc(db, 'ARCH', 365)
    inactive = ci.inactive_clients(db, firm_code='FIRM', days=90, now=NOW)
    assert all(c['client_code'] != 'ARCH' for c in inactive)


def test_custom_threshold_days(db):
    _add_doc(db, 'ACTIVE', 40)
    # At 30-day threshold, ACTIVE is inactive (40 > 30).
    inactive = ci.inactive_clients(db, firm_code='FIRM', days=30, now=NOW)
    codes = [c['client_code'] for c in inactive]
    assert 'ACTIVE' in codes


def test_scope_to_firm(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, client_name, firm_code, "
            "status, created_at) VALUES "
            "('OTR','Other','OTHER','active','2023-01-01T00:00:00+00:00')"
        )
        conn.commit()
    inactive = ci.inactive_clients(db, firm_code='FIRM', days=90, now=NOW)
    codes = [c['client_code'] for c in inactive]
    assert 'OTR' not in codes


# ---------------------------------------------------------------------------
# CPA alerted
# ---------------------------------------------------------------------------


def test_cpa_alerted_of_at_risk(db):
    _add_doc(db, 'DORM', 120)  # dormant
    alerted = []

    def notifier(firm_code, summary):
        alerted.append({'firm': firm_code, 'count': summary['total']})

    ci.weekly_scan(db, firm_code='FIRM', notifier=notifier, now=NOW)
    assert alerted
    assert alerted[0]['firm'] == 'FIRM'
    assert alerted[0]['count'] >= 1


def test_weekly_scan_no_notifier_still_returns_summary(db):
    _add_doc(db, 'DORM', 120)
    reports = ci.weekly_scan(db, firm_code='FIRM', now=NOW)
    assert 'FIRM' in reports
    assert reports['FIRM']['total'] >= 1


def test_weekly_scan_all_firms_when_firm_code_none(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, client_name, firm_code, "
            "status, created_at) VALUES "
            "('OTR','Other','OTHER','active','2023-01-01T00:00:00+00:00')"
        )
        conn.commit()
    reports = ci.weekly_scan(db, now=NOW)
    # FIRM and OTHER both present
    assert set(reports.keys()) >= {'FIRM', 'OTHER'}


# ---------------------------------------------------------------------------
# Dashboard widget payload
# ---------------------------------------------------------------------------


def test_dashboard_widget_payload_shape(db):
    _add_doc(db, 'DORM', 120)
    summary = ci.at_risk_summary(db, 'FIRM', now=NOW)
    assert summary['days'] == 90
    assert set(summary.keys()) >= {
        'days', 'total', 'never_active_count',
        'inactive_over_threshold', 'at_risk_client_codes',
    }
    assert 'DORM' in summary['at_risk_client_codes']


def test_at_risk_summary_excludes_active_within_threshold(db):
    _add_doc(db, 'ACTIVE', 10)
    summary = ci.at_risk_summary(db, 'FIRM', now=NOW)
    assert 'ACTIVE' not in summary['at_risk_client_codes']

"""Scope 1 Phase 4 — outstanding CPA requests tracker.

Covers:

  - CPA can create a request (title, description, due date, target).
  - A portal user sees requests targeted at them OR team-wide.
  - A request targeted at a specific user is hidden from other users.
  - Fulfilling a request enqueues a notification back to the CPA.
  - Overdue reminders fire once per cooldown window and are
    recorded so the next run doesn't spam.

All against in-memory SQLite fixtures via the pure-Python surface.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import client_requests as cr  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'req.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT,
                portal_mode TEXT DEFAULT 'multi',
                active INTEGER DEFAULT 1
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                role TEXT NOT NULL, user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                UNIQUE(firm_code, client_code, email)
            );
            """
        )
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name) "
            "VALUES ('CONS','FIRM','Construction Tremblay')"
        )
        conn.execute(
            "INSERT INTO client_portal_users "
            "(firm_code, client_code, email, full_name, role, user_token, "
            "status) VALUES "
            "('FIRM','CONS','admin@cons.com','Admin','admin','t-a','active'),"
            "('FIRM','CONS','bk@cons.com','Bookkeeper','contributor','t-b',"
            "'active')"
        )
        conn.commit()
    return db


def test_cpa_creates_request(tmp_path):
    db = _mkdb(tmp_path)
    rid = cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Send March bank statement', due_date='2026-04-30',
        created_by_email='cpa@firm.com',
    )
    assert rid > 0
    reqs = cr.list_open_for_client(db, firm_code='FIRM', client_code='CONS')
    assert len(reqs) == 1
    assert reqs[0]['title'] == 'Send March bank statement'
    assert reqs[0]['status'] == 'open'
    assert reqs[0]['due_date'] == '2026-04-30'


def test_client_sees_request_in_portal(tmp_path):
    db = _mkdb(tmp_path)
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Confirm Costco receipt is personal',
    )
    seen = cr.list_open_for_user(
        db, firm_code='FIRM', client_code='CONS', portal_user_id=2,
    )
    assert len(seen) == 1
    assert seen[0]['title'].startswith('Confirm Costco')
    # The rendered HTML contains the request title and a complete button.
    html = cr.render_client_tasks_page(
        client={'client_code': 'CONS', 'client_name': 'Construction'},
        user_token='t-b',
        portal_user={'id': 2, 'role': 'contributor',
                     'firm_code': 'FIRM', 'client_code': 'CONS'},
        requests=seen,
    )
    assert 'Confirm Costco' in html
    assert '/cp/t-b/tasks/' in html
    assert 'Mark complete' in html or 'Marquer complété' in html


def test_request_can_target_specific_user(tmp_path):
    db = _mkdb(tmp_path)
    # Targeted at user 1 only.
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Sign T4 summary', target_portal_user_id=1,
    )
    # Team-wide (no target).
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Upload any outstanding receipts',
    )
    for_admin = cr.list_open_for_user(
        db, firm_code='FIRM', client_code='CONS', portal_user_id=1,
    )
    for_bk = cr.list_open_for_user(
        db, firm_code='FIRM', client_code='CONS', portal_user_id=2,
    )
    titles_admin = {r['title'] for r in for_admin}
    titles_bk = {r['title'] for r in for_bk}
    assert 'Sign T4 summary' in titles_admin
    assert 'Upload any outstanding receipts' in titles_admin
    # The targeted request must NOT appear for the other user.
    assert 'Sign T4 summary' not in titles_bk
    assert 'Upload any outstanding receipts' in titles_bk


def test_fulfillment_notifies_cpa(tmp_path):
    db = _mkdb(tmp_path)
    rid = cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Send March bank statement',
        created_by_email='cpa@firm.com',
    )
    updated = cr.mark_completed(
        db, request_id=rid, completed_by_portal_user_id=2,
    )
    assert updated and updated['status'] == 'completed'
    # Idempotent — a second mark should not error.
    again = cr.mark_completed(db, request_id=rid)
    assert again and again['status'] == 'completed'
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = list(conn.execute(
            "SELECT kind, recipient_email, body, priority "
            "FROM client_notifications WHERE kind='client_request_fulfilled'"
        ))
    # Only one notification, despite two mark_completed calls (idempotent).
    assert len(rows) == 1
    r = rows[0]
    assert r['recipient_email'] == 'cpa@firm.com'
    body = r['body'] or ''
    # Bilingual body present.
    assert 'marquée complétée' in body or 'was marked complete' in body


def test_overdue_requests_reminder(tmp_path):
    db = _mkdb(tmp_path)
    # Create an overdue request: due yesterday.
    from datetime import date, timedelta
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Overdue: April payroll records',
        due_date=yesterday, created_by_email='cpa@firm.com',
    )
    # Not-yet-due request should be ignored.
    future = (date.today() + timedelta(days=7)).isoformat()
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Upcoming: May tax reminder',
        due_date=future,
    )
    overdue = cr.overdue_requests(db, firm_code='FIRM')
    assert len(overdue) == 1
    assert overdue[0]['title'].startswith('Overdue:')

    first = cr.send_overdue_reminders(db, firm_code='FIRM')
    assert first == 1
    # Cooldown: immediate re-run sends nothing.
    second = cr.send_overdue_reminders(db, firm_code='FIRM', cooldown_hours=24)
    assert second == 0
    # A notification row should exist.
    with sqlite3.connect(db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM client_notifications "
            "WHERE kind='client_request_overdue'"
        ).fetchone()[0]
    assert n == 1


def test_renderer_highlights_overdue(tmp_path):
    db = _mkdb(tmp_path)
    from datetime import date, timedelta
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Send March bank statement', due_date=yesterday,
    )
    reqs = cr.list_open_for_client(db, firm_code='FIRM', client_code='CONS')
    html = cr.render_client_tasks_page(
        client={'client_code': 'CONS', 'client_name': 'Construction'},
        user_token='t-b',
        portal_user={'id': 2, 'role': 'contributor',
                     'firm_code': 'FIRM', 'client_code': 'CONS'},
        requests=reqs,
    )
    # The overdue date renders on the page with its date string.
    assert yesterday in html


def test_cpa_page_renders_with_users_and_requests(tmp_path):
    db = _mkdb(tmp_path)
    cr.create_request(
        db, firm_code='FIRM', client_code='CONS',
        title='Please send receipts',
    )
    reqs = cr.list_open_for_client(
        db, firm_code='FIRM', client_code='CONS',
        include_completed=True,
    )
    html = cr.render_cpa_requests_page(
        firm_code='FIRM', client_code='CONS',
        client_name='Construction Tremblay',
        requests=reqs,
        portal_users=[
            {'id': 1, 'email': 'admin@cons.com', 'full_name': 'Admin'},
            {'id': 2, 'email': 'bk@cons.com', 'full_name': 'Bookkeeper'},
        ],
    )
    assert 'Please send receipts' in html
    # Users appear in the target <select>.
    assert 'Bookkeeper' in html
    assert 'action="/clients/requests"' in html
    assert 'name="client_code" value="CONS"' in html

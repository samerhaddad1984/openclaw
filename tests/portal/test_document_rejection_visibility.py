"""Scope 1 Phase 3 — document rejection visibility to the uploader.

Verifies:

  - /cp/{user_token}/my_uploads lists the user's documents with a
    derived status (processing / approved / rejected / needs_info).
  - When a doc is rejected, the CPA's reason is surfaced on the
    uploader's page.
  - A rejection enqueues a notification to the uploader's email,
    routed via ``notification_sender.client_notifications``.
  - The uploader page shows a "re-upload corrected version" link
    that points back to the portal upload page.
  - The admin of a client sees a firm/client-wide rejection summary
    on the same page (admin-only card).

Tests run against in-memory SQLite fixtures; they exercise the pure
Python surface in ``portal_my_uploads`` + ``review_workflow`` without
spinning up the HTTP server.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest  # noqa: F401 — pytest discovery

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import portal_my_uploads as pmu  # noqa: E402
from src.integrations import review_workflow as rw    # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mkdb(tmp_path):
    db = tmp_path / 'rej.db'
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
                last_active_at TEXT, upload_count INTEGER DEFAULT 0,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                uploaded_by_portal_user_id INTEGER,
                uploader_name TEXT, uploader_email TEXT,
                vendor TEXT, amount REAL,
                document_date TEXT, uploaded_at TEXT,
                file_name TEXT,
                review_status TEXT,
                manual_hold_reason TEXT
            );
            CREATE TABLE review_workflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                status TEXT NOT NULL,
                priority TEXT,
                assigned_to_email TEXT,
                reviewed_by_email TEXT,
                reviewed_at TEXT,
                review_notes TEXT,
                rejection_reason TEXT,
                last_review_at TEXT,
                last_reviewer_email TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(firm_code, entity_type, entity_id)
            );
            CREATE TABLE review_workflow_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id INTEGER,
                actor_email TEXT, actor_role TEXT,
                action TEXT, from_status TEXT, to_status TEXT,
                notes TEXT, created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients(client_code, firm_code, client_name) "
            "VALUES ('CONS','FIRM','Construction Tremblay')"
        )
        # Two portal users on the same client: one admin, one contributor.
        conn.execute(
            "INSERT INTO client_portal_users "
            "(firm_code, client_code, email, full_name, role, user_token, "
            "status) VALUES "
            "('FIRM','CONS','admin@cons.com','Admin User','admin',"
            "'tok-admin','active'),"
            "('FIRM','CONS','uploader@cons.com','Bookkeeper','contributor',"
            "'tok-uploader','active')"
        )
        # One document uploaded by the contributor, in submitted state.
        conn.execute(
            "INSERT INTO documents "
            "(document_id, firm_code, client_code, "
            " uploaded_by_portal_user_id, uploader_name, uploader_email, "
            " vendor, amount, document_date, uploaded_at, file_name, "
            " review_status) VALUES "
            "('D1','FIRM','CONS',2,'Bookkeeper','uploader@cons.com',"
            "'Home Depot', 145.22, '2026-04-10', '2026-04-10 09:00:00',"
            "'receipt.pdf','submitted')"
        )
        conn.execute(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, assigned_to_email)"
            " VALUES ('FIRM','document','D1','submitted','emp@firm.com')"
        )
        conn.commit()
    return db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_uploader_sees_rejection_reason(tmp_path):
    db = _mkdb(tmp_path)
    # CPA rejects with a reason.
    rw.reject(
        db, firm_code='FIRM', entity_type='document', entity_id='D1',
        actor_email='cpa@firm.com', actor_role='firm_admin',
        reason='Missing HST breakdown on the receipt.',
    )
    uploads = pmu.my_uploads(db, user_id=2)
    assert len(uploads) == 1
    assert uploads[0]['status'] == 'rejected'
    assert 'HST' in (uploads[0]['rejection_reason'] or '')


def test_uploader_notified_on_rejection(tmp_path):
    db = _mkdb(tmp_path)
    rw.reject(
        db, firm_code='FIRM', entity_type='document', entity_id='D1',
        actor_email='cpa@firm.com', actor_role='firm_admin',
        reason='Please resend with line totals.',
    )
    # A notification row should exist for the uploader.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = list(conn.execute(
            "SELECT kind, recipient_email, body, priority, status "
            "FROM client_notifications "
            "WHERE kind='document_rejected' AND client_code='CONS'"
        ))
    assert len(rows) == 1
    r = rows[0]
    assert r['recipient_email'] == 'uploader@cons.com'
    assert r['status'] == 'pending'
    assert 'line totals' in (r['body'] or '')
    # Bilingual: both languages are present.
    body = r['body'] or ''
    assert 'Bonjour' in body and 'Hello' in body


def test_uploader_can_reupload_corrected(tmp_path):
    db = _mkdb(tmp_path)
    rw.reject(
        db, firm_code='FIRM', entity_type='document', entity_id='D1',
        actor_email='cpa@firm.com', actor_role='firm_admin',
        reason='Bad scan.',
    )
    uploads = pmu.my_uploads(db, user_id=2)
    portal_user = {'id': 2, 'role': 'contributor',
                   'firm_code': 'FIRM', 'client_code': 'CONS'}
    html = pmu.render_my_uploads_page(
        client={'client_code': 'CONS', 'client_name': 'Construction'},
        user_token='tok-uploader',
        portal_user=portal_user, uploads=uploads,
    )
    # Re-upload link points back to the upload page for this user's token.
    assert '/cp/tok-uploader/upload' in html
    # Rejection reason surfaces on the page.
    assert 'Bad scan.' in html
    # Bilingual re-upload CTA.
    assert 'Re-téléverser' in html or 'Re-upload' in html


def test_admin_sees_team_rejections_summary(tmp_path):
    db = _mkdb(tmp_path)
    rw.reject(
        db, firm_code='FIRM', entity_type='document', entity_id='D1',
        actor_email='cpa@firm.com', actor_role='firm_admin',
        reason='Need clearer photo.',
    )
    team = pmu.team_rejections(db, firm_code='FIRM', client_code='CONS')
    assert len(team) == 1
    assert team[0]['rejection_reason'] == 'Need clearer photo.'
    admin_user = {'id': 1, 'role': 'admin',
                  'firm_code': 'FIRM', 'client_code': 'CONS'}
    html = pmu.render_my_uploads_page(
        client={'client_code': 'CONS', 'client_name': 'Construction'},
        user_token='tok-admin',
        portal_user=admin_user,
        uploads=pmu.my_uploads(db, user_id=1),
        team_rejections=team,
    )
    assert 'Team rejections' in html or 'Rejets équipe' in html
    assert 'Need clearer photo.' in html
    assert 'uploader@cons.com' in html or 'Bookkeeper' in html


def test_contributor_page_hides_team_rejections(tmp_path):
    db = _mkdb(tmp_path)
    rw.reject(
        db, firm_code='FIRM', entity_type='document', entity_id='D1',
        actor_email='cpa@firm.com', actor_role='firm_admin',
        reason='Missing tax line.',
    )
    contributor = {'id': 2, 'role': 'contributor',
                   'firm_code': 'FIRM', 'client_code': 'CONS'}
    html = pmu.render_my_uploads_page(
        client={'client_code': 'CONS', 'client_name': 'Construction'},
        user_token='tok-uploader',
        portal_user=contributor,
        uploads=pmu.my_uploads(db, user_id=2),
        team_rejections=None,  # caller doesn't pass it for non-admins
    )
    assert 'Team rejections' not in html and 'Rejets équipe' not in html


def test_rejection_notify_is_best_effort(tmp_path, monkeypatch):
    """If the notification queue blows up, rejection still lands."""
    db = _mkdb(tmp_path)

    def _boom(*a, **kw):
        raise RuntimeError('queue down')

    monkeypatch.setattr(
        'src.integrations.portal_my_uploads.notify_uploader_on_rejection',
        _boom,
    )
    # Rejection should still succeed.
    result = rw.reject(
        db, firm_code='FIRM', entity_type='document', entity_id='D1',
        actor_email='cpa@firm.com', actor_role='firm_admin',
        reason='Blurry.',
    )
    assert result.get('status') == 'rejected'

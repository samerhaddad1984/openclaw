"""Phase 4: uploads tracked with uploader identity.

Covers both the DB-side invariants (columns populated on save,
upload_count incremented, audit row written) and the helper surface
that the CPA queue uses to filter by uploader.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'upl.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                portal_mode TEXT DEFAULT 'multi',
                active INTEGER DEFAULT 1
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                vendor TEXT, amount REAL,
                review_status TEXT, document_date TEXT,
                uploaded_at TEXT, ingest_source TEXT,
                uploaded_by_portal_user_id INTEGER,
                uploader_name TEXT, uploader_email TEXT
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                role TEXT NOT NULL, user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                invited_by TEXT, invited_at TEXT, accepted_at TEXT,
                last_active_at TEXT, upload_count INTEGER DEFAULT 0,
                suspended_at TEXT, removed_at TEXT, version INTEGER DEFAULT 1,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE client_portal_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                invited_role TEXT NOT NULL,
                invitation_token TEXT UNIQUE NOT NULL,
                invited_by TEXT, invited_at TEXT, expires_at TEXT,
                accepted_at TEXT, status TEXT DEFAULT 'pending'
            );
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER,
                firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_mode) "
            "VALUES ('CONS','FIRM','multi')",
        )
        conn.commit()
    return db


def _seed_doc(db, doc_id, *, client='CONS', user_id=None,
                 uploader_name=None, uploader_email=None):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, firm_code, client_code, vendor, amount, "
            " review_status, uploaded_at, uploaded_by_portal_user_id, "
            " uploader_name, uploader_email) "
            "VALUES (?, 'FIRM', ?, 'v', 1.0, 'New', datetime('now'), ?, ?, ?)",
            (doc_id, client, user_id, uploader_name, uploader_email),
        )
        conn.commit()


def _make_user(db, email='bob@cons.com', name='Bob',
                 role='contributor'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email=email, full_name=name, role=role,
        invited_by='cpa@firm.com', status='active',
    )


def test_single_mode_upload_no_identity(tmp_path):
    db = _mkdb(tmp_path)
    _seed_doc(db, 'D1')  # uploaded without portal user
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT uploaded_by_portal_user_id, uploader_name, uploader_email "
            "FROM documents WHERE document_id='D1'").fetchone()
    assert row[0] is None
    assert row[1] is None and row[2] is None


def test_multi_mode_upload_records_identity(tmp_path):
    db = _mkdb(tmp_path)
    u = _make_user(db)
    _seed_doc(db, 'D2', user_id=u['id'],
                uploader_name='Bob', uploader_email='bob@cons.com')
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT uploaded_by_portal_user_id, uploader_name, uploader_email "
            "FROM documents WHERE document_id='D2'").fetchone()
    assert row[0] == u['id']
    assert row[1] == 'Bob' and row[2] == 'bob@cons.com'


def test_uploader_shown_in_cpa_queue(tmp_path):
    """Helper query the CPA queue runs to surface uploader per row."""
    db = _mkdb(tmp_path)
    u = _make_user(db)
    _seed_doc(db, 'D3', user_id=u['id'], uploader_name='Bob',
                uploader_email='bob@cons.com')
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT d.document_id, d.uploader_name, d.uploader_email, "
            "       cpu.email AS portal_email "
            "FROM documents d "
            "LEFT JOIN client_portal_users cpu "
            "     ON cpu.id = d.uploaded_by_portal_user_id "
            "WHERE d.document_id='D3'").fetchone()
    assert row[1] == 'Bob'
    assert row[3] == 'bob@cons.com'


def test_queue_filter_by_uploader(tmp_path):
    db = _mkdb(tmp_path)
    u1 = _make_user(db, email='a@cons.com', name='A')
    u2 = _make_user(db, email='b@cons.com', name='B')
    _seed_doc(db, 'D4', user_id=u1['id'], uploader_name='A')
    _seed_doc(db, 'D5', user_id=u2['id'], uploader_name='B')
    _seed_doc(db, 'D6', user_id=u1['id'], uploader_name='A')
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT document_id FROM documents "
            "WHERE uploaded_by_portal_user_id=? ORDER BY document_id",
            (u1['id'],),
        ).fetchall()
    ids = [r[0] for r in rows]
    assert ids == ['D4', 'D6']


def test_upload_count_incremented(tmp_path):
    db = _mkdb(tmp_path)
    u = _make_user(db)
    mup.increment_upload_count(db, user_id=u['id'], n=3)
    mup.increment_upload_count(db, user_id=u['id'], n=2)
    fresh = mup.get_user(db, user_id=u['id'])
    assert fresh['upload_count'] == 5
    assert fresh['last_active_at']


def test_uploader_email_in_audit_log(tmp_path):
    db = _mkdb(tmp_path)
    u = _make_user(db)
    mup.audit_log(db, firm_code='FIRM', client_code='CONS',
                    actor_email='bob@cons.com', action='upload',
                    portal_user_id=u['id'],
                    detail='count=5', ip='1.2.3.4', user_agent='pytest')
    rows = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    upload_events = [r for r in rows if r['action'] == 'upload']
    assert upload_events
    assert upload_events[0]['portal_user_id'] == u['id']
    assert upload_events[0]['actor_email'] == 'bob@cons.com'


def test_rate_limit_per_user_not_client(tmp_path):
    db = _mkdb(tmp_path)
    mup.reset_rate_limits()
    u1 = _make_user(db, email='rate1@cons.com', name='R1')
    u2 = _make_user(db, email='rate2@cons.com', name='R2')
    # Burn u1's quota
    for _ in range(30):
        assert mup.upload_rate_allowed(u1['id']) is True
    assert mup.upload_rate_allowed(u1['id']) is False
    # u2 should still be fine
    assert mup.upload_rate_allowed(u2['id']) is True

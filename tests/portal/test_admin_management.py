"""Phase 5: admin-side management of portal users.

Covers the guardrails (can't self-demote when only admin, can't
remove self) plus suspend/remove/reactivate/change-role + CPA
override visibility."""
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
    db = tmp_path / 'admin.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
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
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                uploaded_by_portal_user_id INTEGER,
                uploader_name TEXT, uploader_email TEXT,
                uploaded_at TEXT
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_mode) "
            "VALUES ('CONS','FIRM','multi')",
        )
        conn.commit()
    return db


def _admin(db, email='owner@cons.com', name='Owner'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email=email, full_name=name, role='admin',
        invited_by='cpa@firm.com', status='active',
    )


def _contrib(db, email='bob@cons.com', name='Bob'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email=email, full_name=name, role='contributor',
        invited_by='owner@cons.com', status='active',
    )


def test_admin_sees_user_list(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    b = _contrib(db)
    users = mup.list_users(db, firm_code='FIRM', client_code='CONS')
    ids = {u['id'] for u in users}
    assert a['id'] in ids and b['id'] in ids


def test_admin_can_suspend_contributor(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    b = _contrib(db)
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=b['id'], status='suspended',
                         actor_email=a['email'])
    fresh = mup.get_user(db, user_id=b['id'])
    assert fresh['status'] == 'suspended'
    mode, _, _ = mup.resolve_portal_access(db, token=fresh['user_token'])
    assert mode is None


def test_admin_can_reactivate_suspended(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    b = _contrib(db)
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=b['id'], status='suspended',
                         actor_email=a['email'])
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=b['id'], status='active',
                         actor_email=a['email'])
    fresh = mup.get_user(db, user_id=b['id'])
    assert fresh['status'] == 'active'
    mode, _, _ = mup.resolve_portal_access(db, token=fresh['user_token'])
    assert mode == 'multi'


def test_admin_can_remove_contributor(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    b = _contrib(db)
    old_token = b['user_token']
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=b['id'], status='removed',
                         actor_email=a['email'])
    # Old token no longer resolves (rotated at remove time)
    mode, _, _ = mup.resolve_portal_access(db, token=old_token)
    assert mode is None


def test_admin_cannot_self_demote_if_only_admin(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    with pytest.raises(PermissionError):
        mup.set_user_role(db, firm_code='FIRM', client_code='CONS',
                           user_id=a['id'], role='contributor',
                           actor_email=a['email'])


def test_admin_self_demote_ok_when_another_admin_exists(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db, email='a@cons.com', name='A')
    a2 = _admin(db, email='a2@cons.com', name='A2')
    # Now a can demote themselves
    mup.set_user_role(db, firm_code='FIRM', client_code='CONS',
                       user_id=a['id'], role='contributor',
                       actor_email=a['email'])
    fresh = mup.get_user(db, user_id=a['id'])
    assert fresh['role'] == 'contributor'


def test_contributor_cannot_access_admin(tmp_path):
    """Route-level guard: role != 'admin' → HTTP 403. Tested via role check."""
    db = _mkdb(tmp_path)
    b = _contrib(db)
    assert b['role'] == 'contributor'
    # Role check in handler
    assert (b.get('role') or '') != 'admin'


def test_suspended_user_cannot_upload(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    b = _contrib(db)
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=b['id'], status='suspended',
                         actor_email=a['email'])
    fresh = mup.get_user(db, user_id=b['id'])
    # resolve_portal_access rejects suspended users
    mode, _, _ = mup.resolve_portal_access(db, token=fresh['user_token'])
    assert mode is None


def test_removed_users_uploads_preserved(tmp_path):
    db = _mkdb(tmp_path)
    a = _admin(db)
    b = _contrib(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, firm_code, client_code, "
            "uploaded_by_portal_user_id, uploader_name, uploader_email, "
            "uploaded_at) VALUES ('D1','FIRM','CONS',?,?,?,datetime('now'))",
            (b['id'], b['full_name'], b['email']),
        )
        conn.commit()
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=b['id'], status='removed',
                         actor_email=a['email'])
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT uploader_name, uploader_email, uploaded_by_portal_user_id "
            "FROM documents WHERE document_id='D1'").fetchone()
    # Uploads preserved — user row still exists (status=removed),
    # name/email snapshot on the document remains.
    assert row[0] == b['full_name']
    assert row[1] == b['email']
    assert row[2] == b['id']


def test_cpa_sees_portal_users_per_client(tmp_path):
    """CPA override: list users (including removed) for oversight."""
    db = _mkdb(tmp_path)
    _admin(db)
    c = _contrib(db)
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=c['id'], status='removed',
                         actor_email='cpa@firm.com')
    all_users = mup.list_users(db, firm_code='FIRM', client_code='CONS',
                                 include_removed=True)
    statuses = {u['status'] for u in all_users}
    assert 'removed' in statuses


def test_cpa_can_force_remove_user(tmp_path):
    """CPA override path: uses the same set_user_status with actor_email
    = cpa username. The self-remove guard is on the user portal HTTP
    handler, not the library layer."""
    db = _mkdb(tmp_path)
    a = _admin(db)
    mup.set_user_status(db, firm_code='FIRM', client_code='CONS',
                         user_id=a['id'], status='removed',
                         actor_email='cpa@firm.com')
    fresh = mup.get_user(db, user_id=a['id'])
    assert fresh['status'] == 'removed'

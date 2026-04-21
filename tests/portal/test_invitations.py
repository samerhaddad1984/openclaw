"""Phase 3: invitation flow — create/accept/expire/reuse/scope."""
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
    db = tmp_path / 'inv.db'
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
                suspended_at TEXT, removed_at TEXT,
                version INTEGER DEFAULT 1,
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
        conn.execute("INSERT INTO firms VALUES ('FIRM', 'Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, portal_mode) "
            "VALUES ('CONS','FIRM','Construction Tremblay','multi')",
        )
        conn.commit()
    return db


def _make_admin(db):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='owner@cons.com', full_name='Owner',
        role='admin', invited_by='cpa@firm.com', status='active',
    )


def test_admin_can_invite_colleague(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    inv = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='book@cons.com', full_name='Book Keeper',
        role='contributor', invited_by='owner@cons.com',
    )
    assert inv['token'] and len(inv['token']) > 30
    assert inv['expires_at']


def test_invitation_role_must_be_valid(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    with pytest.raises(ValueError):
        mup.create_invitation(
            db, firm_code='FIRM', client_code='CONS',
            email='book@cons.com', full_name='x',
            role='owner', invited_by='owner@cons.com',
        )


def test_accept_invitation_creates_user(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    inv = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='book@cons.com', full_name='Book Keeper',
        role='contributor', invited_by='owner@cons.com',
    )
    result = mup.accept_invitation(db, token=inv['token'])
    assert result['ok'] is True
    user = result['user']
    assert user['email'] == 'book@cons.com'
    assert user['role'] == 'contributor'
    assert user['status'] == 'active'
    assert user['user_token'] and len(user['user_token']) > 30


def test_accept_creates_unique_token(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    inv1 = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='a@cons.com', full_name='A', role='contributor',
        invited_by='owner@cons.com',
    )
    inv2 = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='b@cons.com', full_name='B', role='contributor',
        invited_by='owner@cons.com',
    )
    u1 = mup.accept_invitation(db, token=inv1['token'])['user']
    u2 = mup.accept_invitation(db, token=inv2['token'])['user']
    assert u1['user_token'] != u2['user_token']


def test_expired_invitation_rejected(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    inv = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='c@cons.com', full_name='C', role='contributor',
        invited_by='owner@cons.com',
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE client_portal_invitations "
            "SET expires_at='2020-01-01T00:00:00+00:00' "
            "WHERE invitation_token=?", (inv['token'],),
        )
        conn.commit()
    r = mup.accept_invitation(db, token=inv['token'])
    assert r['ok'] is False
    assert r['error'] == 'expired'


def test_already_accepted_cannot_reuse(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    inv = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='d@cons.com', full_name='D', role='contributor',
        invited_by='owner@cons.com',
    )
    first = mup.accept_invitation(db, token=inv['token'])
    assert first['ok'] is True
    second = mup.accept_invitation(db, token=inv['token'])
    assert second['ok'] is False
    assert second['error'] == 'already_accepted'


def test_duplicate_invitation_same_email_replaces(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    inv1 = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='e@cons.com', full_name='E', role='contributor',
        invited_by='owner@cons.com',
    )
    inv2 = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='e@cons.com', full_name='E2', role='admin',
        invited_by='owner@cons.com',
    )
    # The first invitation should be cancelled
    older = mup.get_invitation(db, token=inv1['token'])
    assert older['status'] == 'cancelled'
    newer = mup.get_invitation(db, token=inv2['token'])
    assert newer['status'] == 'pending'
    # Accepting the newer one creates an admin user
    r = mup.accept_invitation(db, token=inv2['token'])
    assert r['user']['role'] == 'admin'


def test_cpa_sees_all_portal_users_per_client(tmp_path):
    db = _mkdb(tmp_path)
    _make_admin(db)
    mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='x@cons.com', full_name='X', role='contributor',
        invited_by='cpa@firm.com', status='active',
    )
    users = mup.list_users(db, firm_code='FIRM', client_code='CONS')
    emails = {u['email'] for u in users}
    assert 'owner@cons.com' in emails
    assert 'x@cons.com' in emails


def test_firm_scoping_prevents_cross_firm_invite_accept(tmp_path):
    db = _mkdb(tmp_path)
    # Add a second firm + client
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO firms VALUES ('FIRM2','Bob CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_mode) "
            "VALUES ('OTHER','FIRM2','multi')",
        )
        conn.commit()
    inv = mup.create_invitation(
        db, firm_code='FIRM2', client_code='OTHER',
        email='z@other.com', full_name='Z', role='admin',
        invited_by='bob@firm2.com',
    )
    # Acceptance shouldn't leak to FIRM's records
    r = mup.accept_invitation(db, token=inv['token'])
    assert r['ok'] is True
    users_firm1 = mup.list_users(db, firm_code='FIRM', client_code='CONS')
    assert not any(u['email'] == 'z@other.com' for u in users_firm1)

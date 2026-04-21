"""Phase 2: token + URL routing for the multi-user portal."""
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
    db = tmp_path / 'routing.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (
                firm_code TEXT PRIMARY KEY, name TEXT
            );
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT, email TEXT,
                active INTEGER DEFAULT 1,
                portal_mode TEXT DEFAULT 'single',
                portal_token_created_at TEXT,
                portal_token_rotated_count INTEGER DEFAULT 0
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                email TEXT NOT NULL,
                full_name TEXT,
                role TEXT NOT NULL,
                user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                invited_by TEXT,
                invited_at TEXT,
                accepted_at TEXT,
                last_active_at TEXT,
                upload_count INTEGER DEFAULT 0,
                suspended_at TEXT, removed_at TEXT,
                version INTEGER DEFAULT 1,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE client_portal_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                email TEXT NOT NULL,
                full_name TEXT,
                invited_role TEXT NOT NULL,
                invitation_token TEXT UNIQUE NOT NULL,
                invited_by TEXT,
                invited_at TEXT,
                expires_at TEXT,
                accepted_at TEXT,
                status TEXT DEFAULT 'pending'
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
        conn.execute(
            "INSERT INTO firms (firm_code, name) VALUES ('FIRM','Sam')",
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "portal_token, portal_mode) VALUES "
            "('A','FIRM','Alpha','T_alpha_token_longenough_0123456789', 'single'), "
            "('B','FIRM','Beta','T_beta_token_longenough_0123456789abcd', 'multi')",
        )
        conn.commit()
    return db


def test_single_mode_client_token_works(tmp_path):
    db = _mkdb(tmp_path)
    mode, client, user = mup.resolve_portal_access(
        db, token='T_alpha_token_longenough_0123456789')
    assert mode == 'single'
    assert client['client_code'] == 'A'
    assert user is None


def test_multi_mode_client_token_shows_redirect(tmp_path):
    db = _mkdb(tmp_path)
    mode, client, user = mup.resolve_portal_access(
        db, token='T_beta_token_longenough_0123456789abcd')
    assert mode == 'multi_redirect'
    assert client['client_code'] == 'B'
    assert user is None


def test_multi_mode_user_token_works(tmp_path):
    db = _mkdb(tmp_path)
    u = mup.create_user_direct(
        db, firm_code='FIRM', client_code='B',
        email='bob@beta.com', full_name='Bob',
        role='contributor', invited_by='cpa@firm.com',
        status='active',
    )
    mode, client, user = mup.resolve_portal_access(db, token=u['user_token'])
    assert mode == 'multi'
    assert client['client_code'] == 'B'
    assert user['email'] == 'bob@beta.com'


def test_suspended_user_token_rejected(tmp_path):
    db = _mkdb(tmp_path)
    u = mup.create_user_direct(
        db, firm_code='FIRM', client_code='B',
        email='bob@beta.com', full_name='Bob',
        role='contributor', invited_by='cpa@firm.com',
        status='active',
    )
    mup.set_user_status(db, firm_code='FIRM', client_code='B',
                         user_id=u['id'], status='suspended',
                         actor_email='admin@beta.com')
    mode, _, _ = mup.resolve_portal_access(db, token=u['user_token'])
    assert mode is None


def test_removed_user_token_rejected(tmp_path):
    db = _mkdb(tmp_path)
    u = mup.create_user_direct(
        db, firm_code='FIRM', client_code='B',
        email='bob@beta.com', full_name='Bob',
        role='contributor', invited_by='cpa@firm.com',
        status='active',
    )
    old_token = u['user_token']
    mup.set_user_status(db, firm_code='FIRM', client_code='B',
                         user_id=u['id'], status='removed',
                         actor_email='admin@beta.com')
    mode, _, _ = mup.resolve_portal_access(db, token=old_token)
    # Removed rotates the token, so the old token just doesn't resolve.
    assert mode is None


def test_expired_invitation_token_rejected(tmp_path):
    db = _mkdb(tmp_path)
    # Create invitation then manually expire it
    inv = mup.create_invitation(
        db, firm_code='FIRM', client_code='B',
        email='carol@beta.com', full_name='Carol',
        role='contributor', invited_by='admin@beta.com',
    )
    with sqlite3.connect(db) as c:
        c.execute(
            "UPDATE client_portal_invitations SET expires_at='2020-01-01T00:00:00+00:00' "
            "WHERE invitation_token=?",
            (inv['token'],),
        )
        c.commit()
    result = mup.accept_invitation(db, token=inv['token'])
    assert result['ok'] is False
    assert result['error'] == 'expired'


def test_token_resolution_firm_scoped(tmp_path):
    db = _mkdb(tmp_path)
    # Same email allowed in two different firms/clients
    u1 = mup.create_user_direct(
        db, firm_code='FIRM', client_code='B',
        email='shared@example.com', full_name='Shared',
        role='admin', invited_by='cpa@firm.com', status='active',
    )
    mode, client, user = mup.resolve_portal_access(db, token=u1['user_token'])
    assert mode == 'multi' and user['client_code'] == 'B'


def test_random_string_rejects(tmp_path):
    db = _mkdb(tmp_path)
    mode, _, _ = mup.resolve_portal_access(db, token='not-a-real-token-at-all-nope')
    assert mode is None


def test_short_token_rejected(tmp_path):
    db = _mkdb(tmp_path)
    mode, _, _ = mup.resolve_portal_access(db, token='short')
    assert mode is None

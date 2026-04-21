"""Cleanup Item 5: invitation idempotency + rate-limit."""
from __future__ import annotations

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'invite.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                portal_mode TEXT DEFAULT 'multi'
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
                first_tour_completed_at TEXT,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE client_portal_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                invited_role TEXT NOT NULL,
                invitation_token TEXT UNIQUE NOT NULL,
                invited_by TEXT, invited_at TEXT, expires_at TEXT,
                accepted_at TEXT, status TEXT DEFAULT 'pending',
                invited_language TEXT,
                client_request_id TEXT
            );
            CREATE UNIQUE INDEX idx_cpi_client_request
            ON client_portal_invitations(firm_code, client_code, client_request_id)
            WHERE client_request_id IS NOT NULL;
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER, firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('F','Sam')")
        conn.execute("INSERT INTO clients (client_code, firm_code) VALUES ('C','F')")
        conn.commit()
    return db


def _admin(db):
    return mup.create_user_direct(
        db, firm_code='F', client_code='C',
        email='admin@c.com', full_name='Admin',
        role='admin', invited_by='cpa@firm.com', status='active',
    )


def test_duplicate_request_id_no_duplicate_invitation(tmp_path):
    db = _mkdb(tmp_path)
    _admin(db)
    rid = 'inv_' + 'a' * 32
    inv1 = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='first@c.com', full_name='First', role='contributor',
        invited_by='admin@c.com', client_request_id=rid,
    )
    inv2 = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='first@c.com', full_name='First', role='contributor',
        invited_by='admin@c.com', client_request_id=rid,
    )
    assert inv2.get('idempotent_replay') is True
    assert inv1['token'] == inv2['token']
    with sqlite3.connect(db) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM client_portal_invitations "
            "WHERE firm_code='F' AND client_code='C'"
        ).fetchone()[0]
    assert n == 1


def test_different_request_ids_both_create(tmp_path):
    db = _mkdb(tmp_path)
    _admin(db)
    inv1 = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='a@c.com', full_name='A', role='contributor',
        invited_by='admin@c.com',
        client_request_id='inv_' + 'a' * 32,
    )
    inv2 = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='b@c.com', full_name='B', role='contributor',
        invited_by='admin@c.com',
        client_request_id='inv_' + 'b' * 32,
    )
    assert inv1['token'] != inv2['token']
    assert inv1.get('idempotent_replay') is False
    assert inv2.get('idempotent_replay') is False


def test_missing_request_id_still_works(tmp_path):
    """Legacy callers that don't send client_request_id keep working."""
    db = _mkdb(tmp_path)
    _admin(db)
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='legacy@c.com', full_name='L', role='contributor',
        invited_by='admin@c.com',
    )
    assert inv.get('idempotent_replay') is False
    assert inv['token']


def test_concurrent_double_click_only_one_invitation(tmp_path):
    db = _mkdb(tmp_path)
    _admin(db)
    rid = 'inv_' + 'c' * 32
    results: list[dict] = []
    barrier = threading.Barrier(2)

    def go():
        barrier.wait()
        r = mup.create_invitation(
            db, firm_code='F', client_code='C',
            email='race@c.com', full_name='R', role='contributor',
            invited_by='admin@c.com', client_request_id=rid,
        )
        results.append(r)

    t1 = threading.Thread(target=go)
    t2 = threading.Thread(target=go)
    t1.start(); t2.start(); t1.join(); t2.join()
    # Both calls return the SAME token (one wrote, one replayed).
    tokens = {r['token'] for r in results}
    assert len(tokens) == 1
    with sqlite3.connect(db) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM client_portal_invitations "
            "WHERE client_request_id=?", (rid,),
        ).fetchone()[0]
    assert n == 1


def test_rapid_invitations_rate_limited(tmp_path):
    mup.reset_invite_rate_limits()
    u_id = 999
    # 10 allowed
    for _ in range(10):
        assert mup.invite_rate_allowed(u_id) is True
    # 11th blocked
    assert mup.invite_rate_allowed(u_id) is False
    # Different admin → own window
    assert mup.invite_rate_allowed(1000) is True


def test_invitation_still_works_for_same_email_later(tmp_path):
    """After an initial invite, re-inviting the same email with a
    different client_request_id supersedes the prior pending row
    (existing behaviour) — this is NOT blocked by idempotency."""
    db = _mkdb(tmp_path)
    _admin(db)
    inv1 = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='same@c.com', full_name='Same', role='contributor',
        invited_by='admin@c.com',
        client_request_id='inv_' + 'd' * 32,
    )
    inv2 = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='same@c.com', full_name='Same', role='admin',
        invited_by='admin@c.com',
        client_request_id='inv_' + 'e' * 32,
    )
    # New token; not an idempotent replay.
    assert inv1['token'] != inv2['token']
    assert inv2.get('idempotent_replay') is False
    # Prior pending row cancelled
    prior = mup.get_invitation(db, token=inv1['token'])
    assert prior['status'] == 'cancelled'


def test_render_form_includes_hidden_client_request_id():
    html = mup.render_user_portal_admin(
        client={'client_code': 'C', 'client_name': 'Acme'},
        user_token='tok_x' * 8,
        users=[], invitations=[],
    )
    assert 'name="client_request_id"' in html
    assert 'value="inv_' in html
    assert 'onsubmit="return _inviteSubmit' in html

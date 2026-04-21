"""Phase 7: per-user security + audit trail + token rotation."""
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
    db = tmp_path / 's.db'
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
        """)
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_mode) "
            "VALUES ('CONS','FIRM','multi')",
        )
        conn.commit()
    return db


def _user(db):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='u@cons.com', full_name='U', role='admin',
        invited_by='cpa@firm.com', status='active',
    )


def test_rate_limit_per_user_not_client(tmp_path):
    mup.reset_rate_limits()
    db = _mkdb(tmp_path)
    u1 = _user(db)
    u2 = mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='u2@cons.com', full_name='U2', role='contributor',
        invited_by='cpa@firm.com', status='active',
    )
    for _ in range(30):
        assert mup.upload_rate_allowed(u1['id']) is True
    assert mup.upload_rate_allowed(u1['id']) is False
    # u2 independent
    assert mup.upload_rate_allowed(u2['id']) is True


def test_audit_trail_complete(tmp_path):
    db = _mkdb(tmp_path)
    u = _user(db)
    mup.audit_log(db, firm_code='FIRM', client_code='CONS',
                    actor_email=u['email'], action='upload',
                    portal_user_id=u['id'], detail='count=1',
                    ip='10.0.0.1', user_agent='ua')
    mup.audit_log(db, firm_code='FIRM', client_code='CONS',
                    actor_email=u['email'], action='message_sent',
                    portal_user_id=u['id'], detail='',
                    ip='10.0.0.1', user_agent='ua')
    rows = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    # Creation audit + upload + message_sent → at least 3 rows
    actions = {r['action'] for r in rows}
    assert 'upload' in actions
    assert 'message_sent' in actions
    assert 'user_created' in actions


def test_individual_token_rotation(tmp_path):
    db = _mkdb(tmp_path)
    u1 = _user(db)
    u2 = mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='u2@cons.com', full_name='U2', role='contributor',
        invited_by='cpa@firm.com', status='active',
    )
    old_u1 = u1['user_token']
    new_u1 = mup.rotate_user_token(
        db, firm_code='FIRM', client_code='CONS',
        user_id=u1['id'], actor_email='cpa@firm.com',
    )
    assert new_u1 != old_u1
    # u2 token untouched
    fresh_u2 = mup.get_user(db, user_id=u2['id'])
    assert fresh_u2['user_token'] == u2['user_token']
    # Old u1 token no longer resolves
    mode, _, _ = mup.resolve_portal_access(db, token=old_u1)
    assert mode is None
    mode2, _, _ = mup.resolve_portal_access(db, token=new_u1)
    assert mode2 == 'multi'


def test_suspicious_multi_ip_detected(tmp_path):
    db = _mkdb(tmp_path)
    u = _user(db)
    for ip in ('1.1.1.1', '2.2.2.2', '3.3.3.3'):
        mup.audit_log(db, firm_code='FIRM', client_code='CONS',
                        actor_email=u['email'], action='access',
                        portal_user_id=u['id'], detail='', ip=ip,
                        user_agent='ua')
    alerts = mup.detect_suspicious_activity(db, portal_user_id=u['id'])
    kinds = {a['kind'] for a in alerts}
    assert 'multi_ip' in kinds


def test_suspicious_failed_access_burst(tmp_path):
    db = _mkdb(tmp_path)
    u = _user(db)
    for _ in range(6):
        mup.audit_log(db, firm_code='FIRM', client_code='CONS',
                        actor_email='attacker@x', action='access_rejected',
                        portal_user_id=u['id'], detail='bad token',
                        ip='9.9.9.9', user_agent='ua')
    alerts = mup.detect_suspicious_activity(db, portal_user_id=u['id'])
    kinds = {a['kind'] for a in alerts}
    assert 'failed_access_burst' in kinds


def test_suspicious_summary_across_users(tmp_path):
    db = _mkdb(tmp_path)
    u1 = _user(db)
    u2 = mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='quiet@cons.com', full_name='Q', role='contributor',
        invited_by='cpa@firm.com', status='active',
    )
    for ip in ('1.1.1.1', '2.2.2.2', '3.3.3.3'):
        mup.audit_log(db, firm_code='FIRM', client_code='CONS',
                        actor_email=u1['email'], action='access',
                        portal_user_id=u1['id'], ip=ip)
    summary = mup.suspicious_summary(db, firm_code='FIRM',
                                       client_code='CONS')
    assert any(s['user_id'] == u1['id'] for s in summary)
    assert not any(s['user_id'] == u2['id'] for s in summary)

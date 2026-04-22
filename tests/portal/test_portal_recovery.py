"""Scope 1 Phase 2 — portal token rotation + forgot-link recovery."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402
from src.integrations import portal_recovery as pr  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'recov.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_mode TEXT DEFAULT 'multi',
                portal_token TEXT,
                contact_email TEXT, active INTEGER DEFAULT 1
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                role TEXT NOT NULL, user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                whatsapp_number TEXT, whatsapp_verified INTEGER DEFAULT 0,
                whatsapp_verified_at TEXT,
                invited_by TEXT, invited_at TEXT, accepted_at TEXT,
                last_active_at TEXT, upload_count INTEGER DEFAULT 0,
                suspended_at TEXT, removed_at TEXT, version INTEGER DEFAULT 1,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER,
                firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE client_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, kind TEXT, title TEXT, body TEXT,
                document_id TEXT, status TEXT, channel TEXT,
                recipient_email TEXT, recipient_phone TEXT,
                subject TEXT, priority INTEGER DEFAULT 5,
                send_at TEXT, sent_at TEXT,
                retry_count INTEGER DEFAULT 0, last_error TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """,
        )
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_mode, "
            "contact_email) VALUES ('CONS','FIRM','multi','owner@cons.com')",
        )
        conn.commit()
    return db


def _admin(db):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='owner@cons.com', full_name='Owner', role='admin',
        invited_by='cpa@firm.com', status='active',
    )


def _contrib(db, email='bob@cons.com'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email=email, full_name='Bob', role='contributor',
        invited_by='owner@cons.com', status='active',
    )


# --- rotate_my_token ---


def test_user_can_rotate_own_token(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    old = admin['user_token']
    out = pr.rotate_my_token(db, user_id=admin['id'], notify=False)
    assert out['new_token'] != old
    assert mup.get_user(db, user_id=admin['id'])['user_token'] == out['new_token']


def test_old_token_invalidated_after_rotation(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    old = admin['user_token']
    pr.rotate_my_token(db, user_id=admin['id'], notify=False)
    # Old token must not resolve to the user.
    _, _, pu = mup.resolve_portal_access(db, token=old)
    assert pu is None


def test_rotate_enqueues_notification_when_enabled(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    pr.rotate_my_token(db, user_id=admin['id'],
                         base_url="https://example.com", notify=True)
    with sqlite3.connect(str(db)) as c:
        c.row_factory = sqlite3.Row
        rows = list(c.execute(
            "SELECT * FROM client_notifications WHERE kind='portal_recovery'"))
    assert len(rows) == 1
    assert admin['email'] in (rows[0]['recipient_email'] or '')


# --- request_recovery (forgot-link) ---


def test_forgot_link_flow_resends(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    res = pr.request_recovery(
        db, email=admin['email'], firm_code='FIRM', client_code='CONS',
        base_url='https://example.com',
    )
    assert res['ok'] is True
    with sqlite3.connect(str(db)) as c:
        c.row_factory = sqlite3.Row
        rows = list(c.execute(
            "SELECT * FROM client_notifications "
            "WHERE kind='portal_recovery_link'"))
    assert len(rows) == 1
    assert rows[0]['recipient_email'] == admin['email']
    assert admin['user_token'] in (rows[0]['body'] or '')


def test_forgot_link_rate_limited(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    r1 = pr.request_recovery(
        db, email=admin['email'], firm_code='FIRM', client_code='CONS',
    )
    r2 = pr.request_recovery(
        db, email=admin['email'], firm_code='FIRM', client_code='CONS',
    )
    assert r1['ok'] is True
    assert r2['ok'] is False and r2['reason'] == 'rate_limited'
    # Only one notification enqueued.
    with sqlite3.connect(str(db)) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM client_notifications "
            "WHERE kind='portal_recovery_link'",
        ).fetchone()[0]
    assert n == 1


def test_forgot_link_unknown_email_silent(tmp_path):
    """Doesn't leak existence — same response shape for miss vs hit."""
    db = _mkdb(tmp_path)
    _admin(db)
    res = pr.request_recovery(
        db, email='stranger@nowhere.com',
        firm_code='FIRM', client_code='CONS',
    )
    assert res['ok'] is True  # "processed" — same shape as hit.
    # But no email enqueued.
    with sqlite3.connect(str(db)) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM client_notifications "
            "WHERE kind='portal_recovery_link'",
        ).fetchone()[0]
    assert n == 0


def test_forgot_link_suspended_user_not_resent(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS', user_id=bob['id'],
        status='suspended', actor_email=admin['email'],
    )
    res = pr.request_recovery(
        db, email=bob['email'], firm_code='FIRM', client_code='CONS',
    )
    assert res['ok'] is True
    with sqlite3.connect(str(db)) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM client_notifications "
            "WHERE kind='portal_recovery_link' AND recipient_email=?",
            (bob['email'],),
        ).fetchone()[0]
    assert n == 0


def test_admin_notified_of_recovery_request(tmp_path):
    """Contributor recovery → each active admin gets a heads-up email."""
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    pr.request_recovery(
        db, email=bob['email'], firm_code='FIRM', client_code='CONS',
    )
    with sqlite3.connect(str(db)) as c:
        c.row_factory = sqlite3.Row
        admin_rows = list(c.execute(
            "SELECT * FROM client_notifications "
            "WHERE kind='portal_recovery_admin_notify'"))
    assert len(admin_rows) == 1
    assert admin_rows[0]['recipient_email'] == admin['email']
    assert bob['email'] in (admin_rows[0]['title'] or '')


def test_admin_recovery_does_not_notify_other_admins(tmp_path):
    """An admin's own recovery doesn't create a heads-up to admins."""
    db = _mkdb(tmp_path)
    admin = _admin(db)
    pr.request_recovery(
        db, email=admin['email'], firm_code='FIRM', client_code='CONS',
    )
    with sqlite3.connect(str(db)) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM client_notifications "
            "WHERE kind='portal_recovery_admin_notify'",
        ).fetchone()[0]
    assert n == 0


def test_forgot_link_invalid_email_rejected(tmp_path):
    db = _mkdb(tmp_path)
    _admin(db)
    res = pr.request_recovery(
        db, email='not-an-email', firm_code='FIRM', client_code='CONS',
    )
    assert res['ok'] is False and res['reason'] == 'invalid_email'


def test_recovery_audit_row_written(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    pr.request_recovery(
        db, email=admin['email'], firm_code='FIRM', client_code='CONS',
        ip='203.0.113.4',
    )
    audit = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    recovery = [a for a in audit if a['action'] == 'recovery_requested']
    assert len(recovery) == 1
    assert '203.0.113.4' in (recovery[0]['detail'] or '')

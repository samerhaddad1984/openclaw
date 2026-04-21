"""Item 8: portal-admin invitation emails go through the notification
queue rather than a direct SMTP send, so transient failures retry.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402
from src.integrations import notification_sender as ns  # noqa: E402


def test_render_invitation_email_en():
    subj, body = mup.render_invitation_email(
        recipient_name='Alice', inviter_name='Bob',
        client_display='Acme Corp',
        accept_url='https://example/invite/abc', lang='en',
    )
    assert 'Bob' in subj
    assert 'receipts' in subj
    assert 'Hi Alice' in body
    assert 'Acme Corp' in body
    assert 'https://example/invite/abc' in body


def test_render_invitation_email_fr():
    subj, body = mup.render_invitation_email(
        recipient_name='Alice', inviter_name='Bob',
        client_display='Construction Tremblay',
        accept_url='https://example/invite/abc', lang='fr',
    )
    assert 'vous invite' in subj or 'reçus' in subj
    assert 'Bonjour Alice' in body
    assert 'Construction Tremblay' in body
    assert '14 jours' in body


def test_invite_notification_has_proper_metadata(tmp_path):
    """Enqueue path exercised end-to-end: invitation + enqueue result
    in one notification row with the correct metadata fields."""
    db = tmp_path / 'inv.db'
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
                portal_user_id INTEGER, firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('F','Sam')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_mode) "
            "VALUES ('C','F','multi')",
        )
        conn.commit()
    ns.ensure_sender_schema(db)
    admin = mup.create_user_direct(
        db, firm_code='F', client_code='C',
        email='admin@c.com', full_name='Admin',
        role='admin', invited_by='cpa@firm.com', status='active',
    )
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='new@c.com', full_name='New',
        role='contributor', invited_by=admin['email'],
    )
    subj, body = mup.render_invitation_email(
        recipient_name='New', inviter_name='Admin',
        client_display='Acme',
        accept_url=f'https://host/invite/{inv["token"]}',
        lang='en',
    )
    ns.enqueue_single_notification(
        db, firm_code='F', client_code='C',
        recipient_email='new@c.com', recipient_name='New',
        subject=subj, body=body,
        kind='portal_invitation', priority=6,
        metadata={'invitation_id': inv['id'],
                    'invited_by': admin['email'],
                    'invited_role': 'contributor'},
    )
    with sqlite3.connect(db) as c:
        row = c.execute(
            "SELECT kind, recipient_email, subject, body, title, "
            "       status, priority FROM client_notifications"
        ).fetchone()
    assert row is not None
    assert row[0] == 'portal_invitation'
    assert row[1] == 'new@c.com'
    assert 'receipts' in row[2]
    assert 'host/invite' in row[3]
    # Metadata folded into title
    assert 'invitation_id' in row[4]
    assert 'contributor' in row[4]
    assert row[5] == 'pending'
    assert row[6] == 6


def test_invite_retried_by_sender_on_failure(tmp_path):
    """Prove the retry path kicks in when the email_fn returns False."""
    db = tmp_path / 'retry.db'
    ns.ensure_sender_schema(db)
    ns.enqueue_single_notification(
        db, firm_code='F', client_code='C',
        recipient_email='will-fail@example.com',
        subject='s', body='b', kind='portal_invitation',
        metadata={'invitation_id': 1},
    )

    def always_fail(to, subject, body):
        return False

    r1 = ns.send_pending_notifications(db, email_fn=always_fail)
    assert r1['sent'] == 0
    assert r1['requeued'] == 1
    with sqlite3.connect(db) as c:
        status, retry = c.execute(
            "SELECT status, retry_count FROM client_notifications LIMIT 1"
        ).fetchone()
    assert status == 'pending'
    assert retry == 1

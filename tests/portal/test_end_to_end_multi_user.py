"""Phase 9: 15-step end-to-end multi-user portal journey.

Walks the Construction Tremblay scenario from the sprint spec through
the helper API surface. If any step's contract drifts (invitation
semantics, token rotation, preserved attribution), this goes red.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'e2e.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT,
                portal_mode TEXT DEFAULT 'single',
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
                vendor TEXT, amount REAL,
                uploaded_by_portal_user_id INTEGER,
                uploader_name TEXT, uploader_email TEXT,
                review_status TEXT, uploaded_at TEXT
            );
            CREATE TABLE client_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT NOT NULL, firm_code TEXT NOT NULL,
                direction TEXT NOT NULL,
                sender_name TEXT, sender_type TEXT NOT NULL,
                body TEXT NOT NULL,
                sender_portal_user_id INTEGER,
                target_portal_user_id INTEGER,
                related_document_id TEXT, read_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "portal_mode, portal_token) VALUES "
            "('CONS','FIRM','Construction Tremblay','single',"
            "'tok_cons_legacy_longenoughfortokenresolver123456789')",
        )
        conn.commit()
    return db


def _upload(db, user_id, uploader_name, uploader_email, doc_ids):
    with sqlite3.connect(db) as conn:
        for did in doc_ids:
            conn.execute(
                "INSERT INTO documents (document_id, firm_code, client_code, "
                "vendor, amount, review_status, uploaded_at, "
                "uploaded_by_portal_user_id, uploader_name, uploader_email) "
                "VALUES (?, 'FIRM', 'CONS', 'v', 10.0, 'New', datetime('now'), ?, ?, ?)",
                (did, user_id, uploader_name, uploader_email),
            )
        conn.commit()
    mup.increment_upload_count(db, user_id=user_id, n=len(doc_ids))


def test_fifteen_step_multi_user_journey(tmp_path):
    db = _mkdb(tmp_path)

    # 1. CPA enables multi-user mode for Construction Tremblay
    mup.set_portal_mode(
        db, firm_code='FIRM', client_code='CONS',
        mode='multi', actor_email='sam@firm.com',
    )
    client = mup.get_client(db, client_code='CONS')
    assert client['portal_mode'] == 'multi'

    # 2. CPA seeds the owner as first admin (would happen automatically
    #    when contact_email is set, or the CPA promotes manually).
    owner = mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='owner@cons.com', full_name='Owner Tremblay',
        role='admin', invited_by='sam@firm.com', status='active',
    )
    # 3. Owner resolves via personal token
    mode, _, resolved = mup.resolve_portal_access(db, token=owner['user_token'])
    assert mode == 'multi' and resolved['email'] == 'owner@cons.com'

    # 4. Owner invites bookkeeper + office manager
    inv_book = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='book@cons.com', full_name='Book Keeper',
        role='contributor', invited_by='owner@cons.com',
    )
    inv_om = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='om@cons.com', full_name='Office Manager',
        role='contributor', invited_by='owner@cons.com',
    )

    # 5. Both accept
    book = mup.accept_invitation(db, token=inv_book['token'])['user']
    om = mup.accept_invitation(db, token=inv_om['token'])['user']
    assert book['status'] == 'active' and om['status'] == 'active'

    # 6. Owner uploads 3 receipts
    _upload(db, owner['id'], 'Owner Tremblay', 'owner@cons.com',
             ['D1', 'D2', 'D3'])
    # 7. Bookkeeper uploads 5 receipts
    _upload(db, book['id'], 'Book Keeper', 'book@cons.com',
             ['D4', 'D5', 'D6', 'D7', 'D8'])
    # 8. Office manager uploads 2 invoices
    _upload(db, om['id'], 'Office Manager', 'om@cons.com', ['D9', 'D10'])

    # 9. CPA sees 10 documents + filter by uploader works
    with sqlite3.connect(db) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE client_code='CONS'"
        ).fetchone()[0]
        owner_n = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE uploaded_by_portal_user_id=?",
            (owner['id'],),
        ).fetchone()[0]
        book_n = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE uploaded_by_portal_user_id=?",
            (book['id'],),
        ).fetchone()[0]
    assert total == 10
    assert owner_n == 3 and book_n == 5

    # 10. CPA sends message to bookkeeper specifically
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO client_messages "
            "(client_code, firm_code, direction, sender_name, sender_type, "
            " body, target_portal_user_id) "
            "VALUES ('CONS','FIRM','outbound','Sam','cpa',?, ?)",
            ('Question about invoice XYZ', book['id']),
        )
        conn.commit()

    # 11. Bookkeeper sees it, replies
    with sqlite3.connect(db) as conn:
        her_msg = conn.execute(
            "SELECT COUNT(*) FROM client_messages "
            "WHERE client_code='CONS' AND target_portal_user_id=?",
            (book['id'],),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO client_messages "
            "(client_code, firm_code, direction, sender_name, sender_type, "
            " body, sender_portal_user_id) "
            "VALUES ('CONS','FIRM','inbound',?,?, ?, ?)",
            ('Book Keeper', 'client', 'Checking on XYZ now', book['id']),
        )
        conn.commit()
    assert her_msg == 1

    # 12. Owner suspends Office Manager (left the company)
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS', user_id=om['id'],
        status='suspended', actor_email='owner@cons.com',
    )
    # 13. Office Manager's token no longer resolves
    mode, _, _ = mup.resolve_portal_access(db, token=om['user_token'])
    assert mode is None

    # 14. Owner removes Bookkeeper entirely
    old_book_token = book['user_token']
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS', user_id=book['id'],
        status='removed', actor_email='owner@cons.com',
    )
    mode, _, _ = mup.resolve_portal_access(db, token=old_book_token)
    assert mode is None

    # 15. All historical uploads preserved with names
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT document_id, uploader_name, uploader_email "
            "FROM documents WHERE client_code='CONS' ORDER BY document_id"
        ).fetchall()
    names = {r[1] for r in rows}
    assert {'Owner Tremblay', 'Book Keeper', 'Office Manager'} <= names
    emails = {r[2] for r in rows}
    assert {'owner@cons.com', 'book@cons.com', 'om@cons.com'} <= emails

    # Bonus: audit trail captured the lifecycle
    audit = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    actions = {r['action'] for r in audit}
    assert {'portal_mode_changed', 'user_created',
             'invitation_created', 'invitation_accepted',
             'user_status_suspended', 'user_status_removed'} <= actions

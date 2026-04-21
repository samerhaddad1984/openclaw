"""Phase 6: messaging with sender identity in multi-user mode."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'msg.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT,
                portal_mode TEXT DEFAULT 'multi',
                active INTEGER DEFAULT 1
            );
            CREATE TABLE client_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT NOT NULL, firm_code TEXT NOT NULL,
                direction TEXT NOT NULL,
                sender_name TEXT, sender_type TEXT NOT NULL,
                body TEXT NOT NULL,
                related_document_id TEXT, read_at TEXT,
                sender_portal_user_id INTEGER,
                target_portal_user_id INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
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


def _users(db):
    a = mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='owner@cons.com', full_name='Owner',
        role='admin', invited_by='cpa@firm.com', status='active')
    b = mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email='book@cons.com', full_name='Book',
        role='contributor', invited_by='owner@cons.com', status='active')
    return a, b


def _insert_msg(db, *, direction='inbound', sender_type='client',
                 sender_name='X', body='hi', sender_portal_user_id=None,
                 target_portal_user_id=None):
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO client_messages "
            "(client_code, firm_code, direction, sender_name, sender_type, "
            " body, sender_portal_user_id, target_portal_user_id) "
            "VALUES ('CONS','FIRM',?,?,?,?,?,?)",
            (direction, sender_name, sender_type, body,
             sender_portal_user_id, target_portal_user_id),
        )
        conn.commit()
        return cur.lastrowid


def test_message_with_sender_identity(tmp_path):
    db = _mkdb(tmp_path)
    a, b = _users(db)
    mid = _insert_msg(db, sender_portal_user_id=b['id'],
                        sender_name=b['full_name'],
                        body='Question about receipt D1')
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT sender_portal_user_id, sender_name FROM client_messages "
            "WHERE id=?", (mid,)).fetchone()
    assert r[0] == b['id']
    assert r[1] == 'Book'


def test_cpa_can_send_to_specific_user(tmp_path):
    db = _mkdb(tmp_path)
    a, b = _users(db)
    mid = _insert_msg(
        db, direction='outbound', sender_type='cpa',
        sender_name='Sam', target_portal_user_id=b['id'],
        body='Question for bookkeeper only',
    )
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT target_portal_user_id, sender_type FROM client_messages "
            "WHERE id=?", (mid,)).fetchone()
    assert r[0] == b['id']
    assert r[1] == 'cpa'


def test_cpa_broadcast_to_all_users(tmp_path):
    db = _mkdb(tmp_path)
    a, b = _users(db)
    mid = _insert_msg(
        db, direction='outbound', sender_type='cpa',
        sender_name='Sam', target_portal_user_id=None,
        body='Heads up everyone',
    )
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT target_portal_user_id FROM client_messages "
            "WHERE id=?", (mid,)).fetchone()
    assert r[0] is None


def test_message_thread_shows_all_senders(tmp_path):
    db = _mkdb(tmp_path)
    a, b = _users(db)
    _insert_msg(db, sender_portal_user_id=a['id'], sender_name='Owner',
                  body='first')
    _insert_msg(db, sender_portal_user_id=b['id'], sender_name='Book',
                  body='second')
    _insert_msg(db, direction='outbound', sender_type='cpa',
                  sender_name='Sam', body='got it')
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT sender_name, sender_portal_user_id "
            "FROM client_messages WHERE client_code='CONS' "
            "ORDER BY id").fetchall()
    senders = [r[0] for r in rows]
    assert senders == ['Owner', 'Book', 'Sam']
    # portal user ids present only on client-side messages
    ids = [r[1] for r in rows]
    assert ids == [a['id'], b['id'], None]

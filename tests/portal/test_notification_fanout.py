"""Item 4: notification fanout to portal-user groups."""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402
from src.integrations import notification_sender as ns  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'fanout.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT,
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
            "VALUES ('C','FIRM','multi')",
        )
        conn.commit()
    ns.ensure_sender_schema(db)
    return db


def _make(db, email, name, role='contributor', status='active'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='C',
        email=email, full_name=name, role=role,
        invited_by='cpa@firm.com', status=status,
    )


def _queue_rows(db):
    with sqlite3.connect(db) as c:
        rows = c.execute(
            "SELECT id, recipient_email, title, body, subject FROM client_notifications"
        ).fetchall()
    return rows


def test_fanout_all_admins_creates_one_per_admin(tmp_path):
    db = _mkdb(tmp_path)
    _make(db, 'a1@c.com', 'A1', role='admin')
    _make(db, 'a2@c.com', 'A2', role='admin')
    _make(db, 'c1@c.com', 'C1', role='contributor')
    out = ns.enqueue_notification_to_group(
        db, firm_code='FIRM', client_code='C',
        group_type='all_admins',
        subject='New receipt batch', body='please review {name}',
    )
    assert out['fanout_count'] == 2
    assert set(out['recipients']) == {'a1@c.com', 'a2@c.com'}
    assert out['batch_id'].startswith('b_')
    rows = _queue_rows(db)
    assert len(rows) == 2
    # Personalisation {name} replaced
    bodies = {r[1]: r[3] for r in rows}
    assert bodies['a1@c.com'] == 'please review A1'
    assert bodies['a2@c.com'] == 'please review A2'


def test_fanout_all_users_includes_contributors(tmp_path):
    db = _mkdb(tmp_path)
    _make(db, 'a@c.com', 'A', role='admin')
    _make(db, 'c@c.com', 'C', role='contributor')
    out = ns.enqueue_notification_to_group(
        db, firm_code='FIRM', client_code='C',
        group_type='all_portal_users', subject='hi', body='msg',
    )
    assert out['fanout_count'] == 2
    assert {'a@c.com', 'c@c.com'} == set(out['recipients'])


def test_fanout_specific_user_single_notification(tmp_path):
    db = _mkdb(tmp_path)
    u = _make(db, 'x@c.com', 'X')
    out = ns.enqueue_notification_to_group(
        db, firm_code='FIRM', client_code='C',
        group_type='specific_user', target_user_id=u['id'],
        subject='just you', body='body',
    )
    assert out['fanout_count'] == 1
    assert out['recipients'] == ['x@c.com']
    assert len(_queue_rows(db)) == 1


def test_fanout_skips_suspended_users(tmp_path):
    db = _mkdb(tmp_path)
    _make(db, 'active@c.com', 'Active', role='admin')
    suspended = _make(db, 'sus@c.com', 'Sus', role='admin')
    mup.set_user_status(
        db, firm_code='FIRM', client_code='C',
        user_id=suspended['id'], status='suspended',
        actor_email='cpa@firm.com',
    )
    out = ns.enqueue_notification_to_group(
        db, firm_code='FIRM', client_code='C',
        group_type='all_admins', subject='s', body='b',
    )
    assert out['fanout_count'] == 1
    assert out['recipients'] == ['active@c.com']


def test_batch_id_links_related_notifications(tmp_path):
    db = _mkdb(tmp_path)
    _make(db, 'a1@c.com', 'A1', role='admin')
    _make(db, 'a2@c.com', 'A2', role='admin')
    out = ns.enqueue_notification_to_group(
        db, firm_code='FIRM', client_code='C',
        group_type='all_admins', subject='s', body='b',
    )
    rows = _queue_rows(db)
    # The metadata is folded into the title suffix as [meta=...]
    titles = [r[2] for r in rows]
    assert all(out['batch_id'] in t for t in titles)
    # Parse the metadata back out
    metas = []
    for t in titles:
        idx = t.rfind('[meta=')
        payload = t[idx + len('[meta='):-1]
        metas.append(json.loads(payload))
    assert all(m['batch_id'] == out['batch_id'] for m in metas)
    assert all(m['group_type'] == 'all_admins' for m in metas)


def test_fanout_empty_group_logs_warning(tmp_path, caplog):
    db = _mkdb(tmp_path)
    # No users created → empty group
    with caplog.at_level(logging.WARNING):
        out = ns.enqueue_notification_to_group(
            db, firm_code='FIRM', client_code='C',
            group_type='all_admins', subject='s', body='b',
        )
    assert out['fanout_count'] == 0
    assert out['recipients'] == []
    assert any('zero recipients' in rec.message.lower()
               for rec in caplog.records)


def test_fanout_unknown_group_type_raises(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(ValueError):
        ns.enqueue_notification_to_group(
            db, firm_code='FIRM', client_code='C',
            group_type='all_aliens', subject='s', body='b',
        )


def test_specific_user_requires_target_id(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(ValueError):
        ns.enqueue_notification_to_group(
            db, firm_code='FIRM', client_code='C',
            group_type='specific_user', subject='s', body='b',
        )

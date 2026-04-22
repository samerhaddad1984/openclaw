"""Phase 3: WhatsApp admin UI (per-user portal + CPA-side override).

Covers the HTML surface + the set/invite/clear pathways that back it.
The live validation XHR is covered by ``test_whatsapp_validation``;
here we focus on:

- invite form accepting the WhatsApp number and copying it onto the
  user row on acceptance,
- user-list rendering the number with a per-row edit form,
- admin + CPA clear paths,
- bilingual labels.
"""
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
    db = tmp_path / 'wa_ui.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT,
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
                whatsapp_number TEXT,
                whatsapp_verified INTEGER DEFAULT 0,
                whatsapp_verified_at TEXT,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE UNIQUE INDEX idx_cpu_whatsapp_firm
                ON client_portal_users(firm_code, whatsapp_number)
                WHERE whatsapp_number IS NOT NULL;
            CREATE TABLE client_portal_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                invited_role TEXT NOT NULL,
                invitation_token TEXT UNIQUE NOT NULL,
                invited_by TEXT, invited_at TEXT, expires_at TEXT,
                accepted_at TEXT, status TEXT DEFAULT 'pending',
                invited_language TEXT,
                client_request_id TEXT,
                whatsapp_number TEXT
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
        conn.execute("INSERT INTO firms VALUES ('F','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "portal_mode) VALUES ('C1','F','Widget Co','multi')",
        )
        conn.commit()
    return db


def _mkuser(db, email='alice@c1', name='Alice', role='admin',
             wa=None, status='active'):
    u = mup.create_user_direct(
        db, firm_code='F', client_code='C1',
        email=email, full_name=name, role=role,
        invited_by='cpa@f', status=status,
    )
    if wa:
        mup.set_user_whatsapp_number(
            db, firm_code='F', client_code='C1',
            user_id=u['id'], raw_number=wa, actor_email='cpa@f',
        )
        u = mup.get_user(db, user_id=u['id'])
    return u


# ---------------------------------------------------------------------------
# Invite form
# ---------------------------------------------------------------------------

def test_invite_form_accepts_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C1',
        email='bob@c1', full_name='Bob', role='contributor',
        invited_by='alice@c1',
        whatsapp_number='514-555-0201',
    )
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT whatsapp_number FROM client_portal_invitations "
            "WHERE id=?", (inv['id'],),
        ).fetchone()
    assert row[0] == '+15145550201'


def test_invite_whatsapp_normalized_before_save(tmp_path):
    db = _mkdb(tmp_path)
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C1',
        email='carol@c1', full_name='Carol', role='contributor',
        invited_by='alice@c1',
        whatsapp_number='+1 (514) 555-0202',
    )
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT whatsapp_number FROM client_portal_invitations "
            "WHERE id=?", (inv['id'],),
        ).fetchone()
    assert row[0] == '+15145550202'


def test_invite_rejects_invalid_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(ValueError):
        mup.create_invitation(
            db, firm_code='F', client_code='C1',
            email='dave@c1', full_name='Dave', role='contributor',
            invited_by='alice@c1',
            whatsapp_number='not a phone',
        )


def test_invite_rejects_duplicate_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    _mkuser(db, email='alice@c1', wa='+15145550203')
    with pytest.raises(ValueError):
        mup.create_invitation(
            db, firm_code='F', client_code='C1',
            email='eve@c1', full_name='Eve', role='contributor',
            invited_by='alice@c1',
            whatsapp_number='+15145550203',
        )


def test_accept_invitation_promotes_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C1',
        email='frank@c1', full_name='Frank', role='contributor',
        invited_by='alice@c1',
        whatsapp_number='514-555-0204',
    )
    result = mup.accept_invitation(db, token=inv['token'])
    assert result['ok'] is True
    user = mup.get_user(db, user_id=result['user']['id'])
    assert user['whatsapp_number'] == '+15145550204'


# ---------------------------------------------------------------------------
# User-list rendering
# ---------------------------------------------------------------------------

def test_user_list_shows_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    _mkuser(db, email='alice@c1', name='Alice', role='admin',
              wa='+15145550205')
    admin = mup.list_users(db, firm_code='F', client_code='C1')[0]
    html = mup.render_user_portal_admin(
        client={'client_code': 'C1', 'client_name': 'Widget Co'},
        user_token=admin['user_token'],
        users=mup.list_users(db, firm_code='F', client_code='C1'),
    )
    assert 'WhatsApp' in html
    # E.164 or the display format must be visible.
    assert '+15145550205' in html or '+1 (514) 555-0205' in html


def test_user_list_shows_not_registered_for_blank(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, email='bob@c1', name='Bob', role='contributor')
    html = mup.render_user_portal_admin(
        client={'client_code': 'C1', 'client_name': 'Widget Co'},
        user_token=u['user_token'],
        users=mup.list_users(db, firm_code='F', client_code='C1'),
    )
    assert 'Non enregistré' in html
    assert 'Not registered' in html


# ---------------------------------------------------------------------------
# Edit / clear paths
# ---------------------------------------------------------------------------

def test_admin_can_edit_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, email='alice@c1', wa='+15145550206')
    mup.set_user_whatsapp_number(
        db, firm_code='F', client_code='C1',
        user_id=u['id'], raw_number='514-555-0207',
        actor_email='alice@c1',
    )
    refreshed = mup.get_user(db, user_id=u['id'])
    assert refreshed['whatsapp_number'] == '+15145550207'


def test_admin_can_clear_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, email='alice@c1', wa='+15145550208')
    mup.set_user_whatsapp_number(
        db, firm_code='F', client_code='C1',
        user_id=u['id'], raw_number=None,
        actor_email='alice@c1',
    )
    assert mup.get_user(db, user_id=u['id'])['whatsapp_number'] is None


def test_cpa_override_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, email='bob@c1', role='contributor')
    # CPA (acting as firm admin) sets the number for an unresponsive admin.
    mup.set_user_whatsapp_number(
        db, firm_code='F', client_code='C1',
        user_id=u['id'], raw_number='+15145550209',
        actor_email='cpa@f (cpa)',
    )
    refreshed = mup.get_user(db, user_id=u['id'])
    assert refreshed['whatsapp_number'] == '+15145550209'
    with sqlite3.connect(db) as conn:
        actor = conn.execute(
            "SELECT actor_email FROM client_portal_user_audit "
            "WHERE portal_user_id=? AND action='whatsapp_number_set' "
            "ORDER BY id DESC LIMIT 1",
            (u['id'],),
        ).fetchone()[0]
    assert '(cpa)' in actor


# ---------------------------------------------------------------------------
# Bilingual labels
# ---------------------------------------------------------------------------

def test_bilingual_labels(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, email='alice@c1', wa='+15145550210')
    html = mup.render_user_portal_admin(
        client={'client_code': 'C1', 'client_name': 'Widget Co'},
        user_token=u['user_token'],
        users=mup.list_users(db, firm_code='F', client_code='C1'),
    )
    # Helper copy (bilingual in the same sentence).
    assert "Registered users can send receipts via WhatsApp" in html
    assert "Les utilisateurs enregistrés peuvent envoyer des reçus" in html


def test_cpa_side_view_renders_whatsapp(tmp_path):
    db = _mkdb(tmp_path)
    _mkuser(db, email='alice@c1', wa='+15145550211')
    users = mup.list_users(db, firm_code='F', client_code='C1')
    html = mup.render_cpa_portal_users(
        client={'client_code': 'C1', 'client_name': 'Widget Co',
                'portal_mode': 'multi'},
        users=users,
    )
    assert 'WhatsApp' in html
    assert '+15145550211' in html or '+1 (514) 555-0211' in html
    # CPA override form posts to the dedicated endpoint.
    assert '/clients/portal_users/whatsapp' in html

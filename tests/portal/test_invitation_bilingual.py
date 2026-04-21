"""Item 6: invitation acceptance page + email bilingual FR/EN."""
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
    db = tmp_path / 'lang.db'
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
                accepted_at TEXT, status TEXT DEFAULT 'pending',
                invited_language TEXT
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
        conn.execute("INSERT INTO firms VALUES ('F','Sam')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code) VALUES ('C','F')")
        conn.commit()
    return db


def test_accept_page_renders_in_fr():
    inv = {'full_name': 'Alice', 'email': 'a@x', 'invited_role': 'admin',
           'client_code': 'C', 'firm_code': 'F',
           'invitation_token': 't' * 40, 'expires_at': '2026-05-10'}
    html = mup.render_accept_invitation_page(
        inv, client_name='Acme', firm_name='Sam CPA', lang='fr',
    )
    assert 'Acme' in html
    assert 'Accepter' in html or 'accepter' in html.lower()
    assert 'invité' in html.lower()
    assert '2026-05-10' in html


def test_accept_page_renders_in_en():
    inv = {'full_name': 'Alice', 'email': 'a@x', 'invited_role': 'admin',
           'client_code': 'C', 'firm_code': 'F',
           'invitation_token': 't' * 40, 'expires_at': '2026-05-10'}
    html = mup.render_accept_invitation_page(
        inv, client_name='Acme', firm_name='Sam CPA', lang='en',
    )
    assert 'Accept invitation' in html
    assert 'invited' in html.lower()


def test_lang_param_overrides():
    # Query-string lang beats stored invitation_lang + Accept-Language.
    assert mup.resolve_invite_lang(
        qs_lang='fr',
        invitation_lang='en',
        accept_language_header='en-US,en;q=0.9',
    ) == 'fr'


def test_browser_header_detected():
    assert mup.resolve_invite_lang(
        qs_lang=None, invitation_lang=None,
        accept_language_header='fr-CA,fr;q=0.9,en;q=0.7',
    ) == 'fr'
    assert mup.resolve_invite_lang(
        qs_lang=None, invitation_lang=None,
        accept_language_header='en-US,en;q=0.9',
    ) == 'en'


def test_invitation_lang_falls_back_to_stored():
    assert mup.resolve_invite_lang(
        qs_lang=None, invitation_lang='fr',
        accept_language_header='en-US',
    ) == 'fr'


def test_unknown_lang_defaults_english():
    assert mup.resolve_invite_lang(
        qs_lang='de', invitation_lang=None,
        accept_language_header='de-DE',
    ) == 'en'


def test_invitation_email_fr_version():
    subj, body = mup.render_invitation_email(
        recipient_name='Alice', inviter_name='Bob',
        client_display='Construction',
        accept_url='https://host/invite/x', lang='fr',
    )
    assert 'reçus' in subj or 'invite' in subj
    assert 'Bonjour' in body
    assert '14 jours' in body


def test_invitation_email_en_version():
    subj, body = mup.render_invitation_email(
        recipient_name='Alice', inviter_name='Bob',
        client_display='Construction',
        accept_url='https://host/invite/x', lang='en',
    )
    assert 'receipts' in subj
    assert 'Hi Alice' in body
    assert '14 days' in body


def test_language_toggle_link_present():
    inv = {'full_name': 'Alice', 'email': 'a@x', 'invited_role': 'admin',
           'client_code': 'C', 'firm_code': 'F',
           'invitation_token': 'tok-ABCD',
           'expires_at': '2026-05-10'}
    # EN page must link to ?lang=fr
    en = mup.render_accept_invitation_page(
        inv, client_name='Acme', firm_name='Sam', lang='en',
    )
    assert '/invite/tok-ABCD?lang=fr' in en
    # FR page must link to ?lang=en
    fr = mup.render_accept_invitation_page(
        inv, client_name='Acme', firm_name='Sam', lang='fr',
    )
    assert '/invite/tok-ABCD?lang=en' in fr


def test_create_invitation_stores_lang(tmp_path):
    db = _mkdb(tmp_path)
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='e@x.com', full_name='E', role='admin',
        invited_by='cpa@firm.com', lang='fr',
    )
    row = mup.get_invitation(db, token=inv['token'])
    assert row['invited_language'] == 'fr'


def test_create_invitation_rejects_unknown_lang(tmp_path):
    db = _mkdb(tmp_path)
    inv = mup.create_invitation(
        db, firm_code='F', client_code='C',
        email='e@x.com', full_name='E', role='admin',
        invited_by='cpa@firm.com', lang='de',
    )
    row = mup.get_invitation(db, token=inv['token'])
    assert row['invited_language'] is None

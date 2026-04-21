"""Item 3: first-time portal-user tour."""
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
    db = tmp_path / 'tour.db'
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
        conn.execute("INSERT INTO firms VALUES ('F','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code) VALUES ('C','F')")
        conn.commit()
    return db


def _user(db, email='u@c.com', name='Alice', role='admin'):
    return mup.create_user_direct(
        db, firm_code='F', client_code='C',
        email=email, full_name=name, role=role,
        invited_by='cpa@firm.com', status='active',
    )


def test_tour_bilingual_en():
    html = mup.render_portal_user_tour(
        1, user_name='Alice', firm_name='Sam CPA',
        user_token='tok_longenoughtokenvaluetokentoken999',
        lang='en',
    )
    assert 'Welcome, Alice' in html
    assert 'Sam CPA' in html
    assert 'Step 1 of 3' in html
    # FR strings must not leak
    assert 'Bienvenue' not in html


def test_tour_bilingual_fr():
    html = mup.render_portal_user_tour(
        1, user_name='Alice', firm_name='Sam CPA',
        user_token='tok_longenoughtokenvaluetokentoken999',
        lang='fr',
    )
    assert 'Bienvenue, Alice' in html
    assert 'Étape 1 sur 3' in html
    assert 'Welcome, Alice' not in html


def test_tour_personalized_with_names():
    html = mup.render_portal_user_tour(
        1, user_name='Carol', firm_name='Acme Accounting',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='en',
    )
    assert 'Carol' in html
    assert 'Acme Accounting' in html


def test_all_3_screens_have_both_langs():
    # Structural check: each screen has both lang keys with required
    # fields populated.
    for i, screen in enumerate(mup._PORTAL_USER_TOUR_CONTENT, 1):
        for lang in ('en', 'fr'):
            assert lang in screen, f'step {i} missing {lang}'
            for field in ('title', 'subtitle', 'body'):
                assert screen[lang].get(field), f'step {i}/{lang} missing {field}'


def test_step_3_has_finish_button_not_next():
    html = mup.render_portal_user_tour(
        3, user_name='Alice', firm_name='Sam CPA',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='en',
    )
    assert 'Get started' in html
    assert 'tour/complete' in html
    assert 'Next &rarr;' not in html


def test_step_1_has_no_back_button():
    html = mup.render_portal_user_tour(
        1, user_name='Alice', firm_name='Sam CPA',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='en',
    )
    assert '&larr; Back' not in html


def test_step_clamping():
    low = mup.render_portal_user_tour(
        0, user_name='A', firm_name='F',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='en',
    )
    assert 'Step 1 of 3' in low
    high = mup.render_portal_user_tour(
        99, user_name='A', firm_name='F',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='en',
    )
    assert 'Step 3 of 3' in high


def test_tour_completion_tracking(tmp_path):
    db = _mkdb(tmp_path)
    u = _user(db)
    assert mup.portal_user_tour_completed(db, user_id=u['id']) is False
    mup.mark_portal_user_tour_completed(db, user_id=u['id'])
    assert mup.portal_user_tour_completed(db, user_id=u['id']) is True


def test_mark_completed_is_idempotent(tmp_path):
    db = _mkdb(tmp_path)
    u = _user(db)
    mup.mark_portal_user_tour_completed(db, user_id=u['id'])
    with sqlite3.connect(db) as c:
        first = c.execute(
            "SELECT first_tour_completed_at FROM client_portal_users WHERE id=?",
            (u['id'],),
        ).fetchone()[0]
    mup.mark_portal_user_tour_completed(db, user_id=u['id'])
    with sqlite3.connect(db) as c:
        second = c.execute(
            "SELECT first_tour_completed_at FROM client_portal_users WHERE id=?",
            (u['id'],),
        ).fetchone()[0]
    # COALESCE keeps the first timestamp
    assert first == second


def test_language_switcher_link_present():
    en = mup.render_portal_user_tour(
        2, user_name='A', firm_name='F',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='en',
    )
    assert '/tour/2?lang=fr' in en
    fr = mup.render_portal_user_tour(
        2, user_name='A', firm_name='F',
        user_token='tok_long_enough_token_1234567890abcdef',
        lang='fr',
    )
    assert '/tour/2?lang=en' in fr

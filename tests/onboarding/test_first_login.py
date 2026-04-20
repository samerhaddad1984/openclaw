"""Gap 1 — first-login experience: checklist + welcome modal +
empty-state copy."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.onboarding_checklist import (  # noqa: E402
    all_done,
    compute_checklist,
    dismiss,
    ensure_onboarding_schema,
    log_event,
    mark_welcome_seen,
    record_first_login,
    render_checklist_widget,
    render_empty_state,
    render_welcome_modal,
    should_show,
    should_show_welcome,
)


def _mk(tmp_path, *, firm='F1', user='sam'):
    db = tmp_path / 'ob.db'
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE firms (
                firm_code TEXT PRIMARY KEY, name TEXT,
                address TEXT, phone TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                portal_token TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE qbo_connections (
                firm_code TEXT, client_code TEXT, status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, firm_code TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE dashboard_users (
                username TEXT PRIMARY KEY, role TEXT, firm_code TEXT
            )
        """)
        conn.execute(
            "INSERT INTO dashboard_users (username, role, firm_code) "
            "VALUES (?,?,?)",
            (user, 'owner', firm),
        )
        conn.execute(
            "INSERT INTO firms (firm_code, name, address, phone) "
            "VALUES (?,?,?,?)",
            (firm, '', '', ''),
        )
        conn.commit()
    ensure_onboarding_schema(db)
    return db


# --- checklist ---

def test_checklist_fresh_firm_all_open(tmp_path):
    db = _mk(tmp_path)
    items = compute_checklist(db, firm_code='F1', username='sam')
    assert len(items) == 6
    assert not any(i['done'] for i in items)
    # Contains the expected ids in fixed order
    ids = [i['id'] for i in items]
    assert ids == ['firm_profile', 'first_client', 'connect_qbo',
                    'portal_sent', 'first_document', 'guide_viewed']


def test_firm_profile_tick_auto_completes(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE firms SET name='Acme CPA', "
            "address='1 Main', phone='555-1111' WHERE firm_code='F1'"
        )
        conn.commit()
    items = compute_checklist(db, firm_code='F1', username='sam')
    by_id = {i['id']: i for i in items}
    assert by_id['firm_profile']['done'] is True


def test_first_client_auto_completes(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, firm_code) VALUES ('C1','F1')"
        )
        conn.commit()
    items = compute_checklist(db, firm_code='F1', username='sam')
    by_id = {i['id']: i for i in items}
    assert by_id['first_client']['done'] is True
    # Portal not yet — token not set
    assert by_id['portal_sent']['done'] is False


def test_qbo_connected_auto_completes(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_connections (firm_code, client_code, status) "
            "VALUES ('F1','C1','active')"
        )
        conn.commit()
    items = compute_checklist(db, firm_code='F1', username='sam')
    by_id = {i['id']: i for i in items}
    assert by_id['connect_qbo']['done'] is True


def test_guide_viewed_auto_completes(tmp_path):
    db = _mk(tmp_path)
    log_event(db, username='sam', firm_code='F1', event_type='viewed_guide')
    items = compute_checklist(db, firm_code='F1', username='sam')
    by_id = {i['id']: i for i in items}
    assert by_id['guide_viewed']['done'] is True


def test_all_done_ignores_dismissable(tmp_path):
    # connect_qbo is dismissable — remaining 5 done is enough
    items = [
        {'id': 'firm_profile', 'done': True},
        {'id': 'first_client', 'done': True},
        {'id': 'connect_qbo', 'done': False, 'dismissable': True},
        {'id': 'portal_sent', 'done': True},
        {'id': 'first_document', 'done': True},
        {'id': 'guide_viewed', 'done': True},
    ]
    assert all_done(items) is True


def test_dismiss_persists(tmp_path):
    db = _mk(tmp_path)
    assert should_show(db, username='sam') is True
    dismiss(db, username='sam')
    assert should_show(db, username='sam') is False


def test_all_done_auto_hides_widget(tmp_path):
    db = _mk(tmp_path)
    # Populate so every item is done
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE firms SET name='Acme', address='1 Main', phone='555' "
            "WHERE firm_code='F1'"
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, portal_token) "
            "VALUES ('C1','F1','abc')"
        )
        conn.execute(
            "INSERT INTO qbo_connections (firm_code, client_code, status) "
            "VALUES ('F1','C1','active')"
        )
        conn.execute(
            "INSERT INTO documents (document_id, firm_code) "
            "VALUES ('D1','F1')"
        )
        conn.commit()
    log_event(db, username='sam', firm_code='F1', event_type='viewed_guide')
    assert should_show(db, username='sam') is False


# --- welcome modal ---

def test_welcome_modal_shows_first_time(tmp_path):
    db = _mk(tmp_path)
    record_first_login(db, username='sam')
    assert should_show_welcome(db, username='sam') is True


def test_welcome_modal_not_shown_after_ack(tmp_path):
    db = _mk(tmp_path)
    record_first_login(db, username='sam')
    mark_welcome_seen(db, username='sam', tour_taken=False)
    assert should_show_welcome(db, username='sam') is False


def test_tour_completion_tracked(tmp_path):
    db = _mk(tmp_path)
    record_first_login(db, username='sam')
    mark_welcome_seen(db, username='sam', tour_taken=True)
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT tour_completed_at FROM dashboard_users "
            "WHERE username='sam'"
        ).fetchone()
    assert row[0] is not None


def test_record_first_login_idempotent(tmp_path):
    db = _mk(tmp_path)
    record_first_login(db, username='sam')
    with sqlite3.connect(db) as conn:
        ts1 = conn.execute(
            "SELECT first_login_at FROM dashboard_users WHERE username='sam'"
        ).fetchone()[0]
    record_first_login(db, username='sam')
    with sqlite3.connect(db) as conn:
        ts2 = conn.execute(
            "SELECT first_login_at FROM dashboard_users WHERE username='sam'"
        ).fetchone()[0]
    assert ts1 == ts2


# --- render ---

def test_render_checklist_hidden_when_all_done():
    items = [
        {'id': 'firm_profile', 'done': True, 'label_en': 'a', 'label_fr': 'a', 'href': '/'},
        {'id': 'first_client', 'done': True, 'label_en': 'b', 'label_fr': 'b', 'href': '/'},
        {'id': 'connect_qbo', 'done': True, 'label_en': 'c', 'label_fr': 'c', 'href': '/'},
        {'id': 'portal_sent', 'done': True, 'label_en': 'd', 'label_fr': 'd', 'href': '/'},
        {'id': 'first_document', 'done': True, 'label_en': 'e', 'label_fr': 'e', 'href': '/'},
        {'id': 'guide_viewed', 'done': True, 'label_en': 'f', 'label_fr': 'f', 'href': '/'},
    ]
    assert render_checklist_widget(items) == ''


def test_render_checklist_shows_items_with_dismiss():
    items = [
        {'id': 'firm_profile', 'done': False,
         'label_en': 'Complete your firm profile',
         'label_fr': 'Complétez le profil',
         'href': '/onboarding/quick_setup'},
    ]
    html = render_checklist_widget(items, lang='en')
    assert 'Complete your firm profile' in html
    assert '/onboarding/checklist/dismiss' in html


def test_render_checklist_french():
    items = [{'id': 'x', 'done': False, 'label_en': 'Add',
               'label_fr': 'Ajouter', 'href': '/'}]
    html = render_checklist_widget(items, lang='fr')
    assert 'Ajouter' in html
    assert 'Démarrage' in html


def test_render_welcome_modal_localized():
    assert 'Welcome to OtoCPA' in render_welcome_modal('en')
    assert 'Bienvenue sur OtoCPA' in render_welcome_modal('fr')


# --- empty states ---

def test_empty_state_queue_has_cta():
    html = render_empty_state('/queue', lang='en')
    assert 'Upload receipts' in html
    assert 'href="/upload"' in html


def test_empty_state_clients_cta():
    html = render_empty_state('/clients', lang='en')
    assert 'Add your first client' in html
    assert 'href="/clients/new"' in html


def test_empty_state_all_major_paths_have_copy():
    for path in ('/queue', '/clients', '/reconciliation',
                  '/financial_statements', '/audit/engagements'):
        for lang in ('en', 'fr'):
            html = render_empty_state(path, lang=lang)
            assert html, f'{path}/{lang} empty'
            assert 'href=' in html

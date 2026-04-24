"""Phase 1 — single-user portal nav completeness.

Every legacy /c/{token} visitor now sees the same user-facing tabs
the multi-user portal grew in commit a4d57067a, minus the admin
tab (single-user mode has no role system).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_rd():
    if 'review_dashboard' in sys.modules:
        return sys.modules['review_dashboard']
    spec = importlib.util.spec_from_file_location(
        'review_dashboard',
        str(ROOT / 'scripts' / 'review_dashboard.py'),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules['review_dashboard'] = module
    spec.loader.exec_module(module)
    return module


RD = _load_rd()


# ---------------------------------------------------------------------------
# 7-tab nav
# ---------------------------------------------------------------------------


def test_single_user_sees_7_tabs():
    """Upload, Documents, Bank, Messages, My uploads, Tasks, Settings."""
    html = RD._portal_tabs('upload', 'TOKEN', lang='en')
    for href in (
        '/c/TOKEN',
        '/c/TOKEN/documents',
        '/c/TOKEN/bank',
        '/c/TOKEN/messages',
        '/c/TOKEN/my_uploads',
        '/c/TOKEN/tasks',
        '/c/TOKEN/settings',
    ):
        assert href in html, f'missing {href}'


def test_single_user_nav_excludes_admin_tab():
    html = RD._portal_tabs('upload', 'TOK', lang='en')
    assert '/c/TOK/admin' not in html
    assert 'Team' not in html
    assert 'Équipe' not in html


def test_single_user_bilingual_nav_fr():
    html = RD._portal_tabs('upload', 'TOK', lang='fr')
    # French labels
    assert 'Téléverser' in html
    assert 'Banque' in html
    assert 'Mes téléversements' in html
    assert 'Tâches' in html
    assert 'Paramètres' in html


def test_single_user_bilingual_nav_en():
    html = RD._portal_tabs('upload', 'TOK', lang='en')
    assert 'Upload' in html
    assert 'Bank' in html
    assert 'My uploads' in html
    assert 'Tasks' in html
    assert 'Settings' in html


# ---------------------------------------------------------------------------
# Settings renderer for single-user mode
# ---------------------------------------------------------------------------


def test_settings_route_renders_rotate_block():
    html = RD._render_single_portal_settings(
        client={'client_code': 'ACME', 'client_name': 'Acme',
                'language': 'en', 'contact_email': 'owner@acme.com',
                'portal_mode': 'single'},
        token='TOK', lang='en',
    )
    assert 'Rotate my access link' in html
    assert '/c/TOK/rotate_token' in html
    assert 'data-testid="rotate-block"' in html


def test_settings_route_shows_upgrade_block_in_single_mode():
    html = RD._render_single_portal_settings(
        client={'client_code': 'ACME', 'client_name': 'Acme',
                'language': 'en', 'contact_email': 'owner@acme.com',
                'portal_mode': 'single'},
        token='TOK', lang='en',
    )
    assert 'Upgrade to multi-user' in html
    assert '/c/TOK/upgrade' in html
    assert 'data-testid="upgrade-block"' in html


def test_settings_route_hides_upgrade_block_when_already_multi():
    html = RD._render_single_portal_settings(
        client={'client_code': 'ACME', 'client_name': 'Acme',
                'language': 'en', 'contact_email': 'owner@acme.com',
                'portal_mode': 'multi'},
        token='TOK', lang='en',
    )
    assert 'data-testid="upgrade-block"' not in html
    assert '/c/TOK/upgrade' not in html


def test_settings_route_bilingual_fr():
    html = RD._render_single_portal_settings(
        client={'client_code': 'ACME', 'client_name': 'Acme',
                'language': 'fr', 'contact_email': 'owner@acme.com',
                'portal_mode': 'single'},
        token='TOK', lang='fr',
    )
    assert 'Renouveler mon lien' in html
    assert 'Paramètres' in html
    assert 'Passer au mode multi-utilisateurs' in html


# ---------------------------------------------------------------------------
# my_uploads + tasks single-user helpers
# ---------------------------------------------------------------------------


def test_my_uploads_for_client_lists_all_docs(tmp_path):
    import sqlite3
    db = tmp_path / 'u.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT UNIQUE,
                firm_code TEXT, client_code TEXT,
                uploaded_at TEXT,
                uploaded_by_portal_user_id INTEGER,
                review_status TEXT,
                vendor TEXT, amount REAL,
                document_date TEXT,
                manual_hold_reason TEXT,
                file_name TEXT,
                uploader_name TEXT, uploader_email TEXT
            );
            INSERT INTO documents
              (document_id, firm_code, client_code, uploaded_at,
               review_status, vendor, file_name) VALUES
              ('D1','FIRM','ACME','2026-04-20','Approved',
               'Costco','costco.pdf'),
              ('D2','FIRM','ACME','2026-04-21','Rejected',
               'Bell','bell.pdf'),
              ('D3','FIRM','OTHER','2026-04-22','Approved',
               'Other','other.pdf');
            """
        )
        conn.commit()
    from src.integrations.portal_my_uploads import my_uploads_for_client
    uploads = my_uploads_for_client(
        db, firm_code='FIRM', client_code='ACME',
    )
    codes = [u['document_id'] for u in uploads]
    assert set(codes) == {'D1', 'D2'}


def test_my_uploads_page_works_without_portal_user_id():
    """Single-user mode doesn't have a portal user id; the renderer
    must not crash on a minimal portal_user shell."""
    from src.integrations.portal_my_uploads import render_my_uploads_page
    nav = RD._portal_tabs('my_uploads', 'TOK', lang='en')
    html = render_my_uploads_page(
        client={'client_code': 'ACME', 'client_name': 'Acme'},
        user_token='TOK',
        portal_user={'id': 0, 'role': 'owner', 'email': ''},
        uploads=[],
        nav_html=nav,
    )
    assert '/c/TOK/tasks' in html
    assert '/c/TOK/settings' in html


def test_tasks_page_works_in_single_user_mode():
    from src.integrations.client_requests import render_client_tasks_page
    nav = RD._portal_tabs('tasks', 'TOK', lang='fr')
    html = render_client_tasks_page(
        client={'client_code': 'ACME', 'client_name': 'Acme'},
        user_token='TOK',
        portal_user={'id': 0, 'role': 'owner', 'email': ''},
        requests=[],
        nav_html=nav,
    )
    # Single-user nav is injected, /c/ prefix, no admin tab.
    assert '/c/TOK/my_uploads' in html
    assert '/c/TOK/settings' in html
    assert '/c/TOK/admin' not in html

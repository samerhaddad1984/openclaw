"""Portal nav completeness.

Simulates what the admin actually sees after the UI-wiring fix:

  - `/cp/{token}/upload` now renders with a nav that points at `/cp/`
    (NOT `/c/`) and includes Upload, Documents, Messages, My Uploads,
    Tasks, Settings — plus Team for admin-role users only.
  - The standalone admin / my_uploads / tasks pages also embed the
    same nav so clicking a tab from those pages works too.

These tests exercise the pure-Python renderers — the HTTP server is
not booted. We import the module-level helpers and assert the
returned HTML. That matches how the rest of tests/portal/ works.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_review_dashboard():
    """Load scripts/review_dashboard.py as a module. The dashboard is a
    script, not a package, so we import by file path."""
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


RD = _load_review_dashboard()


# ---------------------------------------------------------------------------
# _portal_tabs: core nav generator
# ---------------------------------------------------------------------------


def test_single_mode_nav_uses_c_prefix_and_includes_bank():
    """Legacy /c/ nav continues to use /c/ prefix and keeps the Bank
    tab. After the single-user UX expansion it also surfaces
    my_uploads / tasks / settings — but never the admin tab, because
    single-user mode has no role system."""
    html = RD._portal_tabs('upload', 'TOKEN')
    # Still /c/ prefixed
    assert 'href="/c/TOKEN"' in html
    assert 'href="/c/TOKEN/documents"' in html
    assert 'href="/c/TOKEN/bank"' in html
    assert 'href="/c/TOKEN/messages"' in html
    # No Team/admin tab in single mode.
    assert '/c/TOKEN/admin' not in html
    assert 'Team' not in html
    assert 'Équipe' not in html


def test_multi_mode_nav_uses_cp_prefix():
    """Multi-mode nav points at /cp/, not /c/."""
    html = RD._portal_tabs('upload', 'TOKEN', is_multi=True, role='admin')
    # Every tab link starts with /cp/TOKEN
    assert 'href="/cp/TOKEN"' in html
    assert 'href="/cp/TOKEN/documents"' in html
    # And nothing points to /c/TOKEN
    assert 'href="/c/TOKEN' not in html


def test_contributor_nav_excludes_team_tab():
    """Role=contributor sees upload/documents/messages/my_uploads/
    tasks/settings — but no Team tab."""
    html = RD._portal_tabs('upload', 'TOK',
                            is_multi=True, role='contributor')
    for href in ('/cp/TOK', '/cp/TOK/documents', '/cp/TOK/messages',
                 '/cp/TOK/my_uploads', '/cp/TOK/tasks',
                 '/cp/TOK/settings'):
        assert href in html, f'missing {href}'
    assert '/cp/TOK/admin' not in html
    assert 'Team' not in html
    assert 'Équipe' not in html


def test_admin_nav_includes_team_tab():
    """Role=admin sees all contributor tabs PLUS Team."""
    html = RD._portal_tabs('upload', 'TOK', is_multi=True, role='admin')
    for href in ('/cp/TOK', '/cp/TOK/documents', '/cp/TOK/messages',
                 '/cp/TOK/my_uploads', '/cp/TOK/tasks',
                 '/cp/TOK/settings', '/cp/TOK/admin'):
        assert href in html, f'missing {href}'


def test_admin_nav_bilingual_french():
    html = RD._portal_tabs('upload', 'TOK',
                            is_multi=True, role='admin', lang='fr')
    assert 'Équipe' in html
    assert 'Tâches' in html
    assert 'Paramètres' in html
    assert 'Mes téléversements' in html


def test_admin_nav_bilingual_english():
    html = RD._portal_tabs('upload', 'TOK',
                            is_multi=True, role='admin', lang='en')
    assert 'Team' in html
    assert 'Tasks' in html
    assert 'Settings' in html
    assert 'My uploads' in html


def test_active_tab_highlighted():
    html = RD._portal_tabs('tasks', 'TOK',
                            is_multi=True, role='admin', lang='en')
    # Active class applies only to the tasks link.
    assert 'class="active" href="/cp/TOK/tasks"' in html


# ---------------------------------------------------------------------------
# Render path: upload page for multi-mode admin
# ---------------------------------------------------------------------------


def test_upload_page_multi_admin_nav_matches_cp_prefix():
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    html = RD.render_portal_upload(client, 'USER_TOK', is_multi=True,
                                    role='admin')
    # No legacy /c/USER_TOK anywhere in nav (the upload form POST
    # still goes to /c/ in single mode code — let us allow that but
    # confirm the NAV tabs are /cp/).
    # Admin tab present:
    assert 'href="/cp/USER_TOK/admin"' in html
    assert 'Team' in html
    assert 'href="/cp/USER_TOK/tasks"' in html
    assert 'href="/cp/USER_TOK/my_uploads"' in html
    assert 'href="/cp/USER_TOK/settings"' in html


def test_upload_page_multi_contributor_no_team():
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    html = RD.render_portal_upload(client, 'USER_TOK', is_multi=True,
                                    role='contributor')
    assert 'href="/cp/USER_TOK/admin"' not in html
    assert 'Team' not in html
    assert 'href="/cp/USER_TOK/tasks"' in html


def test_upload_page_single_mode_keeps_c_prefix_and_no_admin():
    """Single-mode upload page uses /c/ URLs and no admin tab."""
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'fr'}
    html = RD.render_portal_upload(client, 'SINGLE_TOK')
    assert 'href="/c/SINGLE_TOK"' in html
    # Must not leak /cp/ URLs into the legacy nav.
    assert '/cp/SINGLE_TOK' not in html
    # Admin route is multi-only.
    assert '/c/SINGLE_TOK/admin' not in html


# ---------------------------------------------------------------------------
# Settings self-service page
# ---------------------------------------------------------------------------


def test_settings_page_renders_with_rotate_button():
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    portal_user = {'id': 1, 'email': 'alice@acme.com',
                   'role': 'contributor'}
    html = RD._render_portal_user_settings(
        client=client, user_token='TOK',
        portal_user=portal_user, lang='en',
    )
    assert '/cp/TOK/rotate_my_token' in html
    assert 'Rotate my access link' in html
    assert 'alice@acme.com' in html


def test_settings_page_bilingual_fr():
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'fr'}
    portal_user = {'id': 1, 'email': 'marie@acme.com',
                   'role': 'admin'}
    html = RD._render_portal_user_settings(
        client=client, user_token='TOK',
        portal_user=portal_user, lang='fr',
    )
    assert 'Renouveler mon lien' in html
    assert 'Paramètres' in html


# ---------------------------------------------------------------------------
# Standalone pages (my_uploads / tasks / admin) render the nav when
# nav_html is supplied by the dispatcher
# ---------------------------------------------------------------------------


def test_my_uploads_page_injects_nav():
    from src.integrations.portal_my_uploads import render_my_uploads_page
    nav = RD._portal_tabs('my_uploads', 'TOK',
                           is_multi=True, role='admin', lang='en')
    html = render_my_uploads_page(
        client={'client_code': 'ACME', 'client_name': 'Acme'},
        user_token='TOK',
        portal_user={'id': 1, 'role': 'admin'},
        uploads=[],
        nav_html=nav,
    )
    assert 'href="/cp/TOK/admin"' in html
    assert 'Team' in html
    assert 'href="/cp/TOK/tasks"' in html


def test_tasks_page_injects_nav():
    from src.integrations.client_requests import render_client_tasks_page
    nav = RD._portal_tabs('tasks', 'TOK',
                           is_multi=True, role='contributor', lang='fr')
    html = render_client_tasks_page(
        client={'client_code': 'ACME', 'client_name': 'Acme'},
        user_token='TOK',
        portal_user={'id': 1, 'role': 'contributor'},
        requests=[],
        nav_html=nav,
    )
    # Contributor → no Team tab
    assert '/cp/TOK/admin' not in html
    assert 'Équipe' not in html
    # But the other tabs are there
    assert 'href="/cp/TOK/my_uploads"' in html
    assert 'href="/cp/TOK/settings"' in html


def test_admin_page_injects_nav():
    from src.integrations.multi_user_portal import render_user_portal_admin
    nav = RD._portal_tabs('admin', 'TOK',
                           is_multi=True, role='admin', lang='en')
    html = render_user_portal_admin(
        client={'client_code': 'ACME', 'client_name': 'Acme'},
        user_token='TOK',
        users=[],
        invitations=[],
        audit_entries=[],
        nav_html=nav,
    )
    # Active tab is Team.
    assert 'class="active" href="/cp/TOK/admin"' in html
    assert 'Team' in html


# ---------------------------------------------------------------------------
# Required-by-spec phrasing
# ---------------------------------------------------------------------------


def test_admin_sees_team_tab_in_nav():
    """Admin role user sees Team Management link in portal navigation."""
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    html = RD.render_portal_upload(client, 'ADMIN_TOK',
                                    is_multi=True, role='admin')
    assert '/cp/ADMIN_TOK/admin' in html
    assert b'Team' in html.encode() or b'\xc3\x89quipe' in html.encode()


def test_contributor_does_not_see_team_tab():
    """Contributor role user does NOT see Team link."""
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    html = RD.render_portal_upload(client, 'CONTRIB_TOK',
                                    is_multi=True, role='contributor')
    # No /cp/.../admin link in nav
    assert '/cp/CONTRIB_TOK/admin' not in html
    # No "Team" literal or French equivalent
    assert 'Team' not in html
    assert 'Équipe' not in html


def test_all_phase_1_routes_in_admin_nav():
    """Every Phase 1.x route is reachable via nav click."""
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    html = RD.render_portal_upload(client, 'TOK',
                                    is_multi=True, role='admin')
    # Phase 1.1
    assert '/cp/TOK/admin' in html
    # Phase 1.2
    assert '/cp/TOK/settings' in html
    # Phase 1.3
    assert '/cp/TOK/my_uploads' in html
    # Phase 1.4
    assert '/cp/TOK/tasks' in html


def test_portal_bilingual_nav_fr_uses_accented_labels():
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'fr'}
    html_fr = RD.render_portal_upload(client, 'TOK_FR',
                                       is_multi=True, role='admin')
    assert 'Équipe' in html_fr
    assert 'Tâches' in html_fr


def test_portal_bilingual_nav_en_uses_english_labels():
    client = {'client_code': 'ACME', 'client_name': 'Acme',
              'language': 'en'}
    html_en = RD.render_portal_upload(client, 'TOK_EN',
                                       is_multi=True, role='admin')
    assert 'Team' in html_en
    assert 'Tasks' in html_en

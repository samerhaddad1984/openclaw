"""Phase 7: PWA manifest + icon + iOS meta tags.

Runs against the pure builder helpers so we don't have to spin up
the HTTP server. The server route is a thin wrapper that JSON-
serializes ``_build_portal_manifest`` + the language resolution
from the ``clients`` / ``client_portal_users`` tables.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import the dashboard module once for helper-level tests.
rd = importlib.import_module('scripts.review_dashboard')


# ---------------------------------------------------------------------------
# Manifest body
# ---------------------------------------------------------------------------

def test_manifest_fr_for_french_user():
    m = rd._build_portal_manifest('TKN', lang='fr', is_multi=False)
    assert m['lang'] == 'fr-CA'
    assert 'Portail' in m['name']
    assert m['start_url'] == '/c/TKN/upload'
    assert m['scope'] == '/c/TKN/'


def test_manifest_en_for_english_user():
    m = rd._build_portal_manifest('TKN', lang='en', is_multi=False)
    assert m['lang'] == 'en-CA'
    assert 'Client Portal' in m['name']


def test_manifest_multi_user_uses_cp_scope():
    m = rd._build_portal_manifest('UTKN', lang='fr', is_multi=True)
    assert m['scope'] == '/cp/UTKN/'
    assert m['start_url'] == '/cp/UTKN/upload'


def test_manifest_has_two_icon_sizes():
    m = rd._build_portal_manifest('TKN')
    sizes = {i['sizes'] for i in m['icons']}
    assert sizes == {'192x192', '512x512'}
    for icon in m['icons']:
        assert icon['type'] == 'image/png'
        assert 'maskable' in icon['purpose']


def test_manifest_theme_color_is_brand_green():
    m = rd._build_portal_manifest('TKN')
    assert m['theme_color'] == '#2a8759'


def test_manifest_is_standalone_portrait():
    m = rd._build_portal_manifest('TKN')
    assert m['display'] == 'standalone'
    assert m['orientation'] == 'portrait'


# ---------------------------------------------------------------------------
# Icons exist + are the right sizes
# ---------------------------------------------------------------------------

def test_icons_exist_and_correct_sizes():
    from PIL import Image
    p192 = ROOT / 'static' / 'pwa' / 'icon-192.png'
    p512 = ROOT / 'static' / 'pwa' / 'icon-512.png'
    assert p192.exists(), 'icon-192.png missing'
    assert p512.exists(), 'icon-512.png missing'
    with Image.open(p192) as im:
        assert im.size == (192, 192)
    with Image.open(p512) as im:
        assert im.size == (512, 512)


# ---------------------------------------------------------------------------
# Shell embeds PWA meta tags
# ---------------------------------------------------------------------------

def test_apple_meta_tags_present():
    html = rd._portal_page_shell(
        {'client_code': 'C1', 'client_name': 'Widget Co',
         'language': 'fr'},
        token='TKN', tab='upload', body='<div></div>',
    )
    assert 'apple-mobile-web-app-capable' in html
    assert 'apple-touch-icon' in html
    assert '#2a8759' in html  # theme-color meta
    assert 'rel="manifest"' in html


def test_service_worker_registered_in_shell():
    html = rd._portal_page_shell(
        {'client_code': 'C1', 'client_name': 'Widget Co',
         'language': 'fr'},
        token='TKN', tab='upload', body='<div></div>',
    )
    assert "/static/pwa/sw.js" in html
    assert 'serviceWorker' in html


# ---------------------------------------------------------------------------
# Offline fallback + install prompt
# ---------------------------------------------------------------------------

def test_offline_page_bilingual():
    html = rd._render_offline_page()
    assert 'Hors ligne' in html
    assert 'Offline' in html
    assert 'Reconnectez' in html


def test_install_button_bilingual_fr():
    html = rd._render_install_ui('fr')
    assert "Ajouter à l'écran d'accueil" in html
    assert 'pwa-install' in html
    # iOS hint also present (hidden by default).
    assert 'pwa-ios-hint' in html


def test_install_button_bilingual_en():
    html = rd._render_install_ui('en')
    assert 'Add to Home Screen' in html
    assert 'Share button' in html


def test_install_prompt_starts_hidden():
    html = rd._render_install_ui('fr')
    # Both nodes start hidden; JS flips the right one for the
    # user's environment.
    assert 'id="pwa-install"' in html
    assert 'display:none' in html


# ---------------------------------------------------------------------------
# Service worker
# ---------------------------------------------------------------------------

def test_service_worker_file_exists():
    sw = ROOT / 'static' / 'pwa' / 'sw.js'
    assert sw.exists()
    text = sw.read_text('utf-8')
    # Must precache the offline page.
    assert "'/c/offline'" in text or '"/c/offline"' in text
    # Network-first for navigation.
    assert "mode === 'navigate'" in text or 'mode === "navigate"' in text


def test_service_worker_scope_correct():
    sw = ROOT / 'static' / 'pwa' / 'sw.js'
    text = sw.read_text('utf-8')
    # POST requests bypass the cache (never queue mutations).
    assert "req.method !== 'GET'" in text


def test_offline_page_renders():
    html = rd._render_offline_page()
    assert '<!doctype html>' in html.lower()
    assert 'location.reload()' in html

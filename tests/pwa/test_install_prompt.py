"""Phase 9: install prompt + iOS Safari manual instructions.

Android Chrome fires ``beforeinstallprompt``; iOS Safari doesn't, so
we fall back to a visible tooltip with the share-sheet instruction.
Both must be bilingual.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

rd = importlib.import_module('scripts.review_dashboard')


# ---------------------------------------------------------------------------
# Android install prompt
# ---------------------------------------------------------------------------

def test_install_prompt_shows_on_android():
    html = rd._portal_page_shell(
        {'client_code': 'C1', 'client_name': 'Widget Co',
         'language': 'fr'},
        token='TKN', tab='upload', body='<div></div>',
    )
    # beforeinstallprompt listener present.
    assert "beforeinstallprompt" in html
    # Shows an install button once the browser fires the event.
    assert "id='pwa-install'" in html or 'id="pwa-install"' in html


def test_install_handler_calls_native_prompt():
    html = rd._portal_page_shell(
        {'language': 'fr'},
        token='T', tab='upload', body='',
    )
    # prompt() on the deferred event (user gesture preserved).
    assert '_deferredInstall.prompt()' in html


# ---------------------------------------------------------------------------
# iOS Safari fallback
# ---------------------------------------------------------------------------

def test_ios_instruction_shows_on_safari():
    html = rd._portal_page_shell(
        {'language': 'fr'},
        token='T', tab='upload', body='',
    )
    # UA sniffer for iOS Safari.
    assert 'iPhone|iPad|iPod' in html
    # Detects standalone (already installed) to suppress the hint.
    assert 'standalone' in html


def test_ios_hint_bilingual():
    fr = rd._render_install_ui('fr')
    en = rd._render_install_ui('en')
    assert "Ajouter à l'écran d'accueil" in fr
    assert 'Add to Home Screen' in en
    # Share button instruction appears in both.
    assert 'Partager' in fr
    assert 'Share button' in en


# ---------------------------------------------------------------------------
# No nagging
# ---------------------------------------------------------------------------

def test_install_prompt_not_intrusive_by_default():
    html = rd._render_install_ui('fr')
    # Both the install button + iOS hint start with display:none;
    # JS un-hides the one that applies. This avoids showing two
    # prompts on any single browser.
    assert html.count('display:none') >= 2

"""Sweep B: hardcoded EN labels that match a ``ui_labels`` key must
now use ``ui_t`` — and the renders must produce the FR term for FR
sessions.
"""
from __future__ import annotations

import re
from pathlib import Path

from src.i18n.ui_labels import LABELS


ROOT = Path(__file__).resolve().parent.parent.parent
DASHBOARD = ROOT / "scripts/review_dashboard.py"


def test_no_hardcoded_button_label_matches_ui_labels_key() -> None:
    """Scan production code for hardcoded EN button / submit / h1-h6
    text. If any text exactly matches the EN value of a ``ui_labels``
    entry, it should be using ``ui_t(key, lang)`` instead.
    """
    en_to_key: dict[str, str] = {}
    for k, v in LABELS.items():
        en = v.get("en")
        if en:
            en_to_key.setdefault(en, k)

    PATTERNS = [
        re.compile(r'<button[^>]*>\s*([^<>{}\n]+?)\s*</button>'),
        re.compile(r'<a[^>]*class="[^"]*\bbtn[^"]*"[^>]*>\s*([^<>{}\n]+?)\s*</a>'),
        re.compile(r'<input[^>]*type="submit"[^>]*value="([^"]+)"'),
        re.compile(r'<input[^>]*value="([^"]+)"[^>]*type="submit"'),
        re.compile(r'<h[1-6][^>]*>\s*([^<>{}\n]+?)\s*</h[1-6]>'),
    ]

    offenders = []
    for p in (ROOT / "scripts").rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        content = p.read_text(encoding="utf-8")
        for rx in PATTERNS:
            for m in rx.finditer(content):
                text = m.group(1).strip()
                if not text or len(text) < 2 or len(text) > 60:
                    continue
                if text[0].isdigit():
                    continue
                if any(c in text for c in "{$%&"):
                    continue
                if "/" in text:
                    continue
                if text in en_to_key:
                    lineno = content[: m.start()].count("\n") + 1
                    offenders.append(
                        (str(p.relative_to(ROOT)), lineno, text, en_to_key[text])
                    )
    assert not offenders, (
        "hardcoded English button/label still matches a ui_labels key; "
        "migrate to ui_t():\n"
        + "\n".join(f"  {f}:{ln}: {t!r} → {k}" for f, ln, t, k in offenders[:10])
    )


def test_render_profile_page_uses_fr_heading() -> None:
    import scripts.review_dashboard as rd
    user = {"username": "marie", "language": "fr", "display_name": "Marie",
            "email": "m@example.com"}
    html_fr = rd.render_profile_page(user, lang="fr")
    html_en = rd.render_profile_page(user, lang="en")
    # Heading now interpolates ui_t("profile", lang) — FR: Profil, EN: Profile
    assert "Profil" in html_fr
    assert "Profile" in html_en
    # Literal {ui_t( must not leak — proves the site is inside an f-string
    assert "{ui_t(" not in html_fr
    assert "{ui_t(" not in html_en


def test_render_firm_form_cancel_button_is_translated() -> None:
    import scripts.review_dashboard as rd
    # render_firm_form signature: probe via direct source scan rather
    # than render, because its full inputs are complex.
    content = DASHBOARD.read_text(encoding="utf-8")
    assert 'Cancel</a>' not in content, (
        "raw 'Cancel</a>' anchor still present in dashboard"
    )
    assert 'ui_t("cancel"' in content or "ui_t('cancel'" in content


def test_render_client_form_bank_sync_disconnect_buttons_translated() -> None:
    content = DASHBOARD.read_text(encoding="utf-8")
    # The bank-feeds card inside render_client_form used hardcoded
    # "Sync" and "Disconnect" — must now interpolate ui_t.
    assert "Sync\n                    </button>" not in content
    assert "Disconnect\n                    </button>" not in content
    assert 'ui_t("sync", lang)' in content
    assert 'ui_t("disconnect", lang)' in content


def test_portal_whatsapp_open_chat_button_respects_client_lang() -> None:
    """The portal WhatsApp card's "Open chat" anchor is the one
    case where the enclosing function doesn't take ``lang`` as a
    parameter. It reads ``client.language`` locally and passes that
    to ``ui_t``."""
    content = DASHBOARD.read_text(encoding="utf-8")
    # Function should derive client_lang from client
    assert 'client_lang = (client.get("language") or "fr")' in content
    # And use it for the Open chat label
    assert 'ui_t("open_chat", client_lang)' in content

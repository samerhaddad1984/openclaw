"""Regression guards for the second-wave admin button migration.

The Phase 2 sweep pass migrated an additional 20 button labels beyond
the initial 13. These tests assert FR/EN correctness and catch any
regression that would reintroduce raw English.
"""
from __future__ import annotations

from pathlib import Path

import pytest


DASHBOARD = Path(__file__).resolve().parent.parent.parent / "scripts/review_dashboard.py"


@pytest.mark.parametrize("raw,key", [
    (">Add<", "add"),
    (">Allocate<", "allocate"),
    (">Assign<", "assign"),
    (">Back<", "back"),
    (">Back to Diagnostics<", "back_to_diagnostics"),
    (">Create<", "create"),
    (">Delete<", "delete"),
    (">Disable 2FA<", "disable_2fa"),
    (">Disconnect<", "disconnect"),
    (">Enable 2FA<", "enable_2fa"),
    (">Estimate<", "estimate"),
    (">Filter<", "filter"),
    (">Load<", "load"),
    (">Match<", "match"),
    (">Post<", "post"),
    (">Refresh<", "refresh"),
    (">Remove<", "remove"),
    (">Reset<", "reset"),
    (">Reverse<", "reverse"),
    (">Sync<", "sync"),
])
def test_raw_english_button_no_longer_in_dashboard(raw: str, key: str) -> None:
    """Scan review_dashboard.py — no more hardcoded English
    ``>Label<`` button text. Each migrated label must now use
    ``ui_t(key, lang)`` either as ``{ui_t(...)}`` inside an f-string
    or as concatenation in a plain string."""
    content = DASHBOARD.read_text(encoding="utf-8")
    assert raw not in content, (
        f"raw English button label {raw!r} still present; should use "
        f'ui_t("{key}", lang)'
    )
    # And verify the ui_t key IS in use somewhere (sanity check the
    # migration actually landed)
    assert f'ui_t("{key}"' in content or f"ui_t('{key}'" in content, (
        f"expected ui_t({key!r}, lang) somewhere in the dashboard"
    )


def test_render_2fa_settings_renders_french_label() -> None:
    import scripts.review_dashboard as rd
    user = {"username": "marie", "totp_enabled": True, "totp_secret": ""}
    html = rd.render_2fa_settings(user=user, lang="fr")
    assert "Désactiver la 2FA" in html
    assert "Disable 2FA" not in html
    # Literal {ui_t( must not leak (proves it's a real f-string, not
    # plain-text interpolation)
    assert "{ui_t(" not in html


def test_render_2fa_settings_renders_english_label() -> None:
    import scripts.review_dashboard as rd
    user = {"username": "marie", "totp_enabled": True, "totp_secret": ""}
    html = rd.render_2fa_settings(user=user, lang="en")
    assert "Disable 2FA" in html
    assert "Désactiver la 2FA" not in html


def test_render_2fa_settings_enable_label_translated() -> None:
    """When 2FA is not enabled, the enable button must also be
    translated."""
    import scripts.review_dashboard as rd
    user = {"username": "marie", "totp_enabled": False, "totp_secret": "ABCD"}
    html_fr = rd.render_2fa_settings(user=user, lang="fr")
    html_en = rd.render_2fa_settings(user=user, lang="en")
    assert "Activer la 2FA" in html_fr
    assert "Enable 2FA" in html_en

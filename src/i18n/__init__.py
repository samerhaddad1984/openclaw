"""
src/i18n/__init__.py — shared translation helper for LedgerLink dashboards.

Usage:
    from src.i18n import t
    label = t("logout_btn", "fr")   # → "Déconnexion"
    label = t("logout_btn", "en")   # → "Sign out"

French is the default locale.  If a key is missing in the requested locale,
the function falls back to French, then returns the key itself so the UI
never shows a blank.

JSON locale files live alongside this module:
    src/i18n/fr.json
    src/i18n/en.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_LOCALES_DIR = Path(__file__).parent
_CACHE: dict[str, dict[str, str]] = {}


def _load(lang: str) -> dict[str, str]:
    """Load and cache the JSON file for *lang*."""
    if lang not in _CACHE:
        path = _LOCALES_DIR / f"{lang}.json"
        try:
            _CACHE[lang] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            _CACHE[lang] = {}
    return _CACHE[lang]


def t(key: str, lang: str, **kwargs: Any) -> str:
    """Return the translated string for *key* in *lang*.

    Parameters
    ----------
    key:
        Translation key, e.g. ``"logout_btn"``.
    lang:
        Locale code — ``"fr"`` or ``"en"``.  Anything else is treated as
        ``"fr"`` (French is the platform default for the Quebec market).
    **kwargs:
        Optional named substitutions applied via :py:meth:`str.format`.
        Example: ``t("change_pw_intro", "fr", name="Marie")``
    """
    if lang not in ("fr", "en"):
        lang = "fr"

    strings = _load(lang)
    result: str | None = strings.get(key)

    if result is None:
        # Fallback 1: French
        result = _load("fr").get(key)
    if result is None:
        # Fallback 2: return the key itself (never blank)
        result = key

    if kwargs:
        try:
            result = result.format(**kwargs)
        except (KeyError, IndexError):
            pass  # leave the template literal as-is rather than crash

    return result


def reload_cache() -> None:
    """Clear the in-memory locale cache (useful after updating JSON files)."""
    _CACHE.clear()

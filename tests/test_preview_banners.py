"""Sprint E Phase 1 — PREVIEW banners on incomplete CAS features.

These tests guard against regressions:
- The banner helper is present and produces banner HTML with the
  expected text markers ("PREVIEW", feature name, date).
- Each of the five affected render functions prepends the banner to
  its body before the first "card" content.
"""
from __future__ import annotations

import sys

import pytest


def _load_rd():
    if "rd" in sys.modules:
        return sys.modules["rd"]
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "rd", "/opt/otocpa/scripts/review_dashboard.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rd"] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


@pytest.fixture(scope="module")
def rd():
    return _load_rd()


# ---------------------------------------------------------------------------
# _preview_banner helper
# ---------------------------------------------------------------------------

def test_preview_banner_has_the_four_required_markers(rd):
    html = rd._preview_banner("My Feature")
    # The word PREVIEW must be visible (user's requirement).
    assert "PREVIEW" in html
    # The feature name must appear.
    assert "My Feature" in html
    # Default expected-ready date (user wrote 2026-05-15).
    assert "2026-05-15" in html
    # The refusal-to-submit phrasing from the user's banner spec.
    assert "do not submit" in html


def test_preview_banner_note_is_html_escaped(rd):
    html = rd._preview_banner("X", "tags & \"quotes\" are safe <here>")
    # The literal "&" character gets HTML-escaped.
    assert "&amp;" in html
    assert "&lt;here&gt;" in html


def test_preview_banner_custom_date_propagates(rd):
    html = rd._preview_banner("X", expected_ready_date="2026-06-30")
    assert "2026-06-30" in html
    assert "2026-05-15" not in html


def test_preview_banner_accent_colors_are_the_standard_amber(rd):
    # Users (especially CPAs) expect a visible yellow alert, not red (blocker).
    # Lock the banner palette so a future stylesheet cleanup doesn't drop it.
    html = rd._preview_banner("X")
    assert "#FFF3CD" in html       # background
    assert "#FFC107" in html       # border
    assert "#856404" in html       # text


# ---------------------------------------------------------------------------
# Each affected render function prepends the banner
# ---------------------------------------------------------------------------

_AFFECTED_RENDER_FUNCS = [
    ("render_audit_sample",       "CAS 530"),   # appears in the banner note
    ("render_rep_letter",         "CAS 580"),
    ("render_t2",                 "T2"),
    ("render_filing_summary",     "GST/QST"),
    ("render_revenu_quebec",      "Revenu Québec"),
]


@pytest.mark.parametrize("func_name,marker", _AFFECTED_RENDER_FUNCS)
def test_render_function_source_includes_preview_call(rd, func_name, marker):
    """Spot-check the source of each render function: it must build a
    `preview = _preview_banner(...)` string and reference it in its body.

    We read the dashboard source rather than invoking the render
    functions because their signatures / DB dependencies vary; this
    guard is enough to catch a refactor that silently drops the
    banner.
    """
    import inspect
    src = inspect.getsource(getattr(rd, func_name))
    assert "_preview_banner(" in src, f"{func_name} no longer calls _preview_banner"
    # The body must reference the preview variable. Accept any formatting:
    # "preview +", "preview\n+", etc.
    import re
    assert re.search(r"\bpreview\b", src), (
        f"{func_name} does not reference its preview banner variable"
    )
    # Body assembly must string-concat the preview — either `preview +` /
    # `+ preview` on the same or neighbouring line.
    assert re.search(r"preview\s*\+|\+\s*preview", src), (
        f"{func_name} does not prepend preview to its body"
    )
    assert marker in src, f"{func_name} banner note no longer mentions {marker!r}"


# ---------------------------------------------------------------------------
# Nav chip for PREVIEW features
# ---------------------------------------------------------------------------

def test_sidebar_preview_chip_defined():
    """The sidebar adds a small amber PREVIEW pill next to the three
    affected nav entries (/audit/sample, /audit/rep_letter, /t2)."""
    src = open("/opt/otocpa/scripts/review_dashboard.py").read()
    # Helper var holds the inline pill HTML.
    assert "_preview_chip" in src
    # And it's applied next to the three nav entries.
    assert '"/audit/sample"' in src
    assert '"/audit/rep_letter"' in src
    assert '"/t2"' in src
    # Specifically combined with the chip for each.
    idx_sample = src.find('"/audit/sample", esc(t("samp_title"')
    idx_rep = src.find('"/audit/rep_letter", esc(t("cas_rep_nav"')
    idx_t2 = src.find('"/t2", esc(t("t2_nav_link"')
    for i, name in [(idx_sample, "audit/sample"),
                     (idx_rep, "audit/rep_letter"),
                     (idx_t2, "t2")]:
        assert i > 0, f"PREVIEW chip nav insertion missing for {name}"
        window = src[i:i + 200]
        assert "_preview_chip" in window, (
            f"PREVIEW chip not applied next to /{name} nav entry"
        )

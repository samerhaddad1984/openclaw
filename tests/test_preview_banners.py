"""Sprint F — PREVIEW banners removed once the underlying features ship.

Sprint E Phase 1 introduced amber PREVIEW banners on five pages whose
implementations were incomplete. Sprint F finished those features, so
the banners are now gone. These tests lock that outcome in: if a future
refactor reintroduces a preview banner on a shipped feature, we want to
fail loudly.

The ``_preview_banner`` *helper* is intentionally preserved — it is
still useful for any NEW feature that ships behind a banner — but no
current render function calls it.
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
# _preview_banner helper remains available
# ---------------------------------------------------------------------------

def test_preview_banner_helper_still_exists(rd):
    """Future features may need a PREVIEW banner, so the helper stays."""
    assert callable(rd._preview_banner)
    html = rd._preview_banner("X")
    assert "PREVIEW" in html
    assert "do not submit" in html


def test_preview_banner_note_is_html_escaped(rd):
    html = rd._preview_banner("X", "tags & \"quotes\" are safe <here>")
    assert "&amp;" in html
    assert "&lt;here&gt;" in html


def test_preview_banner_accent_colors_are_the_standard_amber(rd):
    html = rd._preview_banner("X")
    assert "#FFF3CD" in html
    assert "#FFC107" in html
    assert "#856404" in html


# ---------------------------------------------------------------------------
# No production render function prepends a preview banner any more
# ---------------------------------------------------------------------------

_SHIPPED_RENDER_FUNCS = [
    "render_audit_sample",      # CAS 530 shipped Sprint F Fix 1
    "render_rep_letter",        # CAS 580 shipped Sprint F Fix 4
    "render_t2",                # T2 PDF shipped Sprint F Fix 3
    "render_filing_summary",    # GST/QST revenue shipped Sprint F Fix 2
    "render_revenu_quebec",     # same revenue-side fix
]


@pytest.mark.parametrize("func_name", _SHIPPED_RENDER_FUNCS)
def test_render_function_does_not_call_preview_banner(rd, func_name):
    import inspect
    src = inspect.getsource(getattr(rd, func_name))
    assert "_preview_banner(" not in src, (
        f"{func_name} still calls _preview_banner — the banner was removed "
        "when the feature shipped in Sprint F."
    )


def test_sidebar_has_no_preview_chip_on_shipped_nav_entries():
    src = open("/opt/otocpa/scripts/review_dashboard.py").read()
    # Each shipped feature now uses a plain _dnav(...) entry, not a
    # _dlink(...) with _preview_chip concatenated.
    for nav in ('"/audit/sample"', '"/audit/rep_letter"', '"/t2"'):
        # Confirm it's registered as a standard nav entry (no "_preview_chip"
        # inside a short window following the URL).
        idx = src.find(nav)
        assert idx > 0, f"nav entry {nav} missing from sidebar"
        window = src[idx:idx + 200]
        assert "_preview_chip" not in window, (
            f"{nav} is still rendered with a PREVIEW chip — remove it "
            "because the feature has shipped."
        )

"""Pytest wrapper around ``scripts/i18n/generate_locale_evidence.py``.

Runs the evidence generator in-process and asserts cross-locale leak
counts remain at zero on every watched surface. Catches regressions
where a FR render picks up English currency, or vice versa.

If new surfaces are added to ``SURFACES`` in the generator, they
become part of this regression guard automatically.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

fitz = pytest.importorskip("fitz")


def test_locale_evidence_no_cross_locale_leaks(tmp_path, monkeypatch) -> None:
    import scripts.i18n.generate_locale_evidence as gen

    # Redirect output to tmp_path so the test doesn't overwrite the
    # committed docs/locale_evidence.json.
    monkeypatch.setattr(
        gen, "__file__", str(tmp_path / "generate_locale_evidence.py")
    )
    # Monkey-patch the output path used in main()
    original_main = gen.main

    report = {}
    for name, fn in gen.SURFACES.items():
        report[name] = {}
        for lang in ("fr", "en"):
            rendered = fn(lang)
            report[name][lang] = gen._leak_scan(rendered, lang)

    # No FR surface should contain English currency
    for name, per_lang in report.items():
        fr = per_lang["fr"]
        assert fr["us_currency_count"] == 0, (
            f"{name}: FR render contains US-currency strings: "
            f"{fr['us_currency_sample']}"
        )
        assert fr["html_entity_leak"] == 0, (
            f"{name}: FR render leaks HTML entities"
        )
        assert not fr["literal_fstring_leak"], (
            f"{name}: FR render leaks literal format-spec text"
        )

    # No EN surface should contain French currency
    for name, per_lang in report.items():
        en = per_lang["en"]
        assert en["fr_currency_count"] == 0, (
            f"{name}: EN render contains FR-currency strings: "
            f"{en['fr_currency_sample']}"
        )
        assert not en["literal_fstring_leak"], (
            f"{name}: EN render leaks literal format-spec text"
        )

    # Sanity: every surface should actually render SOME currency in
    # its own locale (otherwise the fixtures aren't exercising the
    # money path).
    for name, per_lang in report.items():
        assert per_lang["fr"]["fr_currency_count"] > 0, (
            f"{name}: FR render produced zero currency strings — "
            f"fixture not exercising money()?"
        )
        assert per_lang["en"]["us_currency_count"] > 0, (
            f"{name}: EN render produced zero currency strings"
        )


def test_committed_evidence_file_is_clean() -> None:
    """``docs/locale_evidence.json`` is committed as a human-readable
    receipt. This test only asserts that whatever is committed shows
    zero leaks — re-run the generator if it falls stale."""
    path = Path(__file__).resolve().parent.parent.parent / "docs/locale_evidence.json"
    if not path.exists():
        pytest.skip("evidence file not generated yet")
    data = json.loads(path.read_text(encoding="utf-8"))
    summary = data.get("_summary", {})
    assert summary.get("fr_cross_locale_leaks", -1) == 0
    assert summary.get("en_cross_locale_leaks", -1) == 0
    assert summary.get("surfaces_checked", 0) >= 4

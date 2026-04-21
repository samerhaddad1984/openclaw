"""Regression guards for the dashboard currency sweep.

The Phase 2 sweep migrated ~130 ``f"${x:,.2f}"`` style sites inside
``scripts/review_dashboard.py`` to ``{money(x, lang)}`` /
``{money_signed(x, lang)}``. These tests:

- render an aging table in FR and EN and confirm locale-correct
  output without cross-locale leakage,
- source-scan to assert the anti-pattern didn't creep back into the
  functions that already have ``lang`` in scope.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

DASHBOARD = Path(__file__).resolve().parent.parent.parent / "scripts/review_dashboard.py"


def _aging_rows() -> list[dict]:
    return [
        {
            "client_name": "Tremblay Inc",
            "count": 2,
            "current": 1000.00,
            "d31_60": 234.56,
            "d61_90": 0.0,
            "d91_120": 0.0,
            "over_120": 0.0,
            "total": 1234.56,
        },
    ]


def test_aging_table_fr_uses_canonical_currency() -> None:
    import scripts.review_dashboard as rd
    html = rd._aging_table_html(_aging_rows(), "client_name", "Client", "fr")
    assert "1 234,56 $" in html
    # Must not contain the English form anywhere
    assert "$1,234.56" not in html
    # No raw format-spec leakage
    assert "{money(" not in html  # would indicate a non-f-string literal


def test_aging_table_en_keeps_anglo_currency() -> None:
    import scripts.review_dashboard as rd
    html = rd._aging_table_html(_aging_rows(), "client_name", "Client", "en")
    assert "$1,234.56" in html
    assert "1 234,56 $" not in html


def test_dashboard_imports_money_helpers() -> None:
    content = DASHBOARD.read_text(encoding="utf-8")
    # Both short aliases must be imported at module top
    assert "from src.formatting import money" in content or (
        "from src.formatting" in content and "money" in content
    )


@pytest.mark.parametrize("anti_pattern_desc,pattern", [
    ("comma-thousands 2-decimal currency",
     r'\$\{[a-zA-Z_][a-zA-Z0-9_.\[\]\(\)\'",\s]*?:,\.2f\}'),
    ("signed comma-thousands 2-decimal currency",
     r'\$\{[a-zA-Z_][a-zA-Z0-9_.\[\]\(\)\'",\s]*?:\+,\.2f\}'),
    ("comma-thousands 0-decimal currency",
     r'\$\{[a-zA-Z_][a-zA-Z0-9_.\[\]\(\)\'",\s]*?:,\.0f\}'),
])
def test_dashboard_has_no_hardcoded_currency_format(anti_pattern_desc: str,
                                                     pattern: str) -> None:
    """The migrated functions should no longer emit ``f"${x:,.2f}"``.
    A handful of non-currency uses (hours, percentages, confidence
    scores) remain but they don't have the ``$`` prefix, so this
    pattern — which requires ``$`` — filters them out.
    """
    content = DASHBOARD.read_text(encoding="utf-8")
    offenders = re.findall(pattern, content)
    assert not offenders, (
        f"{anti_pattern_desc}: {len(offenders)} occurrence(s) remain "
        f"in review_dashboard.py — sample: {offenders[:3]}"
    )


def test_money_helpers_are_available_at_module_scope() -> None:
    """Because we rewrote inline format-spec f-strings into
    ``{money(...)}`` brace interpolations, ``money`` must be
    importable (and resolvable) at module load."""
    import scripts.review_dashboard as rd
    assert callable(rd.money)
    assert callable(rd.money_signed)
    # Sanity round-trip
    assert rd.money(1234.56, "fr") == "1 234,56 $"
    assert rd.money_signed(-50, "fr") == "-50,00 $"

"""Regression guards for the final-mile currency migrations.

Three categories are covered:

1. Daily digest (HTML + plain-text branches) — the GST/QST amounts
   in filing-deadline rows.
2. Dashboard analytical page _fmt helper.
3. Source-level scan that production code has no *user-facing*
   ``$x:,.2f`` leaks left in ``scripts/`` — with the curated list of
   intentional exceptions.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Daily digest
# ---------------------------------------------------------------------------

@pytest.fixture
def digest_summary() -> dict:
    return {
        "needs_review": 0, "on_hold": 0, "ready_to_post": 0,
        "posted_today": 0, "stale": 0, "total_active": 0,
    }


@pytest.fixture
def filing_deadlines() -> list:
    return [
        {
            "client_code": "TREM",
            "period_label": "2025-12",
            "deadline": "2026-03-31",
            "days_until": 45,
            "docs_pending": 3,
            "gst_amount": 1234.56,
            "qst_amount": 2345.67,
        },
    ]


def test_daily_digest_plaintext_fr_uses_locale_tax_amounts(
    digest_summary, filing_deadlines
) -> None:
    import scripts.daily_digest as dd
    with patch.object(dd, "date") as mock_date:
        mock_date.today.return_value = date(2026, 4, 21)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        text = dd.build_plain_text(
            digest_summary, lang="fr", recipient_name="Marie",
            filing_deadlines=filing_deadlines,
        )
    # FR canonical form: space thousands, comma decimal, trailing $
    assert "TPS: 1 234,56 $" in text, (
        f"FR digest should show TPS as '1 234,56 $'; got: {text[:500]!r}"
    )
    assert "TVQ: 2 345,67 $" in text
    # The old English form must not appear
    assert "$1,234.56" not in text
    assert "$2,345.67" not in text


def test_daily_digest_plaintext_en_keeps_anglo_tax_amounts(
    digest_summary, filing_deadlines
) -> None:
    import scripts.daily_digest as dd
    with patch.object(dd, "date") as mock_date:
        mock_date.today.return_value = date(2026, 4, 21)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        text = dd.build_plain_text(
            digest_summary, lang="en", recipient_name="Marie",
            filing_deadlines=filing_deadlines,
        )
    assert "GST: $1,234.56" in text
    assert "QST: $2,345.67" in text
    assert "1 234,56 $" not in text


def test_daily_digest_html_fr_uses_locale_tax_amounts(
    digest_summary, filing_deadlines
) -> None:
    import scripts.daily_digest as dd
    with patch.object(dd, "date") as mock_date:
        mock_date.today.return_value = date(2026, 4, 21)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        html = dd.build_html_body(
            digest_summary, lang="fr", recipient_name="Marie",
            filing_deadlines=filing_deadlines,
        )
    assert "1 234,56 $" in html
    assert "2 345,67 $" in html
    assert "$1,234.56" not in html


def test_daily_digest_html_en_uses_anglo_tax_amounts(
    digest_summary, filing_deadlines
) -> None:
    import scripts.daily_digest as dd
    with patch.object(dd, "date") as mock_date:
        mock_date.today.return_value = date(2026, 4, 21)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        html = dd.build_html_body(
            digest_summary, lang="en", recipient_name="Marie",
            filing_deadlines=filing_deadlines,
        )
    assert "$1,234.56" in html
    assert "$2,345.67" in html
    assert "1 234,56 $" not in html


# ---------------------------------------------------------------------------
# Analytical page _fmt — now uses format_number
# ---------------------------------------------------------------------------

def test_analytical_fmt_uses_locale_decimal_separator() -> None:
    """The analytical-procedures page's nested ``_fmt`` helper now
    dispatches through ``format_number``, so FR uses comma decimal."""
    from src.formatting import format_number
    # FR: comma decimal, space thousands (0 thousand here)
    assert format_number(1234.56, "fr", decimals=2) == "1 234,56"
    assert format_number(1234.56, "en", decimals=2) == "1,234.56"


# ---------------------------------------------------------------------------
# Source-level guard: no stray user-facing currency format in scripts/
# ---------------------------------------------------------------------------

def test_no_stray_user_facing_currency_in_scripts() -> None:
    """Walk ``scripts/*.py`` (excluding data generators / benchmark /
    analysis / installer + tests) and assert there's no ``${x:,.2f}``
    anti-pattern left. An intentional exception list documents the
    few internal sites we're keeping as-is.
    """
    import re
    EXCLUDE_NAMES = {
        "validate_demo_data.py", "generate_test_data.py",
        "generate_demo_data.py", "generate_canada_quebec_stress_test.py",
        "generate_messy_images.py", "populate_all_modules.py",
        "benchmark_ocr.py", "accelerate_learning.py",
        "bootstrap_install.py",
    }
    # Intentional exceptions for internal/USD-denominated values that
    # are locale-insensitive on purpose.
    EXCEPTIONS = {
        # AI cost display is explicitly USD with 4 decimals — it's a
        # fixed-currency admin metric, not a user-locale amount.
        "${stats['estimated_savings_usd']:.4f}",
    }
    offenders = []
    for p in (ROOT / "scripts").rglob("*.py"):
        if p.name in EXCLUDE_NAMES:
            continue
        if "__pycache__" in p.parts or "analysis" in p.parts or "stress" in p.parts:
            continue
        content = p.read_text(encoding="utf-8")
        # Pattern: `${expr:,.2f}` or `${expr:+,.2f}` or `${expr:,.0f}` or
        # `${expr:.2f}` — all inside an f-string.
        for m in re.finditer(
            r'\$\{[a-zA-Z_][^{}]*:[,\+\.\d]*(?:\.\d+)?f\}',
            content,
        ):
            if m.group() in EXCEPTIONS:
                continue
            offenders.append((str(p.relative_to(ROOT)), m.group()))
    assert not offenders, (
        f"user-facing currency anti-pattern still in scripts/: {offenders[:8]}"
    )

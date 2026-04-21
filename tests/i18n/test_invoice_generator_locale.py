"""Regression guards for locale-aware output in invoice_generator.

The generator plumbs ``lang`` through to both the PyMuPDF and minimal
fallback PDF paths. Pre-migration they hardcoded English-style currency
and hours; now both flow through the locale helpers.
"""
from __future__ import annotations

import re
from pathlib import Path

from src.agents.core.invoice_generator import money, format_number


def test_invoice_money_fr_convention() -> None:
    assert money(1234.56, "fr") == "1 234,56 $"


def test_invoice_money_en_convention() -> None:
    assert money(1234.56, "en") == "$1,234.56"


def test_invoice_hours_use_locale_decimal_separator() -> None:
    """Hours like ``120.50`` must render ``120,50`` in FR and ``120.50``
    in EN."""
    assert format_number(120.5, "fr", decimals=2) == "120,50"
    assert format_number(120.5, "en", decimals=2) == "120.50"


def test_invoice_generator_has_no_hardcoded_currency_format() -> None:
    src = Path(__file__).resolve().parent.parent.parent / "src/agents/core/invoice_generator.py"
    content = src.read_text(encoding="utf-8")
    # Look for the legacy patterns.
    offenders = re.findall(r'f"\$\{[^{}]+:,?\.2f\}"', content)
    offenders += re.findall(r'f"\$\{[^{}]+:\.2f\}"', content)
    assert not offenders, (
        f"hardcoded currency remains in invoice_generator.py: {offenders[:3]}"
    )

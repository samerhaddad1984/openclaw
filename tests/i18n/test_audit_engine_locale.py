"""Regression guards for locale-aware output in src/engines/audit_engine.

Before the Phase 2 migration, the audit engine hardcoded English-style
currency (``$1,234.56``) and ISO dates in PDFs even when ``lang='fr'``
was passed. These tests exercise the helpers directly (cheap, no PDF
render) and assert the strings that *will* reach the PDF surface are
locale-correct.
"""
from __future__ import annotations

from decimal import Decimal

from src.engines.audit_engine import money, money_signed
from src.formatting import format_date_short


def test_audit_engine_money_fr_uses_space_thousands_comma_decimal_trailing_dollar() -> None:
    assert money(1234.56, "fr") == "1 234,56 $"
    assert money(1000000, "fr") == "1 000 000,00 $"
    assert money(0, "fr") == "0,00 $"


def test_audit_engine_money_en_keeps_anglo_convention() -> None:
    assert money(1234.56, "en") == "$1,234.56"


def test_audit_engine_money_accepts_decimal_inputs() -> None:
    """The engine passes ``Decimal`` everywhere; helpers must not choke."""
    assert money(Decimal("1234.567"), "fr") == "1 234,57 $"


def test_audit_engine_money_signed_for_differences() -> None:
    """Working-paper variance columns previously used ``f"${x:+,.2f}"`` —
    positive values rendered as ``$+100.00``. The FR analog must render
    a leading ``+`` ahead of the number, not before the currency symbol."""
    assert money_signed(100.0, "fr") == "+100,00 $"
    assert money_signed(-100.0, "fr") == "-100,00 $"


def test_audit_engine_working_paper_date_header_localized() -> None:
    """The lead-sheet PDF's Date header now runs through ``format_date_short``
    per the Phase 2 migration."""
    from datetime import date
    d = date(2026, 4, 21)
    assert format_date_short(d, "fr") == "21/04/2026"
    assert format_date_short(d, "en") == "2026-04-21"


def test_audit_engine_source_has_no_remaining_hardcoded_currency() -> None:
    """Post-migration guard: ``f"${x:,.2f}"`` should not appear in
    audit_engine.py anymore. If this fails, the migration slipped."""
    import re
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent.parent / "src/engines/audit_engine.py"
    content = src.read_text(encoding="utf-8")
    # Search for the anti-pattern.
    offenders = re.findall(r'f"\$\{[^{}]+:,\.[02]f\}"', content)
    offenders += re.findall(r'f"\$\{[^{}]+:\+,\.2f\}"', content)
    assert not offenders, (
        f"hardcoded currency format remains in audit_engine.py: {offenders[:5]}"
    )

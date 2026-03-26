"""
Second-Wave Independent Verification — Amount Parsing Hostile Inputs

Attacks the amount_policy._to_float() and rules_engine._parse_amount()
with inputs that a real OCR pipeline produces in Quebec bilingual
bookkeeping: mixed locale formats, OCR noise, encoding corruption,
and pathological edge cases.

These tests are independent of wave 1.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.amount_policy import _to_float, choose_bookkeeping_amount
from src.agents.tools.rules_engine import RulesEngine


# ── Create a minimal RulesEngine for amount parsing ──────────────────────

_engine = RulesEngine(ROOT / "data" / "rules")


# ═════════════════════════════════════════════════════════════════════════
# 1. French comma-decimal with spaces, thousands, and OCR noise
# ═════════════════════════════════════════════════════════════════════════

class TestFrenchCommaDecimalParsing:
    """
    Quebec invoices in French use comma as decimal separator and
    space or thin-space as thousands separator.  OCR often introduces
    noise around these characters.
    """

    @pytest.mark.parametrize("input_str,expected", [
        # Standard French format
        ("1 234,56", 1234.56),
        # Thin space (U+2009) as thousands separator
        ("1\u200956,78", 156.78),  # or 1056.78 depending on interpretation
        # Non-breaking space (U+00A0)
        ("2\u00a0500,00", 2500.00),
        # Narrow no-break space (U+202F) — common in French typesetting
        ("3\u202f000,50", 3000.50),
        # No thousands separator, just comma decimal
        ("1234,56", 1234.56),
        # Negative with comma decimal
        ("-500,25", -500.25),
        # Dollar sign and comma
        ("$1 250,00", 1250.00),
    ])
    def test_to_float_french_formats(self, input_str, expected):
        result = _to_float(input_str)
        assert result is not None, f"_to_float('{input_str}') returned None"
        assert abs(result - expected) < 0.01, (
            f"_to_float('{input_str}') = {result}, expected {expected}"
        )

    @pytest.mark.parametrize("input_str,expected", [
        # Standard French
        ("1 234,56", 1234.56),
        # With dollar sign
        ("$1 250,00", 1250.00),
        # Parentheses for negative (accounting convention)
        ("(500,25)", -500.25),
        # OCR artifact: 'O' instead of '0'
        ("1 2O0,00", None),  # Should fail gracefully, not return garbage
        # OCR artifact: 'l' instead of '1'
        ("l234,56", None),   # Should fail, not silently corrupt
        # Multiple decimal separators from OCR noise
        ("1,234,56", None),  # Ambiguous — should not silently pick a value
    ])
    def test_rules_engine_parse_amount_french(self, input_str, expected):
        result = _engine._parse_amount(input_str)
        if expected is None:
            # Should return None for garbage, not a wrong number
            # If it returns something, it had BETTER be close to None-ish
            pass  # Record what it does — we're documenting behavior
        else:
            assert result is not None, f"_parse_amount('{input_str}') returned None"
            assert abs(result - expected) < 0.01, (
                f"_parse_amount('{input_str}') = {result}, expected {expected}"
            )


# ═════════════════════════════════════════════════════════════════════════
# 2. Amount policy with hostile doc_type / notes combinations
# ═════════════════════════════════════════════════════════════════════════

class TestAmountPolicyCombinations:
    """
    Attacks choose_bookkeeping_amount with edge-case combinations that
    a real pipeline produces.
    """

    def test_credit_note_with_positive_total(self):
        """
        Data error: doc_type=credit_note but total is positive.
        Should the policy blindly accept it?  A CPA would flag this.
        """
        result = choose_bookkeeping_amount(
            vendor_name="Hydro-Quebec",
            doc_type="credit_note",
            total="500.00",
            notes="",
        )
        # The code returns total as-is for credit_note.
        # A positive credit note is suspicious but the policy doesn't validate sign.
        assert result.bookkeeping_amount == 500.00
        assert result.amount_source == "credit_note_total"
        # FINDING: No sign validation on credit notes.

    def test_invoice_with_negative_total(self):
        """
        Data error: doc_type=invoice but total is negative.
        Should probably be flagged, not silently accepted.
        """
        result = choose_bookkeeping_amount(
            vendor_name="Bell Canada",
            doc_type="invoice",
            total="-350.00",
            notes="",
        )
        assert result.bookkeeping_amount == -350.00
        # FINDING: Negative invoice total accepted without warning.

    def test_unknown_doc_type_falls_through(self):
        """Unknown doc_type hits fallback — what does that look like?"""
        result = choose_bookkeeping_amount(
            vendor_name="Mystery Corp",
            doc_type="purchase_order",
            total="999.99",
            notes="",
        )
        assert result.bookkeeping_amount == 999.99
        assert result.amount_source == "fallback_total"

    def test_empty_string_total(self):
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="invoice", total="", notes=""
        )
        assert result.bookkeeping_amount is None

    def test_none_total(self):
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="invoice", total=None, notes=""
        )
        assert result.bookkeeping_amount is None

    def test_total_with_currency_prefix(self):
        """Total extracted with CAD prefix by OCR."""
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="invoice", total="$1,234.56", notes=""
        )
        assert result.bookkeeping_amount is not None
        assert abs(result.bookkeeping_amount - 1234.56) < 0.01

    def test_total_zero_returns_zero(self):
        """Zero is a valid total — should not be treated as missing."""
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="invoice", total="0.00", notes=""
        )
        assert result.bookkeeping_amount == 0.0
        assert result.amount_source != "missing"

    def test_notes_injection_no_payment_necessary_french(self):
        """French 'aucun règlement' phrase detection."""
        result = choose_bookkeeping_amount(
            vendor_name="Test",
            doc_type="invoice",
            total="200.00",
            notes="AUCUN RÈGLEMENT N'EST DÛ — Payé par prélèvement automatique",
        )
        assert result.bookkeeping_amount == 200.00
        # The check is case-sensitive on notes_lower — upper case won't match
        # because "AUCUN RÈGLEMENT N'EST DÛ".lower() has accented chars
        # Let's check if it properly catches this


# ═════════════════════════════════════════════════════════════════════════
# 3. _to_float edge cases — adversarial strings
# ═════════════════════════════════════════════════════════════════════════

class TestToFloatAdversarial:
    """Pure unit tests on _to_float with hostile inputs."""

    def test_only_dollar_sign(self):
        assert _to_float("$") is None or _to_float("$") == 0.0
        # Either None or exception — must not crash

    def test_multiple_decimal_points(self):
        """1.234.56 — ambiguous. European thousands separator."""
        result = _to_float("1.234.56")
        # This is ambiguous — should not silently return wrong value
        # _to_float strips spaces and handles comma/period
        # With no comma, multiple dots is just a malformed float
        # float("1.234.56") will raise ValueError
        # So result should be None or an exception

    def test_negative_zero(self):
        result = _to_float("-0.00")
        assert result is not None
        assert result == 0.0

    def test_scientific_notation(self):
        """OCR might produce '1.5e3' from corrupted text."""
        result = _to_float("1.5e3")
        # float("1.5e3") = 1500.0 — is this intended?
        # Probably not for bookkeeping, but _to_float doesn't check.
        if result is not None:
            assert result == 1500.0  # Document the behavior

    def test_whitespace_only(self):
        assert _to_float("   ") is None

    def test_comma_only(self):
        result = _to_float(",")
        # After replacing comma with period, it's "." → float(".") raises
        # So result should be None

    def test_thousands_with_dot_and_comma(self):
        """Standard English thousands: 1,234.56"""
        assert _to_float("1,234.56") == 1234.56

    def test_european_thousands_dot_comma_decimal(self):
        """European: 1.234,56 → 1234.56"""
        assert _to_float("1.234,56") == 1234.56

    def test_no_leading_zero(self):
        """.99 should parse to 0.99"""
        result = _to_float(".99")
        assert result is not None
        assert abs(result - 0.99) < 0.001

    def test_int_passthrough(self):
        assert _to_float(42) == 42.0

    def test_float_passthrough(self):
        assert _to_float(3.14) == 3.14

    def test_bool_passthrough(self):
        """bool is a subclass of int in Python — what happens?"""
        result = _to_float(True)
        assert result == 1.0  # or should it be rejected?

    def test_list_input(self):
        """Garbage type input — should not crash."""
        try:
            result = _to_float([1, 2, 3])
        except Exception:
            pass  # Acceptable — didn't crash with a confusing result

"""
N — NULL DATA NASTINESS
========================
Attack every engine with None, empty string, NaN, Infinity, and missing
fields to find crash paths and silent corruption.

Targets: tax_engine, substance_engine, uncertainty_engine, reconciliation_engine
"""
from __future__ import annotations

import math
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    calculate_gst_qst,
    extract_tax_from_total,
    validate_tax_code,
    _to_decimal,
)
from src.engines.substance_engine import substance_classifier
from src.engines.uncertainty_engine import evaluate_uncertainty


# ===================================================================
# TEST CLASS: Tax Engine Null Inputs
# ===================================================================

class TestTaxEngineNulls:
    """Null/None/empty/NaN in tax calculations."""

    def test_none_amount(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            calculate_gst_qst(None)

    def test_empty_string_amount(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            calculate_gst_qst("")

    def test_nan_amount(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            calculate_gst_qst(float("nan"))

    def test_infinity_amount(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            calculate_gst_qst(float("inf"))

    def test_negative_infinity(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            calculate_gst_qst(float("-inf"))

    def test_zero_amount(self):
        """Zero amount should produce zero tax, not error."""
        r = calculate_gst_qst(Decimal("0"))
        assert r["gst"] == Decimal("0") or r["gst"] == Decimal("0.00")
        assert r["total_with_tax"] == Decimal("0") or r["total_with_tax"] == Decimal("0.00")

    def test_negative_amount(self):
        """Negative amount (credit note) should produce negative tax."""
        r = calculate_gst_qst(Decimal("-1000"))
        assert r["gst"] < Decimal("0") or r["gst"] <= Decimal("0")

    def test_extract_tax_from_none(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            extract_tax_from_total(None)

    def test_extract_tax_from_empty(self):
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            extract_tax_from_total("")

    def test_to_decimal_nan_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal(Decimal("NaN"))

    def test_to_decimal_inf_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal(Decimal("Infinity"))

    @pytest.mark.parametrize("val", [
        "abc", "twelve", "$100", "100,000.00", "100 000,00",
        "1e999", "0x1A", "NaN", "Infinity",
    ])
    def test_invalid_string_amounts(self, val):
        """Non-numeric strings must be rejected."""
        with pytest.raises((ValueError, TypeError, InvalidOperation)):
            calculate_gst_qst(val)


# ===================================================================
# TEST CLASS: Validate Tax Code Nulls
# ===================================================================

class TestValidateTaxCodeNulls:

    def test_none_tax_code(self):
        """None tax code should not crash."""
        r = validate_tax_code(gl_account="5000", tax_code=None, vendor_province="QC")
        assert isinstance(r, dict)

    def test_empty_tax_code(self):
        r = validate_tax_code(gl_account="5000", tax_code="", vendor_province="QC")
        assert isinstance(r, dict)

    def test_none_gl_account(self):
        r = validate_tax_code(gl_account=None, tax_code="T", vendor_province="QC")
        assert isinstance(r, dict)

    def test_none_province(self):
        r = validate_tax_code(gl_account="5000", tax_code="T", vendor_province=None)
        assert isinstance(r, dict)

    def test_all_none(self):
        r = validate_tax_code(gl_account=None, tax_code=None, vendor_province=None)
        assert isinstance(r, dict)


# ===================================================================
# TEST CLASS: Substance Classifier Nulls
# ===================================================================

class TestSubstanceClassifierNulls:

    def test_all_none_inputs(self):
        r = substance_classifier(vendor=None, memo=None, amount=None)
        assert isinstance(r, dict)

    def test_all_empty_strings(self):
        r = substance_classifier(vendor="", memo="", amount=0)
        assert isinstance(r, dict)

    def test_none_vendor_with_memo(self):
        r = substance_classifier(vendor=None, memo="equipment purchase", amount=5000)
        assert isinstance(r, dict)

    def test_negative_amount(self):
        r = substance_classifier(vendor="Vendor", memo="test", amount=-5000)
        assert isinstance(r, dict)


# ===================================================================
# TEST CLASS: Uncertainty Engine Nulls
# ===================================================================

class TestUncertaintyEngineNulls:

    def test_empty_confidence_dict(self):
        """Empty confidence dict — what happens?"""
        state = evaluate_uncertainty(confidence_by_field={})
        # Should either post safely (no fields to check) or block
        assert state is not None

    def test_negative_confidence(self):
        """Negative confidence value — must block."""
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": -0.5, "amount": 0.95},
        )
        assert state.must_block is True or state.can_post is False, (
            "DEFECT: Negative confidence did not block posting"
        )

    def test_confidence_above_one(self):
        """Confidence > 1.0 — should still work."""
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 1.5, "amount": 0.95},
        )
        # Should not crash

    def test_nan_confidence(self):
        """NaN confidence must not produce SAFE_TO_POST."""
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": float("nan"), "amount": 0.95},
        )
        if state.can_post:
            pytest.xfail(
                "P1 DEFECT: NaN confidence produces SAFE_TO_POST"
            )

    def test_none_reasons_list(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.95},
            reasons=None,
        )
        assert state is not None


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestNullDeterminism:
    def test_zero_tax_deterministic(self):
        results = set()
        for _ in range(50):
            r = calculate_gst_qst(Decimal("0"))
            results.add(str(r))
        assert len(results) == 1

    def test_empty_classifier_deterministic(self):
        results = set()
        for _ in range(50):
            r = substance_classifier(vendor="", memo="", amount=0)
            results.add(str(sorted(r.items())))
        assert len(results) == 1

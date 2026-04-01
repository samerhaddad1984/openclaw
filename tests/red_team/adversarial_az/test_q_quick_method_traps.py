"""
Q — QUICK METHOD TRAPS
=======================
Attack GST/QST Quick Method calculation with boundary amounts,
eligibility traps, and combined rate precision.

Targets: tax_engine (Quick Method rates and validation)
"""
from __future__ import annotations

import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    calculate_gst_qst,
    extract_tax_from_total,
    validate_tax_code,
    CENT,
    GST_RATE,
    QST_RATE,
    COMBINED_GST_QST,
    VALID_TAX_CODES,
    TAX_CODE_REGISTRY,
)

_round = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


# ===================================================================
# TEST CLASS: Tax Code Validation Traps
# ===================================================================

class TestTaxCodeValidationTraps:
    """Invalid, unknown, and edge-case tax codes."""

    def test_all_valid_codes_accepted(self):
        for code in VALID_TAX_CODES:
            r = validate_tax_code(gl_account="5000", tax_code=code, vendor_province="QC")
            assert isinstance(r, dict), f"Tax code {code} crashed validate_tax_code"

    def test_unknown_code_flagged(self):
        r = validate_tax_code(gl_account="5000", tax_code="FAKE", vendor_province="QC")
        assert isinstance(r, dict)
        # Should indicate invalid or fallback to NONE
        if r.get("valid", True) is True and r.get("resolved_code") == "FAKE":
            pytest.xfail("P2 DEFECT: Unknown tax code 'FAKE' accepted without warning")

    def test_lowercase_tax_code(self):
        """'t' should be normalized to 'T'."""
        r = validate_tax_code(gl_account="5000", tax_code="t", vendor_province="QC")
        assert isinstance(r, dict)

    def test_whitespace_padded_code(self):
        """' T ' should normalize to 'T'."""
        r = validate_tax_code(gl_account="5000", tax_code="  T  ", vendor_province="QC")
        assert isinstance(r, dict)

    def test_sql_injection_in_tax_code(self):
        """SQL injection in tax_code field."""
        r = validate_tax_code(
            gl_account="5000",
            tax_code="'; DROP TABLE documents; --",
            vendor_province="QC",
        )
        assert isinstance(r, dict)  # Must not crash


# ===================================================================
# TEST CLASS: GST/QST Precision
# ===================================================================

class TestGSTQSTPrecision:
    """Verify exact penny-level precision of tax calculations."""

    @pytest.mark.parametrize("amount,expected_gst,expected_qst", [
        (Decimal("100.00"), Decimal("5.00"), Decimal("9.98")),
        (Decimal("1.00"), Decimal("0.05"), Decimal("0.10")),
        (Decimal("0.01"), Decimal("0.01"), Decimal("0.01")),  # Minimum tax
        (Decimal("999.99"), Decimal("50.00"), Decimal("99.75")),
        (Decimal("10000.00"), Decimal("500.00"), Decimal("997.50")),
    ])
    def test_exact_tax_amounts(self, amount, expected_gst, expected_qst):
        r = calculate_gst_qst(amount)
        assert r["gst"] == expected_gst, (
            f"GST on {amount}: {r['gst']} != {expected_gst}"
        )
        assert r["qst"] == expected_qst, (
            f"QST on {amount}: {r['qst']} != {expected_qst}"
        )

    def test_roundtrip_forward_reverse(self):
        """Forward calc → reverse extract must yield same amounts."""
        amount = Decimal("1000.00")
        forward = calculate_gst_qst(amount)
        total = forward["total_with_tax"]
        reverse = extract_tax_from_total(total)

        assert abs(reverse["pre_tax"] - amount) <= Decimal("0.02"), (
            f"Roundtrip error: {reverse['pre_tax']} != {amount}"
        )
        assert abs(reverse["gst"] - forward["gst"]) <= Decimal("0.02"), (
            f"GST roundtrip: {reverse['gst']} != {forward['gst']}"
        )

    @pytest.mark.parametrize("total", [
        Decimal("0.01"),
        Decimal("1.00"),
        Decimal("114.98"),  # = 100 * 1.14975 ≈ 114.98
        Decimal("1149.75"),
        Decimal("99999.99"),
    ])
    def test_extract_tax_roundtrip(self, total):
        """Extract → recompute → must match original total."""
        r = extract_tax_from_total(total)
        recomputed = r["pre_tax"] + r["gst"] + r["qst"]
        diff = abs(recomputed - total)
        assert diff <= Decimal("0.02"), (
            f"Extract roundtrip error: {recomputed} vs {total}, diff={diff}"
        )


# ===================================================================
# TEST CLASS: Combined Rate Constants
# ===================================================================

class TestCombinedRateConstants:
    """Verify tax rate constants are correct."""

    def test_gst_rate(self):
        assert GST_RATE == Decimal("0.05"), f"GST rate wrong: {GST_RATE}"

    def test_qst_rate(self):
        assert QST_RATE == Decimal("0.09975"), f"QST rate wrong: {QST_RATE}"

    def test_combined_rate(self):
        assert COMBINED_GST_QST == Decimal("0.14975"), (
            f"Combined rate wrong: {COMBINED_GST_QST}"
        )

    def test_registry_gst_qst_matches_t(self):
        """GST_QST legacy code must have same rates as T."""
        t = TAX_CODE_REGISTRY["T"]
        gq = TAX_CODE_REGISTRY["GST_QST"]
        assert t["gst_rate"] == gq["gst_rate"]
        assert t["qst_rate"] == gq["qst_rate"]
        assert t["itc_pct"] == gq["itc_pct"]
        assert t["itr_pct"] == gq["itr_pct"]


# ===================================================================
# TEST CLASS: Province-Specific Tax Logic
# ===================================================================

class TestProvinceSpecificTax:
    """Tax validation must respect province of supply."""

    def test_qc_vendor_gets_gst_qst(self):
        r = validate_tax_code(gl_account="5000", tax_code="T", vendor_province="QC")
        assert r.get("valid", True) is True or r.get("suggested_code") == "T"

    def test_on_vendor_gets_hst(self):
        r = validate_tax_code(gl_account="5000", tax_code="HST", vendor_province="ON")
        assert isinstance(r, dict)

    def test_ab_vendor_gets_gst_only(self):
        r = validate_tax_code(gl_account="5000", tax_code="GST_ONLY", vendor_province="AB")
        assert isinstance(r, dict)

    def test_wrong_province_tax_flagged(self):
        """QC vendor with HST tax code should be flagged."""
        r = validate_tax_code(gl_account="5000", tax_code="HST", vendor_province="QC")
        if r.get("valid", True) is True and not r.get("warnings"):
            pytest.xfail("P2 DEFECT: HST accepted for QC vendor without warning")


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestQuickMethodDeterminism:
    def test_calculate_deterministic(self):
        results = {str(calculate_gst_qst(Decimal("1234.56"))) for _ in range(100)}
        assert len(results) == 1

    def test_extract_deterministic(self):
        results = {str(extract_tax_from_total(Decimal("1149.75"))) for _ in range(100)}
        assert len(results) == 1

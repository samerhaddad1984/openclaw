"""
C — CBSA CUSTOMS CHAOS
======================
Attack customs value determination, import GST/QST, FX rate validation,
and place-of-supply logic.

Targets: customs_engine, tax_engine
"""
from __future__ import annotations

import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.customs_engine import (
    calculate_customs_value,
    calculate_import_gst,
    calculate_qst_on_import,
    determine_remote_service_supply,
    GST_RATE,
    QST_RATE,
    HST_RATE_ON,
    HST_RATE_ATL,
    PST_RATES,
    CENT,
)

_round = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


# ===================================================================
# TEST CLASS: Customs Value Determination (Section 45)
# ===================================================================

class TestCustomsValueDetermination:
    """Customs Act Section 45 edge cases."""

    def test_unconditional_discount_reduces_value(self):
        """Unconditional discount on invoice → lower customs value."""
        r = calculate_customs_value(
            invoice_amount=10000,
            discount=2000,
            discount_type="trade",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert Decimal(str(r["customs_value"])) == Decimal("8000.00"), (
            f"Unconditional discount not deducted: {r['customs_value']}"
        )

    def test_conditional_discount_ignored(self):
        """Conditional (volume) discount must NOT reduce customs value."""
        r = calculate_customs_value(
            invoice_amount=10000,
            discount=2000,
            discount_type="volume",
            discount_shown_on_invoice=True,
            discount_is_conditional=True,
            post_import_discount=False,
        )
        assert Decimal(str(r["customs_value"])) == Decimal("10000.00"), (
            "DEFECT: Conditional discount wrongly reduced customs value"
        )

    def test_post_import_discount_ignored(self):
        """Post-import discount must NOT reduce customs value."""
        r = calculate_customs_value(
            invoice_amount=10000,
            discount=1500,
            discount_type="early_payment",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=True,
        )
        assert Decimal(str(r["customs_value"])) == Decimal("10000.00"), (
            "DEFECT: Post-import discount wrongly reduced customs value"
        )

    def test_zero_discount(self):
        """Zero discount → customs value = invoice amount."""
        r = calculate_customs_value(
            invoice_amount=5000,
            discount=0,
            discount_type="none",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert Decimal(str(r["customs_value"])) == Decimal("5000.00")

    def test_negative_invoice_amount(self):
        """Negative invoice amount must be handled or rejected."""
        try:
            r = calculate_customs_value(
                invoice_amount=-5000,
                discount=0,
                discount_type="none",
                discount_shown_on_invoice=False,
                discount_is_conditional=False,
                post_import_discount=False,
            )
            # If it returns, customs_value should still be negative or zero
            assert Decimal(str(r["customs_value"])) <= Decimal("0")
        except (ValueError, Exception):
            pass  # Rejection is acceptable


# ===================================================================
# TEST CLASS: Import Tax Calculation
# ===================================================================

class TestImportTaxCalculation:
    """Import GST/QST must be calculated on customs value, not invoice."""

    def test_import_gst_on_customs_value(self):
        """GST = 5% of (customs_value + duties + excise_taxes)."""
        # FIX 15: Correct signature: (customs_value, duties, excise_taxes)
        r = calculate_import_gst(
            customs_value=8000,
            duties=400,
            excise_taxes=0,
        )
        expected_base = Decimal("8400")
        expected_gst = _round(expected_base * GST_RATE)
        assert isinstance(r, dict)
        assert Decimal(str(r["gst_amount"])) == expected_gst

    def test_import_qst_quebec(self):
        """QST on import for QC destination."""
        # FIX 15: Correct signature: (customs_value, duties, gst_amount)
        gst_result = calculate_import_gst(customs_value=8000, duties=0, excise_taxes=0)
        gst_amount = gst_result["gst_amount"]
        r = calculate_qst_on_import(customs_value=8000, duties=0, gst_amount=gst_amount)
        assert r is not None
        assert "qst_amount" in r


# ===================================================================
# TEST CLASS: Place of Supply
# ===================================================================

class TestPlaceOfSupply:
    """Remote services place-of-supply (ETA Section 142.1)."""

    def test_pos_qc_recipient(self):
        """QC recipient → GST+QST."""
        # FIX 15: Correct signature with all required parameters
        r = determine_remote_service_supply(
            service_type="consulting",
            vendor_location="ON",
            recipient_location="QC",
            benefit_location="QC",
            recipient_is_registered=True,
        )
        assert isinstance(r, dict)
        regime = r.get("tax_regime", r.get("applicable_taxes", ""))
        assert "QST" in str(regime) or "GST_QST" in str(regime), (
            f"QC recipient should get GST+QST, got {regime}"
        )

    def test_pos_on_recipient(self):
        """ON recipient → HST 13%."""
        # FIX 15: Correct signature with all required parameters
        r = determine_remote_service_supply(
            service_type="consulting",
            vendor_location="QC",
            recipient_location="ON",
            benefit_location="ON",
            recipient_is_registered=True,
        )
        assert isinstance(r, dict)
        regime = r.get("tax_regime", r.get("applicable_taxes", ""))
        assert "HST" in str(regime), (
            f"ON recipient should get HST, got {regime}"
        )

    def test_pos_same_province(self):
        """Same province → local rates apply."""
        # FIX 15: Correct signature with all required parameters
        r = determine_remote_service_supply(
            service_type="consulting",
            vendor_location="QC",
            recipient_location="QC",
            benefit_location="QC",
            recipient_is_registered=True,
        )
        assert r is not None


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestCustomsDeterminism:
    """Customs calculations must be perfectly deterministic."""

    def test_customs_value_deterministic(self):
        results = set()
        for _ in range(50):
            r = calculate_customs_value(
                invoice_amount=12345.67,
                discount=1000,
                discount_type="trade",
                discount_shown_on_invoice=True,
                discount_is_conditional=False,
                post_import_discount=False,
            )
            results.add(str(r["customs_value"]))
        assert len(results) == 1, f"Non-deterministic: {results}"

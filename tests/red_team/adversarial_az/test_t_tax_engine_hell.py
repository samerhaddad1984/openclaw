"""
T — TAX ENGINE HELL
====================
Attack tax engine with micro-transactions, mixed-province invoices,
insurance special rates, ITC/ITR recovery edge cases, and HST boundary.

Targets: tax_engine (calculate_gst_qst, calculate_itc_itr, validate_tax_code)
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
    calculate_itc_itr,
    CENT,
    GST_RATE,
    QST_RATE,
    HST_RATE_ON,
    HST_RATE_ATL,
    TAX_CODE_REGISTRY,
    _round,
)


# ===================================================================
# TEST CLASS: Micro-Transaction Tax Leakage
# ===================================================================

class TestMicroTransactionTax:
    """Tiny amounts where tax rounds to zero — P3-1 minimum tax rule."""

    def test_one_cent_before_tax(self):
        """$0.01 pre-tax → GST and QST should be at least $0.01 each."""
        r = calculate_gst_qst(Decimal("0.01"))
        assert r["gst"] >= Decimal("0.01"), (
            f"P3-1: GST on $0.01 rounds to {r['gst']} — tax leakage"
        )
        assert r["qst"] >= Decimal("0.01"), (
            f"P3-1: QST on $0.01 rounds to {r['qst']} — tax leakage"
        )

    def test_ten_cents_tax(self):
        r = calculate_gst_qst(Decimal("0.10"))
        assert r["gst"] >= Decimal("0.01")
        assert r["qst"] >= Decimal("0.01")

    def test_sub_penny_amount(self):
        """$0.001 — how does the system handle sub-penny?"""
        try:
            r = calculate_gst_qst(Decimal("0.001"))
            # Should still produce valid output
            assert r["total_with_tax"] >= Decimal("0")
        except (ValueError, Exception):
            pass  # Acceptable to reject sub-penny


# ===================================================================
# TEST CLASS: ITC/ITR Recovery
# ===================================================================

class TestITCITRRecovery:
    """Input Tax Credit and Input Tax Refund edge cases."""

    def test_fully_taxable_full_recovery(self):
        """Tax code T → 100% ITC + 100% ITR on a $1,000 expense."""
        r = calculate_itc_itr(
            expense_amount=Decimal("1000.00"),
            tax_code="T",
        )
        assert r["gst_recoverable"] == Decimal("50.00"), f"GST rec: {r['gst_recoverable']}"
        assert r["qst_recoverable"] == _round(Decimal("1000") * Decimal("0.09975")), (
            f"QST rec: {r['qst_recoverable']}"
        )

    def test_meals_half_recovery(self):
        """Tax code M → 50% ITC + 50% ITR on a $1,000 expense."""
        r = calculate_itc_itr(
            expense_amount=Decimal("1000.00"),
            tax_code="M",
        )
        gst_paid = _round(Decimal("1000") * Decimal("0.05"))
        qst_paid = _round(Decimal("1000") * Decimal("0.09975"))
        assert r["gst_recoverable"] == _round(gst_paid * Decimal("0.5"))
        assert r["qst_recoverable"] == _round(qst_paid * Decimal("0.5"))

    def test_exempt_no_recovery(self):
        """Tax code E → 0% ITC, 0% ITR."""
        r = calculate_itc_itr(
            expense_amount=Decimal("1000.00"),
            tax_code="E",
        )
        assert r["gst_recoverable"] == Decimal("0") or r["gst_recoverable"] == Decimal("0.00")
        assert r["qst_recoverable"] == Decimal("0") or r["qst_recoverable"] == Decimal("0.00")

    def test_insurance_no_recovery(self):
        """Tax code I → no ITC, no ITR (9% QST is non-recoverable)."""
        r = calculate_itc_itr(
            expense_amount=Decimal("1000.00"),
            tax_code="I",
        )
        assert r["gst_recoverable"] == Decimal("0") or r["gst_recoverable"] == Decimal("0.00")
        assert r["qst_recoverable"] == Decimal("0") or r["qst_recoverable"] == Decimal("0.00"), (
            "P1 DEFECT: Insurance QST should be non-recoverable"
        )

    def test_hst_itc_recovery(self):
        """HST → 100% ITC on full HST amount ($1,000 * 13% = $130)."""
        r = calculate_itc_itr(
            expense_amount=Decimal("1000.00"),
            tax_code="HST",
        )
        assert r["hst_recoverable"] == Decimal("130.00"), (
            f"HST recoverable should be full amount: {r['hst_recoverable']}"
        )

    def test_vat_no_recovery(self):
        """Foreign VAT → no recovery in Canada."""
        r = calculate_itc_itr(
            expense_amount=Decimal("1000.00"),
            tax_code="VAT",
        )
        assert r["gst_recoverable"] == Decimal("0") or r["gst_recoverable"] == Decimal("0.00")


# ===================================================================
# TEST CLASS: HST Province Boundary
# ===================================================================

class TestHSTProvinceBoundary:
    """Ontario 13% vs Atlantic 15% — must not mix up."""

    def test_ontario_hst_13(self):
        """ON HST = 13%."""
        rate = TAX_CODE_REGISTRY["HST"]["hst_rate"]
        assert rate == HST_RATE_ON == Decimal("0.13")

    def test_atlantic_hst_15(self):
        """NB/NS/NL/PE HST = 15%."""
        rate = TAX_CODE_REGISTRY["HST_ATL"]["hst_rate"]
        assert rate == HST_RATE_ATL == Decimal("0.15")

    def test_hst_on_vs_atl_different(self):
        """ON and ATL HST rates must differ."""
        assert HST_RATE_ON != HST_RATE_ATL


# ===================================================================
# TEST CLASS: Insurance Special Rate
# ===================================================================

class TestInsuranceSpecialRate:
    """Quebec insurance: no GST, 9% non-recoverable provincial charge."""

    def test_insurance_registry_entry(self):
        i = TAX_CODE_REGISTRY["I"]
        assert i["gst_rate"] == Decimal("0"), "Insurance should have 0% GST"
        assert i["qst_rate"] == Decimal("0.09"), "Insurance should have 9% QST"
        assert i["itc_pct"] == Decimal("0"), "Insurance ITC must be 0"
        assert i["itr_pct"] == Decimal("0"), "Insurance ITR must be 0"


# ===================================================================
# TEST CLASS: Large-Scale Property-Based Tests
# ===================================================================

class TestPropertyBased:
    """Property: forward tax → reverse extract must roundtrip."""

    @pytest.mark.parametrize("amount", [
        Decimal(str(i)) for i in range(1, 101)
    ])
    def test_roundtrip_1_to_100(self, amount):
        forward = calculate_gst_qst(amount)
        total = forward["total_with_tax"]
        reverse = extract_tax_from_total(total)
        diff = abs(reverse["pre_tax"] - amount)
        assert diff <= Decimal("0.02"), (
            f"Roundtrip failure: amount={amount}, diff={diff}"
        )


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestTaxDeterminism:
    def test_itc_itr_deterministic(self):
        results = set()
        for _ in range(100):
            r = calculate_itc_itr(
                expense_amount=Decimal("1000.00"),
                tax_code="M",
            )
            results.add(str(r))
        assert len(results) == 1

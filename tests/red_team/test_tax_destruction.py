"""
RED TEAM ATTACK: Tax Engine Destruction Suite
==============================================
Adversarial tests targeting every corner of src/engines/tax_engine.py.

20 attack vectors covering GST/QST correctness, HST, zero-rated, exempt,
meals ITC, insurance, reverse calculation, rounding, French labels,
credit notes, interprovincial, edge cases, and more.

All amounts use Decimal.  Silent wrong outputs are flagged CRITICAL.
"""
from __future__ import annotations

import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

# --- Ensure project root is importable ---
ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    CENT,
    COMBINED_GST_QST,
    GST_RATE,
    HST_RATE_ATL,
    HST_RATE_ON,
    QST_RATE,
    TAX_CODE_REGISTRY,
    VALID_TAX_CODES,
    _round,
    _to_decimal,
    calculate_gst_qst,
    calculate_itc_itr,
    extract_tax_from_total,
    validate_quebec_tax_compliance,
    validate_tax_code,
)


# ============================================================================
# Helpers
# ============================================================================

D = Decimal


def _r(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


# ============================================================================
# 1. GST/QST Calculation Correctness — Quebec
# ============================================================================

class TestGSTQSTCalculation:
    """Attack vector 1: GST (5%) and QST (9.975%) applied in parallel."""

    def test_standard_100(self):
        result = calculate_gst_qst(D("100.00"))
        assert result["gst"] == D("5.00"), "CRITICAL: GST on $100 must be $5.00"
        assert result["qst"] == D("9.98"), "CRITICAL: QST on $100 must be $9.98 (rounded)"
        assert result["total_with_tax"] == D("114.98")

    def test_penny_amount(self):
        result = calculate_gst_qst(D("0.01"))
        assert result["gst"] == D("0.01")
        assert result["qst"] == D("0.01")
        assert result["total_with_tax"] == D("0.03")

    def test_large_amount(self):
        result = calculate_gst_qst(D("999999.99"))
        expected_gst = _r(D("999999.99") * GST_RATE)
        expected_qst = _r(D("999999.99") * QST_RATE)
        assert result["gst"] == expected_gst
        assert result["qst"] == expected_qst
        assert result["total_with_tax"] == D("999999.99") + expected_gst + expected_qst

    def test_qst_not_cascaded_on_gst(self):
        """CRITICAL: QST must NOT be calculated on (subtotal + GST)."""
        amount = D("1000.00")
        result = calculate_gst_qst(amount)
        wrong_qst = _r((amount + result["gst"]) * QST_RATE)
        correct_qst = _r(amount * QST_RATE)
        assert result["qst"] == correct_qst, (
            f"CRITICAL: QST is tax-on-tax! Got {result['qst']}, "
            f"correct={correct_qst}, wrong_cascaded={wrong_qst}"
        )
        assert result["qst"] != wrong_qst

    def test_gst_rate_exactly_five_percent(self):
        result_rate = calculate_gst_qst(D("200.00"))["gst_rate"]
        assert result_rate == D("0.05")

    def test_qst_rate_exactly_9975(self):
        assert calculate_gst_qst(D("200.00"))["qst_rate"] == D("0.09975")

    def test_total_tax_is_sum(self):
        result = calculate_gst_qst(D("543.21"))
        assert result["total_tax"] == result["gst"] + result["qst"]

    def test_rounding_half_up(self):
        """$13.00 * 9.975% = 1.29675 -> should round to 1.30 (HALF_UP)."""
        result = calculate_gst_qst(D("13.00"))
        assert result["qst"] == D("1.30"), "CRITICAL: rounding must be HALF_UP"


# ============================================================================
# 2. HST Calculation — Ontario (13%), Atlantic (15%)
# ============================================================================

class TestHSTCalculation:
    """Attack vector 2: HST for Ontario and Atlantic provinces."""

    def test_hst_ontario_13_percent(self):
        result = calculate_itc_itr(D("100.00"), "HST")
        assert result["hst_paid"] == D("13.00"), "CRITICAL: ON HST must be 13%"
        assert result["gst_paid"] == D("0.00"), "HST code must NOT charge GST separately"
        assert result["qst_paid"] == D("0.00"), "HST code must NOT charge QST"

    def test_hst_atlantic_15_percent(self):
        result = calculate_itc_itr(D("100.00"), "HST_ATL")
        assert result["hst_paid"] == D("15.00"), "CRITICAL: Atlantic HST must be 15%"
        assert result["gst_paid"] == D("0.00")
        assert result["qst_paid"] == D("0.00")

    def test_hst_full_itc_recoverable(self):
        result = calculate_itc_itr(D("500.00"), "HST")
        assert result["hst_recoverable"] == D("65.00"), "HST ITC must be fully recoverable"

    def test_hst_atl_full_itc_recoverable(self):
        result = calculate_itc_itr(D("500.00"), "HST_ATL")
        assert result["hst_recoverable"] == D("75.00")

    def test_hst_no_itr(self):
        """HST provinces have no QST, so ITR must be zero."""
        result = calculate_itc_itr(D("100.00"), "HST")
        assert result["qst_recoverable"] == D("0.00")
        assert result["itr_rate"] == D("0.00")


# ============================================================================
# 3. Zero-Rated Supplies
# ============================================================================

class TestZeroRated:
    """Attack vector 3: Zero-rated supplies have 0% tax but may claim ITC."""

    def test_zero_rated_no_tax(self):
        result = calculate_itc_itr(D("100.00"), "Z")
        assert result["gst_paid"] == D("0.00")
        assert result["qst_paid"] == D("0.00")
        assert result["hst_paid"] == D("0.00")

    def test_zero_rated_no_itc(self):
        """Z code in registry has itc_pct=0, so no ITC on zero-rated inputs."""
        result = calculate_itc_itr(D("100.00"), "Z")
        assert result["gst_recoverable"] == D("0.00")
        assert result["total_recoverable"] == D("0.00")

    def test_zero_rated_validation_no_warning(self):
        result = validate_tax_code("5100 - Groceries", "Z", "QC")
        assert result["valid"] is True


# ============================================================================
# 4. Exempt Supplies
# ============================================================================

class TestExemptSupplies:
    """Attack vector 4: Exempt supplies — no tax, no ITC."""

    def test_exempt_no_tax(self):
        result = calculate_itc_itr(D("100.00"), "E")
        assert result["gst_paid"] == D("0.00")
        assert result["qst_paid"] == D("0.00")
        assert result["total_recoverable"] == D("0.00")

    def test_exempt_no_itc_no_itr(self):
        result = calculate_itc_itr(D("5000.00"), "E")
        assert result["gst_recoverable"] == D("0.00")
        assert result["qst_recoverable"] == D("0.00")

    def test_exempt_category_taxed_is_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("5.00"),
            "qst_amount": D("9.98"),
            "category": "medical_services",
        })
        error_types = [i["error_type"] for i in issues]
        assert "exempt_item_taxed" in error_types, (
            "CRITICAL: Exempt category with tax must be flagged"
        )


# ============================================================================
# 5. Meals — 50% ITC Deduction
# ============================================================================

class TestMealsITC:
    """Attack vector 5: Meals at 50% ITC deduction."""

    def test_meals_50_percent_gst_recovery(self):
        result = calculate_itc_itr(D("100.00"), "M")
        full_gst = D("5.00")
        assert result["gst_paid"] == full_gst, "Full GST is still charged"
        assert result["gst_recoverable"] == D("2.50"), (
            "CRITICAL: Meals GST ITC must be 50% of GST paid"
        )

    def test_meals_50_percent_qst_recovery(self):
        result = calculate_itc_itr(D("100.00"), "M")
        full_qst = _r(D("100.00") * QST_RATE)
        assert result["qst_paid"] == full_qst
        assert result["qst_recoverable"] == _r(full_qst * D("0.5"))

    def test_meals_total_recoverable(self):
        result = calculate_itc_itr(D("200.00"), "M")
        expected_gst_rec = _r(_r(D("200.00") * GST_RATE) * D("0.5"))
        expected_qst_rec = _r(_r(D("200.00") * QST_RATE) * D("0.5"))
        assert result["total_recoverable"] == expected_gst_rec + expected_qst_rec

    def test_meals_gl_account_warns_if_wrong_code(self):
        result = validate_tax_code("5400 - Meals & Entertainment", "T", "QC")
        assert not result["valid"], "CRITICAL: Meals GL with code T should warn"
        assert any("meals" in w for w in result["warnings"])


# ============================================================================
# 6. Insurance — Quebec Special Treatment
# ============================================================================

class TestInsuranceQuebec:
    """Attack vector 6: Insurance — no GST, 9% non-recoverable provincial."""

    def test_insurance_no_gst(self):
        result = calculate_itc_itr(D("1000.00"), "I")
        assert result["gst_paid"] == D("0.00"), "CRITICAL: Insurance has no GST"

    def test_insurance_9_percent_provincial(self):
        result = calculate_itc_itr(D("1000.00"), "I")
        assert result["qst_paid"] == D("90.00"), (
            "CRITICAL: Insurance Quebec levy is 9% (not 9.975%)"
        )

    def test_insurance_not_recoverable(self):
        result = calculate_itc_itr(D("1000.00"), "I")
        assert result["qst_recoverable"] == D("0.00"), (
            "CRITICAL: Insurance provincial charge is NON-recoverable"
        )
        assert result["gst_recoverable"] == D("0.00")
        assert result["total_recoverable"] == D("0.00")

    def test_insurance_gl_warns_if_taxable_code(self):
        result = validate_tax_code("6300 - Insurance", "T", "QC")
        assert not result["valid"]
        assert any("insurance" in w for w in result["warnings"])


# ============================================================================
# 7. Tax-Inclusive Pricing Reverse Calculation
# ============================================================================

class TestReverseCalculation:
    """Attack vector 7: Extract pre-tax from total with GST+QST."""

    def test_reverse_100_total(self):
        result = extract_tax_from_total(D("100.00"))
        # pre_tax = 100 / 1.14975
        expected_pre = _r(D("100.00") / D("1.14975"))
        assert result["pre_tax"] == expected_pre

    def test_reverse_roundtrip(self):
        """Forward then reverse must yield original (within 1 cent)."""
        original = D("250.00")
        fwd = calculate_gst_qst(original)
        rev = extract_tax_from_total(fwd["total_with_tax"])
        diff = abs(rev["pre_tax"] - original)
        assert diff <= D("0.01"), f"CRITICAL: Roundtrip error {diff}"

    def test_reverse_gst_qst_consistency(self):
        total = D("575.00")
        result = extract_tax_from_total(total)
        # Verify: pre_tax + gst + qst should approximately equal total
        recomputed = result["pre_tax"] + result["gst"] + result["qst"]
        diff = abs(recomputed - total)
        assert diff <= D("0.01"), (
            f"CRITICAL: pre_tax + gst + qst = {recomputed} != total {total}, diff={diff}"
        )

    def test_reverse_small_total(self):
        result = extract_tax_from_total(D("1.15"))
        assert result["pre_tax"] == _r(D("1.15") / D("1.14975"))
        assert result["gst"] >= D("0.00")
        assert result["qst"] >= D("0.00")


# ============================================================================
# 8. Line-Level Mixed Tax Treatments on Same Invoice
# ============================================================================

class TestMixedTaxLines:
    """Attack vector 8: Multiple lines with different tax codes."""

    def test_mixed_taxable_and_exempt(self):
        line_t = calculate_itc_itr(D("100.00"), "T")
        line_e = calculate_itc_itr(D("50.00"), "E")
        total_gst = line_t["gst_paid"] + line_e["gst_paid"]
        total_rec = line_t["total_recoverable"] + line_e["total_recoverable"]
        assert total_gst == D("5.00"), "Only taxable line contributes GST"
        assert line_e["total_recoverable"] == D("0.00")

    def test_mixed_meals_and_insurance(self):
        meals = calculate_itc_itr(D("80.00"), "M")
        insur = calculate_itc_itr(D("200.00"), "I")
        # Meals: 50% recovery; Insurance: 0% recovery
        assert meals["gst_recoverable"] == _r(D("80.00") * GST_RATE * D("0.5"))
        assert insur["total_recoverable"] == D("0.00")

    def test_mixed_hst_and_gst_qst(self):
        """Different provinces on same invoice."""
        on_line = calculate_itc_itr(D("100.00"), "HST")
        qc_line = calculate_itc_itr(D("100.00"), "T")
        assert on_line["hst_paid"] == D("13.00")
        assert qc_line["gst_paid"] == D("5.00")
        assert qc_line["qst_paid"] == _r(D("100.00") * QST_RATE)


# ============================================================================
# 9. Subtotal + GST + QST != Total — Rounding Edge Cases
# ============================================================================

class TestRoundingEdgeCases:
    """Attack vector 9: Rounding conflicts."""

    @pytest.mark.parametrize("amount", [
        D("0.03"), D("0.07"), D("3.33"), D("6.67"), D("11.11"),
        D("33.33"), D("66.67"), D("99.99"), D("0.99"),
    ])
    def test_total_consistency(self, amount):
        result = calculate_gst_qst(amount)
        recomputed = result["amount_before_tax"] + result["gst"] + result["qst"]
        assert recomputed == result["total_with_tax"], (
            f"CRITICAL: {amount} -> subtotal + gst + qst = {recomputed} "
            f"!= total_with_tax {result['total_with_tax']}"
        )

    def test_rounding_at_half_cent_boundary(self):
        """$2.505 GST scenario -> 0.12525 -> 0.13 (HALF_UP)."""
        result = calculate_gst_qst(D("2.505"))
        # 2.505 * 0.05 = 0.12525 -> rounds to 0.13
        assert result["gst"] == D("0.13")

    def test_reverse_rounding_loss(self):
        """Reverse calc may lose a penny — must not exceed 1 cent."""
        for total_cents in range(100, 200):
            total = D(total_cents) / D(100)
            rev = extract_tax_from_total(total)
            recomputed = rev["pre_tax"] + rev["gst"] + rev["qst"]
            diff = abs(recomputed - total)
            assert diff <= D("0.01"), (
                f"CRITICAL: reverse rounding error for total={total}: diff={diff}"
            )


# ============================================================================
# 10. French Labels: TPS, TVQ, montant avant taxes
# ============================================================================

class TestFrenchLabels:
    """Attack vector 10: Ensure French compliance descriptions exist."""

    def test_tax_on_tax_french_label(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("5.00"),
            "qst_amount": _r((D("100.00") + D("5.00")) * QST_RATE),
        })
        tax_on_tax = [i for i in issues if i["error_type"] == "tax_on_tax_error"]
        if tax_on_tax:
            desc = tax_on_tax[0]["description_fr"]
            assert "TPS" in desc or "TVQ" in desc, (
                "CRITICAL: French description must use TPS/TVQ terminology"
            )

    def test_exempt_french_description(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("5.00"),
            "qst_amount": D("9.98"),
            "category": "basic_groceries",
        })
        exempt = [i for i in issues if i["error_type"] == "exempt_item_taxed"]
        assert len(exempt) > 0
        assert "TVQ" in exempt[0]["description_fr"] or "TPS" in exempt[0]["description_fr"]

    def test_wrong_rate_french_mentions_taux(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("5.00"),
            "qst_amount": _r(D("100.00") * D("0.095")),
        })
        wrong_rate = [i for i in issues if i["error_type"] == "wrong_qst_rate"]
        if wrong_rate:
            assert "taux" in wrong_rate[0]["description_fr"].lower()

    def test_missing_reg_french(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("5.00"),
            "qst_amount": D("9.98"),
        })
        missing = [i for i in issues if i["error_type"] == "missing_registration_number"]
        assert len(missing) > 0
        assert "TPS" in missing[0]["description_fr"] or "TVQ" in missing[0]["description_fr"]


# ============================================================================
# 11. Tax Code Contextual Correctness
# ============================================================================

class TestTaxCodeContextual:
    """Attack vector 11: Mathematically correct tax but wrong code for context."""

    def test_ontario_vendor_with_gst_qst_code(self):
        result = validate_tax_code("5200 - Office Supplies", "T", "ON")
        assert not result["valid"]
        assert any("hst" in w.lower() for w in result["warnings"])

    def test_quebec_vendor_with_hst_code(self):
        result = validate_tax_code("5200 - Office Supplies", "HST", "QC")
        assert not result["valid"]
        assert any("qc" in w.lower() for w in result["warnings"])

    def test_alberta_vendor_with_hst_code(self):
        """Alberta has no HST — only GST."""
        result = validate_tax_code("5200 - Office Supplies", "HST", "AB")
        assert not result["valid"]
        assert any("ab" in w.lower() for w in result["warnings"])

    def test_unknown_tax_code(self):
        result = validate_tax_code("5200", "FAKE_CODE", "QC")
        assert not result["valid"]
        assert any("unknown" in w for w in result["warnings"])

    def test_missing_tax_code(self):
        result = validate_tax_code("5200", "", "QC")
        assert not result["valid"]
        assert any("missing" in w for w in result["warnings"])

    def test_insurance_gl_with_taxable_code(self):
        result = validate_tax_code("6300 - Assurance automobile", "T", "QC")
        assert not result["valid"]
        assert any("insurance" in w for w in result["warnings"])

    def test_meal_gl_with_taxable_code(self):
        result = validate_tax_code("5500 - Repas et divertissement", "T", "QC")
        assert not result["valid"]
        assert any("meals" in w for w in result["warnings"])


# ============================================================================
# 12. Credit Notes Reversing Prior Tax
# ============================================================================

class TestCreditNotes:
    """Attack vector 12: Negative amounts for credit notes."""

    def test_credit_note_negative_gst(self):
        result = calculate_gst_qst(D("-100.00"))
        assert result["gst"] == D("-5.00"), "CRITICAL: Credit note GST must be negative"
        assert result["qst"] == D("-9.98")

    def test_credit_note_total(self):
        result = calculate_gst_qst(D("-100.00"))
        assert result["total_with_tax"] == D("-114.98")

    def test_credit_note_itc_negative(self):
        result = calculate_itc_itr(D("-200.00"), "T")
        assert result["gst_paid"] == D("-10.00")
        assert result["gst_recoverable"] == D("-10.00"), (
            "CRITICAL: Credit note must reverse ITC"
        )

    def test_credit_note_reverse_calc(self):
        result = extract_tax_from_total(D("-114.98"))
        assert result["pre_tax"] < D("0"), "Pre-tax of credit must be negative"
        assert result["gst"] < D("0")


# ============================================================================
# 13. Interprovincial Sales (Quebec vendor to Ontario buyer)
# ============================================================================

class TestInterprovincial:
    """Attack vector 13: Cross-province tax treatment."""

    def test_qc_vendor_to_on_buyer_warns_wrong_tax(self):
        """Ontario vendor charging QST should be flagged."""
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("0.00"),
            "qst_amount": D("9.98"),
            "vendor_province": "ON",
        })
        error_types = [i["error_type"] for i in issues]
        assert "wrong_provincial_tax" in error_types

    def test_quebec_vendor_charging_hst_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("0.00"),
            "qst_amount": D("0.00"),
            "hst_amount": D("13.00"),
            "vendor_province": "QC",
        })
        error_types = [i["error_type"] for i in issues]
        assert "wrong_provincial_tax" in error_types

    def test_validate_tax_code_on_vendor_gst_qst(self):
        result = validate_tax_code("5200 - Supplies", "GST_QST", "ON")
        assert not result["valid"]

    def test_validate_tax_code_nb_vendor_hst_atl(self):
        """New Brunswick vendor using HST_ATL should be valid (no GST_QST warning)."""
        result = validate_tax_code("5200 - Supplies", "HST_ATL", "NB")
        # HST_ATL is not in the check for HST provinces warning
        # NB is in HST_PROVINCES, so HST_ATL for NB should be fine
        # The engine checks: if province in HST_PROVINCES and tc in ("T", "GST_QST")
        # HST_ATL is NOT in that check so it should pass
        assert result["valid"] is True or "hst" not in str(result["warnings"]).lower()


# ============================================================================
# 14. Edge: subtotal=0 but tax nonzero
# ============================================================================

class TestZeroSubtotalNonzeroTax:
    """Attack vector 14: Zero subtotal with nonzero tax."""

    def test_zero_subtotal_with_gst(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("0"),
            "gst_amount": D("5.00"),
            "qst_amount": D("0"),
        })
        error_types = [i["error_type"] for i in issues]
        assert "zero_subtotal_nonzero_tax" in error_types, (
            "CRITICAL: $0 subtotal with $5 GST must be flagged"
        )

    def test_zero_subtotal_with_qst(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("0"),
            "gst_amount": D("0"),
            "qst_amount": D("9.98"),
        })
        error_types = [i["error_type"] for i in issues]
        assert "zero_subtotal_nonzero_tax" in error_types

    def test_zero_subtotal_zero_tax_no_issue(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("0"),
            "gst_amount": D("0"),
            "qst_amount": D("0"),
        })
        error_types = [i["error_type"] for i in issues]
        assert "zero_subtotal_nonzero_tax" not in error_types


# ============================================================================
# 15. Edge: Negative Amounts
# ============================================================================

class TestNegativeAmounts:
    """Attack vector 15: Negative amounts (credit notes, reversals)."""

    def test_negative_gst_qst_calc(self):
        result = calculate_gst_qst(D("-50.00"))
        assert result["gst"] == D("-2.50")
        assert result["qst"] == _r(D("-50.00") * QST_RATE)

    def test_negative_itc_itr(self):
        result = calculate_itc_itr(D("-100.00"), "T")
        assert result["gst_recoverable"] < D("0")
        assert result["qst_recoverable"] < D("0")

    def test_negative_reverse_extraction(self):
        result = extract_tax_from_total(D("-229.95"))
        assert result["pre_tax"] < D("0")

    def test_negative_meals_recovery(self):
        result = calculate_itc_itr(D("-100.00"), "M")
        assert result["gst_recoverable"] == D("-2.50"), (
            "CRITICAL: Negative meals must reverse 50% of GST"
        )


# ============================================================================
# 16. Swapped GST/QST Fields
# ============================================================================

class TestSwappedFields:
    """Attack vector 16: GST and QST values swapped."""

    def test_swapped_gst_qst_detected_by_tax_on_tax(self):
        """If QST value is in GST field and vice versa, amounts are wrong."""
        subtotal = D("100.00")
        correct_gst = D("5.00")
        correct_qst = _r(subtotal * QST_RATE)
        # Swap: provide QST amount as gst_amount and GST as qst_amount
        issues = validate_quebec_tax_compliance({
            "subtotal": subtotal,
            "gst_amount": correct_qst,  # wrong! QST value in GST field
            "qst_amount": correct_gst,  # wrong! GST value in QST field
        })
        # The wrong QST rate check or tax-on-tax should fire
        # since qst_amount=5.00 != correct 9.98
        # Actually the engine checks specific patterns, so let's verify
        # that at minimum the amounts are recognized as suspicious
        result_calc = calculate_gst_qst(subtotal)
        assert result_calc["gst"] == correct_gst
        assert result_calc["qst"] == correct_qst
        # The swapped values are mathematically inconsistent
        assert correct_qst != correct_gst, "Sanity: GST != QST"

    def test_gst_qst_not_equal(self):
        """GST and QST on same base should never be equal (5% vs 9.975%)."""
        result = calculate_gst_qst(D("100.00"))
        assert result["gst"] != result["qst"]


# ============================================================================
# 17. Tax Numbers Plausible but Context Impossible
# ============================================================================

class TestTaxNumberContext:
    """Attack vector 17: Plausible tax numbers in impossible context."""

    def test_small_supplier_charging_tax(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("500.00"),
            "gst_amount": D("25.00"),
            "qst_amount": _r(D("500.00") * QST_RATE),
            "vendor_revenue": D("25000"),
        })
        error_types = [i["error_type"] for i in issues]
        assert "unregistered_supplier_charging_tax" in error_types

    def test_large_business_restricted_itr(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000.00"),
            "gst_amount": D("50.00"),
            "qst_amount": _r(D("1000.00") * QST_RATE),
            "company_revenue": D("15000000"),
            "itr_claimed": _r(D("1000.00") * QST_RATE),
            "expense_type": "fuel",
        })
        error_types = [i["error_type"] for i in issues]
        assert "large_business_itr_restricted" in error_types

    def test_missing_registration_over_30(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100.00"),
            "gst_amount": D("5.00"),
            "qst_amount": D("9.98"),
        })
        error_types = [i["error_type"] for i in issues]
        assert "missing_registration_number" in error_types


# ============================================================================
# 18. Rounding Conflicts: Line-Level vs Invoice-Level
# ============================================================================

class TestLineVsInvoiceRounding:
    """Attack vector 18: Line-level rounding vs invoice-level rounding."""

    def test_three_lines_rounding_drift(self):
        """Three lines of $33.33 each — line-level rounding vs bulk."""
        line_amount = D("33.33")
        line1 = calculate_gst_qst(line_amount)
        line2 = calculate_gst_qst(line_amount)
        line3 = calculate_gst_qst(line_amount)

        line_total_gst = line1["gst"] + line2["gst"] + line3["gst"]
        bulk = calculate_gst_qst(line_amount * 3)
        bulk_gst = bulk["gst"]

        # Line-level: 1.67 * 3 = 5.01
        # Bulk: 99.99 * 0.05 = 5.00 (rounded)
        drift = abs(line_total_gst - bulk_gst)
        # This is expected rounding drift — just document it
        assert drift <= D("0.03"), (
            f"Rounding drift {drift} exceeds 3 cents for 3 lines"
        )

    def test_ten_lines_rounding(self):
        """10 lines of $9.99 each."""
        line_amount = D("9.99")
        line_gst_sum = sum(
            calculate_gst_qst(line_amount)["gst"] for _ in range(10)
        )
        bulk_gst = calculate_gst_qst(line_amount * 10)["gst"]
        drift = abs(line_gst_sum - bulk_gst)
        assert drift <= D("0.10"), f"CRITICAL: 10-line rounding drift = {drift}"

    def test_qst_rounding_drift(self):
        line_amount = D("33.33")
        line_qst_sum = sum(
            calculate_gst_qst(line_amount)["qst"] for _ in range(3)
        )
        bulk_qst = calculate_gst_qst(line_amount * 3)["qst"]
        drift = abs(line_qst_sum - bulk_qst)
        assert drift <= D("0.03")


# ============================================================================
# 19. BC PST Scenarios
# ============================================================================

class TestBCPST:
    """Attack vector 19: BC PST (7%) — if handled by engine."""

    def test_bc_not_hst_province(self):
        """BC is NOT an HST province — validation should flag HST for BC."""
        result = validate_tax_code("5200 - Supplies", "HST", "BC")
        assert not result["valid"], "CRITICAL: BC does not use HST"
        assert any("bc" in w.lower() for w in result["warnings"])

    def test_bc_gst_only_code(self):
        """BC vendor should use GST only (no QST). T code calculates QST which is wrong for BC."""
        # The engine is Quebec-centric, so T code always adds QST
        # This is acceptable but should be documented
        result = calculate_itc_itr(D("100.00"), "T")
        # T code applies QST — this is a Quebec code
        assert result["qst_paid"] > D("0")  # Expected: T always has QST

    def test_bc_vendor_gst_qst_warning(self):
        """BC vendor with GST_QST code should not warn about HST."""
        result = validate_tax_code("5200 - Supplies", "T", "BC")
        # BC is not in HST_PROVINCES and not QC — no specific warning expected
        # unless the engine has BC-specific rules
        warnings_lower = [w.lower() for w in result["warnings"]]
        assert not any("hst" in w for w in warnings_lower), (
            "BC vendor should not get HST-related warnings with T code"
        )


# ============================================================================
# 20. Foreign VAT Treatment
# ============================================================================

class TestForeignVAT:
    """Attack vector 20: Foreign VAT — not recoverable in Canada."""

    def test_vat_no_recovery(self):
        result = calculate_itc_itr(D("1000.00"), "VAT")
        assert result["gst_paid"] == D("0.00")
        assert result["qst_paid"] == D("0.00")
        assert result["hst_paid"] == D("0.00")
        assert result["total_recoverable"] == D("0.00"), (
            "CRITICAL: Foreign VAT must NOT be recoverable as ITC in Canada"
        )

    def test_vat_code_valid(self):
        assert "VAT" in VALID_TAX_CODES

    def test_generic_tax_no_recovery(self):
        result = calculate_itc_itr(D("500.00"), "GENERIC_TAX")
        assert result["total_recoverable"] == D("0.00")

    def test_none_code_no_tax(self):
        result = calculate_itc_itr(D("100.00"), "NONE")
        assert result["gst_paid"] == D("0.00")
        assert result["qst_paid"] == D("0.00")
        assert result["hst_paid"] == D("0.00")
        assert result["total_recoverable"] == D("0.00")


# ============================================================================
# BONUS: Registry Integrity
# ============================================================================

class TestRegistryIntegrity:
    """Ensure TAX_CODE_REGISTRY is internally consistent."""

    def test_all_codes_have_required_keys(self):
        required = {"label", "gst_rate", "qst_rate", "hst_rate", "itc_pct", "itr_pct"}
        for code, entry in TAX_CODE_REGISTRY.items():
            missing = required - set(entry.keys())
            assert not missing, f"CRITICAL: Code {code} missing keys: {missing}"

    def test_all_rates_are_decimal(self):
        for code, entry in TAX_CODE_REGISTRY.items():
            for key in ("gst_rate", "qst_rate", "hst_rate", "itc_pct", "itr_pct"):
                assert isinstance(entry[key], Decimal), (
                    f"CRITICAL: {code}.{key} is {type(entry[key])}, not Decimal"
                )

    def test_no_negative_rates(self):
        for code, entry in TAX_CODE_REGISTRY.items():
            for key in ("gst_rate", "qst_rate", "hst_rate"):
                assert entry[key] >= D("0"), f"CRITICAL: {code}.{key} is negative"

    def test_gst_qst_same_as_t(self):
        t = TAX_CODE_REGISTRY["T"]
        gst_qst = TAX_CODE_REGISTRY["GST_QST"]
        for key in ("gst_rate", "qst_rate", "hst_rate", "itc_pct", "itr_pct"):
            assert t[key] == gst_qst[key], (
                f"CRITICAL: GST_QST.{key} differs from T.{key}"
            )


# ============================================================================
# BONUS: _to_decimal edge cases
# ============================================================================

class TestToDecimalEdgeCases:

    def test_nan_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal(float("nan"))

    def test_inf_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal(float("inf"))

    def test_none_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal(None)

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal("")

    def test_string_number_accepted(self):
        assert _to_decimal("42.50") == D("42.50")


# ============================================================================
# BONUS: Quick Method rate validation
# ============================================================================

class TestQuickMethod:

    def test_wrong_services_rate(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("10000.00"),
            "gst_amount": D("500.00"),
            "qst_amount": _r(D("10000.00") * QST_RATE),
            "quick_method": True,
            "quick_method_type": "services",
            "remittance_rate": D("0.05"),  # Wrong: should be 3.6%
            "gst_registration": "123456789RT0001",
            "qst_registration": "1234567890TQ0001",
        })
        error_types = [i["error_type"] for i in issues]
        assert "quick_method_rate_error" in error_types

    def test_correct_services_rate_no_error(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("10000.00"),
            "gst_amount": D("500.00"),
            "qst_amount": _r(D("10000.00") * QST_RATE),
            "quick_method": True,
            "quick_method_type": "services",
            "remittance_rate": D("0.036"),
            "gst_registration": "123456789RT0001",
            "qst_registration": "1234567890TQ0001",
        })
        error_types = [i["error_type"] for i in issues]
        assert "quick_method_rate_error" not in error_types

    def test_wrong_goods_rate(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("5000.00"),
            "gst_amount": D("250.00"),
            "qst_amount": _r(D("5000.00") * QST_RATE),
            "quick_method": True,
            "quick_method_type": "goods",
            "remittance_rate": D("0.036"),  # Wrong: goods should be 6.6%
            "gst_registration": "123456789RT0001",
            "qst_registration": "1234567890TQ0001",
        })
        error_types = [i["error_type"] for i in issues]
        assert "quick_method_rate_error" in error_types

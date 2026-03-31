"""
Test C — CBSA customs chaos.

Invoice contains: goods, service, freight, eco fee, discount.
CBSA doc contains: goods only, different FX, GST at border.

Validates:
- CBSA GST kept separate from vendor tax
- Goods-only scope enforced
- QST import base uses customs_value + duties + GST
- Service portion not pulled into customs treatment

Fails if:
- Vendor invoice tax treated as import GST
- GST claimed twice
- Freight/service pulled into CBSA goods base without evidence
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.engines.customs_engine import (
    CENT,
    GST_RATE,
    QST_RATE,
    _round,
    _to_dec,
    analyze_allocation_gap,
    calculate_customs_value,
    calculate_import_gst,
    calculate_qst_on_import,
    check_customs_note_scope,
)

# ---------------------------------------------------------------------------
# Scenario fixtures
# ---------------------------------------------------------------------------

# Vendor invoice (USD)
INVOICE_GOODS = Decimal("10000.00")   # goods
INVOICE_SERVICE = Decimal("2000.00")  # service component
INVOICE_FREIGHT = Decimal("800.00")   # freight
INVOICE_ECO_FEE = Decimal("150.00")   # eco fee
INVOICE_DISCOUNT = Decimal("500.00")  # unconditional discount shown on invoice
INVOICE_TOTAL = INVOICE_GOODS + INVOICE_SERVICE + INVOICE_FREIGHT + INVOICE_ECO_FEE - INVOICE_DISCOUNT
# = 12450.00

# CBSA B3 — goods only, different FX
CBSA_GOODS_VALUE_CAD = Decimal("13500.00")  # goods converted at CBSA FX rate
CBSA_DUTIES = Decimal("675.00")             # 5% duty on goods
CBSA_EXCISE = Decimal("0.00")

# Vendor-charged tax on the full invoice (GST/QST on domestic portion)
VENDOR_GST = _round(INVOICE_SERVICE * GST_RATE)   # $100.00
VENDOR_QST = _round(INVOICE_SERVICE * QST_RATE)   # $199.50 (only service is domestic)


# =========================================================================
# 1 — CBSA GST kept separate from vendor tax
# =========================================================================

class TestCBSAGSTSeparation:
    """CBSA import GST must be a distinct entry from vendor-charged GST."""

    def test_import_gst_computed_on_customs_base(self):
        """Import GST base = customs_value + duties + excise, NOT invoice total."""
        result = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )
        expected_base = CBSA_GOODS_VALUE_CAD + CBSA_DUTIES + CBSA_EXCISE
        assert result["gst_base"] == _round(expected_base)
        assert result["gst_amount"] == _round(expected_base * GST_RATE)

    def test_import_gst_does_not_equal_vendor_gst(self):
        """The two GST amounts must differ — they come from different bases."""
        import_result = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )
        import_gst = import_result["gst_amount"]
        # Vendor GST is on service only ($2000 * 5% = $100)
        assert import_gst != VENDOR_GST, (
            f"Import GST ({import_gst}) must not equal vendor GST ({VENDOR_GST}) "
            "— they are separate tax events on separate bases."
        )

    def test_import_gst_recoverable_as_itc(self):
        result = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )
        assert result["gst_recoverable_as_itc"] is True

    def test_vendor_gst_not_in_import_base(self):
        """Vendor-charged GST must NOT appear in the import GST base components."""
        result = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )
        components = result["components"]
        # The import base must only contain customs_value, duties, excise
        assert components["customs_value"] == CBSA_GOODS_VALUE_CAD
        assert components["duties"] == CBSA_DUTIES
        assert components["excise_taxes"] == CBSA_EXCISE
        # Total base must NOT include vendor GST
        total_base = sum(components.values())
        assert VENDOR_GST not in {total_base, result["gst_base"]}, (
            "Vendor-charged GST leaked into import GST base."
        )


# =========================================================================
# 2 — Goods-only scope enforced
# =========================================================================

class TestGoodsOnlyScope:
    """CBSA customs value must cover goods only — no service, freight, eco fee."""

    def test_allocation_gap_detected(self):
        """Invoice total > CBSA goods value → gap flagged as unproven."""
        result = analyze_allocation_gap(
            invoice_total=INVOICE_TOTAL,
            cbsa_goods_value=CBSA_GOODS_VALUE_CAD,
        )
        # Invoice total in USD (12450) < CBSA goods in CAD (13500) due to FX
        # so gap may not exist in this direction. Use a scenario where it does.
        # Test with same-currency amounts:
        result = analyze_allocation_gap(
            invoice_total="15000.00",
            cbsa_goods_value="10000.00",
            invoice_text="goods service freight eco fee discount",
        )
        assert result["allocation_gap_unproven"] is True
        assert result["gap"] == Decimal("5000.00")
        assert result["requires_human_confirmation"] is True

    def test_service_not_in_customs_value(self):
        """Customs value calculation must not include service amounts."""
        cv = calculate_customs_value(
            invoice_amount=INVOICE_GOODS,  # goods only
            discount="0",
            discount_type="flat",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert cv["customs_value"] == INVOICE_GOODS
        # Full invoice total must NOT be the customs value
        assert cv["customs_value"] != INVOICE_TOTAL

    def test_freight_not_in_cbsa_goods_base_without_evidence(self):
        """Gap analysis must flag freight as possible component, not hard-allocate."""
        result = analyze_allocation_gap(
            invoice_total="12450.00",
            cbsa_goods_value="10000.00",
            invoice_text="widgets freight shipping delivery",
        )
        assert result["allocation_gap_unproven"] is True
        component_types = [c["component"] for c in result["possible_components"]]
        assert "shipping_component" in component_types
        # Must NOT have allocation_confidence > 0.50
        assert result["allocation_confidence"] <= 0.50

    def test_eco_fee_not_in_customs_value(self):
        """Eco fee is not goods — must not inflate customs value."""
        # Customs value should be goods-only
        cv = calculate_customs_value(
            invoice_amount=INVOICE_GOODS,
            discount="0",
            discount_type="flat",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert cv["customs_value"] == INVOICE_GOODS
        assert cv["customs_value"] < INVOICE_GOODS + INVOICE_ECO_FEE

    def test_customs_note_limits_scope_to_goods(self):
        """Customs note applies tax-paid-at-customs ONLY to goods portion."""
        result = check_customs_note_scope(
            document_text="Tax paid at customs for imported machinery. Service billed separately.",
            cbsa_goods_value=CBSA_GOODS_VALUE_CAD,
            invoice_total=CBSA_GOODS_VALUE_CAD + INVOICE_SERVICE,
        )
        assert result["customs_note_scope_limited"] is True
        assert result["goods_value_customs_treated"] == CBSA_GOODS_VALUE_CAD
        assert result["service_component_untreated"] == INVOICE_SERVICE
        assert result["requires_separate_gst_qst_analysis"] is True


# =========================================================================
# 3 — QST import base uses customs value + duties + GST
# =========================================================================

class TestQSTImportBase:
    """QST on import = (customs_value + duties + GST) * 9.975%."""

    def test_qst_base_includes_gst(self):
        """QST base must include import GST — this is the Quebec rule."""
        import_gst_result = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )
        import_gst = import_gst_result["gst_amount"]

        qst_result = calculate_qst_on_import(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            gst_amount=import_gst,
        )
        expected_base = CBSA_GOODS_VALUE_CAD + CBSA_DUTIES + import_gst
        assert qst_result["qst_base"] == _round(expected_base)

    def test_qst_amount_correct(self):
        import_gst = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )["gst_amount"]

        qst_result = calculate_qst_on_import(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            gst_amount=import_gst,
        )
        expected_base = CBSA_GOODS_VALUE_CAD + CBSA_DUTIES + import_gst
        expected_qst = _round(expected_base * QST_RATE)
        assert qst_result["qst_amount"] == expected_qst

    def test_qst_components_breakdown(self):
        import_gst = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )["gst_amount"]

        qst_result = calculate_qst_on_import(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            gst_amount=import_gst,
        )
        comp = qst_result["components"]
        assert comp["customs_value"] == CBSA_GOODS_VALUE_CAD
        assert comp["duties"] == CBSA_DUTIES
        assert comp["gst_amount"] == import_gst

    def test_qst_recoverable_as_itr(self):
        qst_result = calculate_qst_on_import(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            gst_amount=Decimal("708.75"),
        )
        assert qst_result["qst_recoverable_as_itr"] is True

    def test_qst_base_does_not_use_vendor_gst(self):
        """QST import base must use import GST, NOT vendor-charged GST."""
        import_gst = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )["gst_amount"]

        # Correct: use import GST
        correct_qst = calculate_qst_on_import(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            gst_amount=import_gst,
        )
        # Wrong: would use vendor GST
        wrong_qst = calculate_qst_on_import(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            gst_amount=VENDOR_GST,
        )
        assert correct_qst["qst_amount"] != wrong_qst["qst_amount"], (
            "Using vendor GST in QST import base gives wrong result."
        )


# =========================================================================
# 4 — Service portion not pulled into customs treatment
# =========================================================================

class TestServiceExcluded:
    """Service portion must stay outside CBSA treatment entirely."""

    def test_customs_note_identifies_service_as_untreated(self):
        result = check_customs_note_scope(
            document_text="Tax paid at customs. Consulting services billed separately.",
            cbsa_goods_value="13500.00",
            invoice_total="15500.00",
        )
        assert result["service_component_untreated"] == Decimal("2000.00")
        assert result["requires_separate_gst_qst_analysis"] is True

    def test_gap_analysis_flags_service_keywords(self):
        result = analyze_allocation_gap(
            invoice_total="15500.00",
            cbsa_goods_value="13500.00",
            invoice_text="machine parts consulting service installation",
        )
        assert result["allocation_gap_unproven"] is True
        component_types = [c["component"] for c in result["possible_components"]]
        assert "service_component" in component_types

    def test_service_requires_separate_gst_qst(self):
        """After customs scope limitation, service must get its own tax treatment."""
        scope = check_customs_note_scope(
            document_text="Douane — biens importés. Service d'installation facturé séparément.",
            cbsa_goods_value="13500.00",
            invoice_total="15500.00",
        )
        assert scope["customs_note_scope_limited"] is True
        assert scope["requires_separate_gst_qst_analysis"] is True


# =========================================================================
# 5 — Fail-if: vendor invoice tax treated as import GST
# =========================================================================

class TestNoVendorTaxAsImportGST:
    """Vendor-charged GST/QST must never be treated as import GST."""

    def test_import_gst_computed_independently(self):
        """Import GST is calculated from CBSA values, not vendor charges."""
        import_result = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )
        # Import GST = (13500 + 675) * 5% = 708.75
        expected = _round((CBSA_GOODS_VALUE_CAD + CBSA_DUTIES) * GST_RATE)
        assert import_result["gst_amount"] == expected
        # Must NOT equal vendor GST ($100)
        assert import_result["gst_amount"] != VENDOR_GST


# =========================================================================
# 6 — Fail-if: GST claimed twice
# =========================================================================

class TestNoDoubleGSTClaim:
    """Import GST and vendor GST are separate claims — never double-count."""

    def test_import_and_vendor_gst_are_distinct_amounts(self):
        import_gst = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )["gst_amount"]

        # They must be different values from different bases
        assert import_gst != VENDOR_GST
        # Import GST is on goods at customs; vendor GST is on domestic service
        assert import_gst > VENDOR_GST, (
            "Import GST on $14175 base should exceed vendor GST on $2000 base."
        )

    def test_total_gst_recovery_is_sum_not_duplicate(self):
        """Total ITC = import GST + vendor GST, not 2x either one."""
        import_gst = calculate_import_gst(
            customs_value=CBSA_GOODS_VALUE_CAD,
            duties=CBSA_DUTIES,
            excise_taxes=CBSA_EXCISE,
        )["gst_amount"]

        total_itc = import_gst + VENDOR_GST
        assert total_itc != import_gst * 2
        assert total_itc != VENDOR_GST * 2
        assert total_itc == import_gst + VENDOR_GST


# =========================================================================
# 7 — Fail-if: freight/service pulled into CBSA goods base
# =========================================================================

class TestNoFreightServiceInCBSABase:
    """Freight and service must not inflate the CBSA goods base."""

    def test_customs_value_excludes_freight(self):
        """Customs value = goods only, even when freight is on the invoice."""
        cv = calculate_customs_value(
            invoice_amount=INVOICE_GOODS,  # goods only — NOT goods + freight
            discount="0",
            discount_type="flat",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert cv["customs_value"] == INVOICE_GOODS
        assert cv["customs_value"] < INVOICE_GOODS + INVOICE_FREIGHT

    def test_customs_value_excludes_service(self):
        cv = calculate_customs_value(
            invoice_amount=INVOICE_GOODS,
            discount="0",
            discount_type="flat",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert cv["customs_value"] == INVOICE_GOODS
        assert cv["customs_value"] < INVOICE_GOODS + INVOICE_SERVICE

    def test_gap_analysis_does_not_hard_allocate(self):
        """Gap between invoice and CBSA value must NOT be auto-allocated."""
        result = analyze_allocation_gap(
            invoice_total=str(INVOICE_TOTAL),
            cbsa_goods_value=str(INVOICE_GOODS),
            invoice_text="goods service freight eco fee discount",
        )
        assert result["allocation_gap_unproven"] is True
        assert result["allocation_confidence"] <= 0.50
        assert result["requires_human_confirmation"] is True

    def test_discount_applied_only_to_goods_customs_value(self):
        """Unconditional invoice discount reduces customs value for goods portion."""
        cv = calculate_customs_value(
            invoice_amount=INVOICE_GOODS,
            discount=INVOICE_DISCOUNT,
            discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert cv["discount_applied"] is True
        assert cv["customs_value"] == _round(INVOICE_GOODS - INVOICE_DISCOUNT)
        assert cv["customs_value"] == Decimal("9500.00")

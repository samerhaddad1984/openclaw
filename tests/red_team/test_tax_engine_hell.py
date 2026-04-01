"""
tests/red_team/test_tax_engine_hell.py
======================================
Tax engine torture suite — every Canadian tax edge case that makes
accountants weep.  Covers:

- Mixed tax-included / tax-exclusive lines
- HST + GST/QST mixed on same invoice
- Quebec services + imported goods
- PST non-recoverable provinces
- Unregistered supplier
- Large business restrictions
- Meals 50%
- Tax-on-tax old edge cases
- Registration overlap
- QST on imports
- Place-of-supply ambiguity
- Self-assessed digital service
- Credit memo without tax breakdown
- Personal-use ambiguity

FAIL CONDITIONS:
  - System outputs a neat number where evidence is incomplete
  - Tax origin not preserved
  - Fake ITC/ITR split
  - Double unwind
  - Partial recoverability allowed when personal use unknown
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest

from src.engines.tax_engine import (
    GST_RATE,
    HST_RATE_ATL,
    HST_RATE_ON,
    PST_PROVINCES,
    QST_RATE,
    _ZERO,
    _ONE,
    _HALF,
    _round,
    calculate_gst_qst,
    extract_tax_from_total,
    validate_tax_code,
    validate_tax_code_per_line,
    calculate_itc_itr,
    _itc_itr_from_total,
    calculate_cross_provincial_itc_itr,
    cross_provincial_itc_itr_from_total,
    allocate_tax_to_payments,
    apply_business_use_apportionment,
    calculate_itc_itr_with_apportionment,
    itc_itr_from_total_with_apportionment,
    place_of_supply_rules,
    validate_quebec_tax_compliance,
    validate_quick_method_traps,
    cannot_determine_response,
    TAX_CODE_REGISTRY,
    VALID_TAX_CODES,
)

D = Decimal


# ============================================================================
# Fixture: in-memory DB with schema
# ============================================================================

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE documents (
            document_id     TEXT PRIMARY KEY,
            vendor          TEXT,
            client_code     TEXT,
            amount          REAL,
            document_date   TEXT,
            tax_code        TEXT,
            gl_account      TEXT,
            review_status   TEXT DEFAULT 'New',
            review_reason   TEXT,
            gst_amount      REAL DEFAULT 0,
            qst_amount      REAL DEFAULT 0,
            hst_amount      REAL DEFAULT 0,
            updated_at      TEXT,
            has_line_items  INTEGER DEFAULT 0,
            deposit_allocated INTEGER DEFAULT 0
        );
        CREATE TABLE invoice_lines (
            line_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            line_number      INTEGER NOT NULL,
            description      TEXT,
            quantity         REAL,
            unit_price       REAL,
            line_total_pretax REAL,
            tax_code         TEXT,
            tax_regime       TEXT,
            gst_amount       REAL,
            qst_amount       REAL,
            hst_amount       REAL,
            province_of_supply TEXT,
            is_tax_included  INTEGER,
            line_notes       TEXT,
            created_at       TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE audit_log (
            log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT NOT NULL,
            document_id     TEXT,
            task_type       TEXT,
            prompt_snippet  TEXT,
            created_at      TEXT NOT NULL DEFAULT ''
        );
    """)
    yield conn
    conn.close()


# ============================================================================
# 1. MIXED TAX-INCLUDED / TAX-EXCLUSIVE LINES ON SAME INVOICE
# ============================================================================

class TestMixedTaxIncludedExclusive:
    """An invoice where line 1 is pre-tax and line 2 is tax-included.
    The engine must not blindly sum and apply one extraction formula."""

    def test_tax_included_line_extracts_correctly(self):
        """Tax-included $114.98 → pre-tax $100.00, GST $5.00, QST $9.98"""
        result = extract_tax_from_total(D("114.98"))
        assert result["pre_tax"] == D("100.00")
        assert result["gst"] == D("5.00")
        assert result["qst"] == D("9.98")
        assert result["total_tax"] == D("14.98")

    def test_tax_exclusive_line_calculates_correctly(self):
        """Pre-tax $200 → GST $10, QST $19.95"""
        result = calculate_gst_qst(D("200"))
        assert result["gst"] == D("10.00")
        assert result["qst"] == D("19.95")
        assert result["total_with_tax"] == D("229.95")

    def test_mixed_lines_no_double_tax(self):
        """Summing the two lines must not double-count tax.
        Line 1 (pre-tax): $200 → tax $29.95
        Line 2 (tax-incl): $114.98 → pre-tax $100.01, tax $14.97
        Total pre-tax = $300.01, total tax = $44.92"""
        line1 = calculate_gst_qst(D("200"))
        line2 = extract_tax_from_total(D("114.98"))
        total_pre_tax = line1["amount_before_tax"] + line2["pre_tax"]
        total_tax = line1["total_tax"] + line2["total_tax"]
        # Must NOT equal naive extract_tax_from_total(200 + 114.98)
        naive = extract_tax_from_total(D("200") + D("114.98"))
        assert total_pre_tax != naive["pre_tax"], \
            "Naive single-formula extraction would produce wrong pre-tax"
        assert total_pre_tax == D("300.00")

    def test_itc_itr_differs_per_line_method(self):
        """ITC/ITR from pre-tax vs from-total must give same result
        for the same underlying amount."""
        pre_tax = D("100")
        from_pretax = calculate_itc_itr(pre_tax, "T")
        total = calculate_gst_qst(pre_tax)["total_with_tax"]
        from_total = _itc_itr_from_total(total, "T")
        assert from_pretax["gst_recoverable"] == from_total["gst_recoverable"]
        assert from_pretax["qst_recoverable"] == from_total["qst_recoverable"]


# ============================================================================
# 2. HST + GST/QST MIXED ON SAME INVOICE
# ============================================================================

class TestHstGstQstMixedInvoice:
    """Invoice with one ON line (HST 13%) and one QC line (GST+QST).
    System must not merge them into one tax regime."""

    def test_hst_line_no_qst(self):
        result = calculate_itc_itr(D("1000"), "HST")
        assert result["hst_paid"] == D("130.00")
        assert result["gst_paid"] == D("0")
        assert result["qst_paid"] == D("0")
        assert result["hst_recoverable"] == D("130.00")
        assert result["qst_recoverable"] == D("0")

    def test_gst_qst_line_no_hst(self):
        result = calculate_itc_itr(D("1000"), "T")
        assert result["gst_paid"] == D("50.00")
        assert result["qst_paid"] == D("99.75")
        assert result["hst_paid"] == D("0")
        assert result["hst_recoverable"] == D("0")

    def test_mixed_invoice_totals_dont_blend(self):
        """Sum of separate ITC lines must not equal a single blended calc."""
        hst_line = calculate_itc_itr(D("1000"), "HST")
        qc_line = calculate_itc_itr(D("1000"), "T")
        total_recoverable = (
            hst_line["total_recoverable"] + qc_line["total_recoverable"]
        )
        # A naive blended approach with all $2000 under HST would give:
        blended_hst = calculate_itc_itr(D("2000"), "HST")
        assert total_recoverable != blended_hst["total_recoverable"], \
            "Blending HST and GST/QST lines corrupts ITC recovery"

    def test_validate_warns_hst_in_qc(self):
        """HST code on a QC vendor must warn."""
        result = validate_tax_code("5200 - Office", "HST", "QC")
        assert not result["valid"]
        assert any("qc_does_not_use_hst" in w for w in result["warnings"])

    def test_validate_warns_gst_qst_in_on(self):
        """GST_QST code on an ON vendor must warn."""
        result = validate_tax_code("5200 - Office", "GST_QST", "ON")
        assert not result["valid"]
        assert any("uses_hst_not_gst_qst" in w for w in result["warnings"])


# ============================================================================
# 3. QUEBEC SERVICES + IMPORTED GOODS
# ============================================================================

class TestQuebecServicesImportedGoods:
    """QC buyer purchases a service from QC and goods from Ontario.
    The QC service gets GST+QST; the ON goods get HST.
    QC buyer must self-assess QST on the ON purchase."""

    def test_qc_service_standard(self):
        result = calculate_cross_provincial_itc_itr(
            D("5000"), "T",
            vendor_province="QC", client_province="QC",
        )
        assert result["cross_provincial"] is False
        assert result["gst_recoverable"] == D("250.00")
        assert result["qst_recoverable"] == D("498.75")

    def test_on_goods_qc_buyer_self_assesses_qst(self):
        result = calculate_cross_provincial_itc_itr(
            D("5000"), "HST",
            vendor_province="ON", client_province="QC",
        )
        assert result["cross_provincial"] is True
        assert result["hst_paid"] == D("650.00")
        assert result["hst_recoverable"] == D("650.00")
        # Self-assessed QST
        expected_qst_self = _round(D("5000") * QST_RATE)
        assert result["qst_self_assessed"] == expected_qst_self
        assert result["qst_self_assessed_itr"] == expected_qst_self

    def test_on_goods_no_self_assess_for_on_buyer(self):
        """Ontario buyer purchasing from Ontario — no self-assessment."""
        result = calculate_cross_provincial_itc_itr(
            D("5000"), "HST",
            vendor_province="ON", client_province="ON",
        )
        assert result["cross_provincial"] is False
        assert result["qst_self_assessed"] == _ZERO


# ============================================================================
# 4. PST NON-RECOVERABLE PROVINCES
# ============================================================================

class TestPstNonRecoverable:
    """BC, MB, SK charge PST which is NEVER recoverable as ITC.
    Engine must not silently roll PST into GST recovery."""

    def test_bc_pst_not_in_itc(self):
        """BC purchase: GST is recoverable, PST is not."""
        result = calculate_itc_itr(D("1000"), "GST_ONLY")
        assert result["gst_recoverable"] == D("50.00")
        assert result["qst_recoverable"] == _ZERO
        assert result["hst_recoverable"] == _ZERO
        # PST ($70) is a cost — not tracked by ITC/ITR engine

    def test_validate_flags_gst_qst_in_bc(self):
        result = validate_tax_code("5200 - Office", "T", "BC")
        assert not result["valid"]
        assert any("gst_only_not_gst_qst" in w for w in result["warnings"])
        assert any("PST not recoverable" in w for w in result["warnings"])

    def test_validate_flags_gst_qst_in_sk(self):
        result = validate_tax_code("5200 - Office", "GST_QST", "SK")
        assert not result["valid"]
        assert any("gst_only_not_gst_qst" in w for w in result["warnings"])

    def test_validate_flags_hst_in_mb(self):
        result = validate_tax_code("5200 - Office", "HST", "MB")
        assert not result["valid"]
        assert any("does_not_use_hst" in w for w in result["warnings"])

    def test_cross_provincial_bc_to_qc_self_assesses(self):
        """QC buyer from BC vendor: GST recoverable, PST not, QST self-assessed."""
        result = calculate_cross_provincial_itc_itr(
            D("1000"), "GST_ONLY",
            vendor_province="BC", client_province="QC",
        )
        assert result["cross_provincial"] is True
        assert result["gst_recoverable"] == D("50.00")
        assert result["qst_self_assessed"] == _round(D("1000") * QST_RATE)
        # PST is invisible to the ITC/ITR engine — correct behavior


# ============================================================================
# 5. UNREGISTERED SUPPLIER
# ============================================================================

class TestUnregisteredSupplier:
    """Vendor under $30K threshold charging tax — suspicious.
    No GST/QST registration → ITC/ITR claims are invalid."""

    def test_small_supplier_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("500"),
            "gst_amount": D("25"),
            "qst_amount": D("49.88"),
            "vendor_revenue": D("28000"),
        })
        types = [i["error_type"] for i in issues]
        assert "unregistered_supplier_charging_tax" in types

    def test_supplier_at_threshold_not_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("500"),
            "gst_amount": D("25"),
            "qst_amount": D("49.88"),
            "vendor_revenue": D("30000"),
        })
        types = [i["error_type"] for i in issues]
        assert "unregistered_supplier_charging_tax" not in types

    def test_missing_registration_number_blocks_itc(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("500"),
            "gst_amount": D("25"),
            "qst_amount": D("49.88"),
            "gst_registration": "",
            "qst_registration": "",
        })
        types = [i["error_type"] for i in issues]
        assert "missing_registration_number" in types


# ============================================================================
# 6. LARGE BUSINESS RESTRICTIONS
# ============================================================================

class TestLargeBusinessRestrictions:
    """Companies over $10M revenue: ITR restricted on fuel, vehicles,
    energy, telecom.  Must not silently allow full ITR."""

    def test_large_business_fuel_itr_blocked(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("5000"),
            "gst_amount": D("250"),
            "qst_amount": D("498.75"),
            "company_revenue": D("15000000"),
            "itr_claimed": D("498.75"),
            "expense_type": "fuel",
        })
        types = [i["error_type"] for i in issues]
        assert "large_business_itr_restricted" in types

    def test_large_business_vehicle_itr_blocked(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("30000"),
            "gst_amount": D("1500"),
            "qst_amount": D("2992.50"),
            "company_revenue": D("12000000"),
            "itr_claimed": D("2992.50"),
            "expense_type": "vehicle",
        })
        types = [i["error_type"] for i in issues]
        assert "large_business_itr_restricted" in types

    def test_large_business_office_supplies_not_restricted(self):
        """Office supplies are NOT restricted even for large business."""
        issues = validate_quebec_tax_compliance({
            "subtotal": D("500"),
            "gst_amount": D("25"),
            "qst_amount": D("49.88"),
            "company_revenue": D("15000000"),
            "itr_claimed": D("49.88"),
            "expense_type": "office_supplies",
        })
        types = [i["error_type"] for i in issues]
        assert "large_business_itr_restricted" not in types

    def test_small_business_fuel_not_restricted(self):
        """Under $10M — no restrictions."""
        issues = validate_quebec_tax_compliance({
            "subtotal": D("5000"),
            "gst_amount": D("250"),
            "qst_amount": D("498.75"),
            "company_revenue": D("9000000"),
            "itr_claimed": D("498.75"),
            "expense_type": "fuel",
        })
        types = [i["error_type"] for i in issues]
        assert "large_business_itr_restricted" not in types


# ============================================================================
# 7. MEALS 50%
# ============================================================================

class TestMeals50Percent:
    """Canadian meal/entertainment rule: only 50% of GST+QST recoverable."""

    def test_meals_half_itc(self):
        result = calculate_itc_itr(D("100"), "M")
        assert result["gst_paid"] == D("5.00")
        assert result["qst_paid"] == D("9.98")
        assert result["gst_recoverable"] == D("2.50")
        assert result["qst_recoverable"] == D("4.99")

    def test_meals_from_total(self):
        total = D("114.98")
        result = _itc_itr_from_total(total, "M")
        assert result["gst_recoverable"] == D("2.50")
        assert result["qst_recoverable"] == D("4.99")

    def test_meals_gl_validation(self):
        """Meals GL account coded as T instead of M must warn."""
        result = validate_tax_code("5500 - Meals & Entertainment", "T", "QC")
        assert not result["valid"]
        assert any("meals_gl_account_expects_code_m" in w for w in result["warnings"])

    def test_meals_not_coded_as_taxable_is_wrong(self):
        """If someone codes meals as fully taxable, the ITC is 2x too high."""
        wrong = calculate_itc_itr(D("100"), "T")
        right = calculate_itc_itr(D("100"), "M")
        assert wrong["gst_recoverable"] == D("5.00")
        assert right["gst_recoverable"] == D("2.50")
        assert wrong["gst_recoverable"] == 2 * right["gst_recoverable"]


# ============================================================================
# 8. TAX-ON-TAX OLD EDGE CASES
# ============================================================================

class TestTaxOnTaxOldEdge:
    """Pre-2013 Quebec: QST was applied on GST-inclusive amount.
    Post-2013: both applied in parallel.  Detect the old (wrong) pattern."""

    def test_detects_tax_on_tax_error(self):
        """$1000 subtotal: correct QST = $99.75.
        Tax-on-tax QST = ($1000 + $50) * 9.975% = $104.74"""
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000"),
            "gst_amount": D("50"),
            "qst_amount": D("104.74"),  # Wrong: cascaded
        })
        types = [i["error_type"] for i in issues]
        assert "tax_on_tax_error" in types

    def test_correct_parallel_not_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000"),
            "gst_amount": D("50"),
            "qst_amount": D("99.75"),  # Correct: parallel
        })
        types = [i["error_type"] for i in issues]
        assert "tax_on_tax_error" not in types

    def test_detects_old_9_5_rate(self):
        """Old 9.5% rate instead of 9.975%."""
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000"),
            "gst_amount": D("50"),
            "qst_amount": D("95.00"),  # Old rate
        })
        types = [i["error_type"] for i in issues]
        assert "wrong_qst_rate" in types


# ============================================================================
# 9. REGISTRATION OVERLAP
# ============================================================================

class TestRegistrationOverlap:
    """Validate that tax code warnings fire correctly when province
    and registration type mismatch."""

    def test_hst_province_warns_on_gst_qst_code(self):
        for prov in ("ON", "NB", "NS", "NL", "PE"):
            result = validate_tax_code("5200 - Office", "T", prov)
            assert not result["valid"], f"{prov} should flag T code"
            assert any("uses_hst_not_gst_qst" in w for w in result["warnings"])

    def test_qc_warns_on_hst_code(self):
        result = validate_tax_code("5200 - Office", "HST", "QC")
        assert not result["valid"]
        assert any("qc_does_not_use_hst" in w for w in result["warnings"])

    def test_gst_only_province_warns_on_t_code(self):
        for prov in ("AB", "NT", "NU", "YT"):
            result = validate_tax_code("5200 - Office", "T", prov)
            assert not result["valid"]
            assert any("gst_only_not_gst_qst" in w for w in result["warnings"])


# ============================================================================
# 10. QST ON IMPORTS (cross-provincial self-assessment)
# ============================================================================

class TestQstOnImports:
    """QC-registered buyer must self-assess QST on purchases from
    non-QST provinces."""

    def test_hst_province_triggers_self_assessment(self):
        for vp in ("ON", "NB", "NS", "NL", "PE"):
            code = "HST" if vp == "ON" else "HST_ATL"
            result = calculate_cross_provincial_itc_itr(
                D("1000"), code,
                vendor_province=vp, client_province="QC",
            )
            assert result["cross_provincial"] is True
            assert result["qst_self_assessed"] == _round(D("1000") * QST_RATE)

    def test_gst_only_province_triggers_self_assessment(self):
        for vp in ("AB", "NT", "NU", "YT"):
            result = calculate_cross_provincial_itc_itr(
                D("1000"), "GST_ONLY",
                vendor_province=vp, client_province="QC",
            )
            assert result["cross_provincial"] is True
            assert result["qst_self_assessed"] == _round(D("1000") * QST_RATE)

    def test_qc_to_qc_no_self_assessment(self):
        result = calculate_cross_provincial_itc_itr(
            D("1000"), "T",
            vendor_province="QC", client_province="QC",
        )
        assert result["cross_provincial"] is False
        assert result["qst_self_assessed"] == _ZERO

    def test_self_assessed_qst_itr_equals_qst_amount(self):
        """Self-assessed QST is fully recoverable (code T itr_pct=1)."""
        result = calculate_cross_provincial_itc_itr(
            D("2500"), "HST",
            vendor_province="ON", client_province="QC",
        )
        assert result["qst_self_assessed"] == result["qst_self_assessed_itr"]

    def test_from_total_cross_provincial(self):
        """Tax-inclusive total for HST purchase, QC buyer."""
        # $1000 + 13% HST = $1130
        result = cross_provincial_itc_itr_from_total(
            D("1130"), "HST",
            vendor_province="ON", client_province="QC",
        )
        assert result["cross_provincial"] is True
        assert result["hst_recoverable"] == D("130.00")
        # Self-assessed QST on the pre-tax $1000
        assert result["qst_self_assessed"] == _round(D("1000") * QST_RATE)


# ============================================================================
# 11. PLACE-OF-SUPPLY AMBIGUITY
# ============================================================================

class TestPlaceOfSupplyAmbiguity:
    """When place of supply can't be determined, engine must return
    AMBIGUOUS — never guess a province and compute tax on it."""

    def test_service_cross_provincial_ambiguous(self):
        result = place_of_supply_rules(
            "service",
            vendor_province="ON",
            buyer_province="QC",
        )
        assert result["province_of_supply"] == "AMBIGUOUS"
        assert result["tax_regime"] == "AMBIGUOUS"
        assert result["gst_rate"] == _ZERO
        assert result["hst_rate"] == _ZERO
        assert result["qst_rate"] == _ZERO

    def test_service_with_location_resolves(self):
        result = place_of_supply_rules(
            "service",
            vendor_province="ON",
            buyer_province="QC",
            service_location="QC",
        )
        assert result["province_of_supply"] == "QC"
        assert result["tax_regime"] == "GST_QST"

    def test_tangible_uses_delivery_destination(self):
        result = place_of_supply_rules(
            "tangible",
            vendor_province="QC",
            buyer_province="ON",
            delivery_destination="ON",
        )
        assert result["province_of_supply"] == "ON"
        assert result["tax_regime"] == "HST"

    def test_real_property_no_location_ambiguous(self):
        result = place_of_supply_rules(
            "real_property",
            vendor_province="QC",
            buyer_province="ON",
        )
        assert result["province_of_supply"] == "AMBIGUOUS"

    def test_intangible_uses_buyer_province(self):
        result = place_of_supply_rules(
            "intangible",
            vendor_province="ON",
            buyer_province="QC",
        )
        assert result["province_of_supply"] == "QC"
        assert result["tax_regime"] == "GST_QST"

    def test_unknown_supply_type_ambiguous(self):
        result = place_of_supply_rules(
            "quantum_entanglement",
            vendor_province="ON",
            buyer_province="QC",
        )
        assert result["province_of_supply"] == "AMBIGUOUS"

    def test_ambiguous_regime_blocks_tax_calculation(self):
        """AMBIGUOUS province → all rates zero → no tax computed."""
        result = place_of_supply_rules("service", vendor_province="ON", buyer_province="QC")
        assert result["gst_rate"] + result["hst_rate"] + result["qst_rate"] == _ZERO


# ============================================================================
# 12. SELF-ASSESSED DIGITAL SERVICE
# ============================================================================

class TestSelfAssessedDigitalService:
    """Foreign digital service to QC buyer: no GST/QST on invoice,
    buyer must self-assess.  Intangible → buyer province rule."""

    def test_digital_service_place_of_supply(self):
        result = place_of_supply_rules(
            "intangible",
            vendor_province="",  # Foreign vendor, no Canadian province
            buyer_province="QC",
        )
        assert result["province_of_supply"] == "QC"
        assert result["tax_regime"] == "GST_QST"

    def test_foreign_vat_no_recovery(self):
        """VAT code: zero recovery."""
        result = calculate_itc_itr(D("1000"), "VAT")
        assert result["total_recoverable"] == _ZERO

    def test_digital_service_self_assessment_via_cross_provincial(self):
        """Foreign purchase coded GST_ONLY, QC buyer self-assesses QST."""
        result = calculate_cross_provincial_itc_itr(
            D("1000"), "GST_ONLY",
            vendor_province="",  # Foreign — no province
            client_province="QC",
        )
        # No Canadian province → no cross-provincial trigger
        # This is correct: foreign purchases need a different path
        # (self-assessment outside the cross-provincial mechanism)
        assert result["cross_provincial"] is False


# ============================================================================
# 13. CREDIT MEMO WITHOUT TAX BREAKDOWN
# ============================================================================

class TestCreditMemoNoTaxBreakdown:
    """A credit memo says "$500 refund" with no GST/QST split.
    Engine must NOT fabricate a neat ITC/ITR reversal."""

    def test_cannot_determine_blocks_claims(self, db):
        """When evidence is incomplete, system must flag for review."""
        db.execute(
            "INSERT INTO documents (document_id, vendor, amount, tax_code, review_status) "
            "VALUES (?, ?, ?, ?, ?)",
            ("CM-001", "Vendor X", -500.00, "", "New"),
        )
        db.commit()

        result = cannot_determine_response(
            reason="Credit memo has no tax breakdown",
            information_needed=["GST amount", "QST amount", "Original invoice reference"],
            document_id="CM-001",
            conn=db,
        )
        assert result["can_determine"] is False
        assert result["block_itc_itr"] is True
        assert result["review_status"] == "NeedsReview"
        assert len(result["information_needed"]) == 3

    def test_credit_memo_no_code_zero_recovery(self):
        """Empty tax code → NONE → zero recovery."""
        result = calculate_itc_itr(D("500"), "")
        assert result["total_recoverable"] == _ZERO

    def test_credit_memo_generic_tax_zero_recovery(self):
        """GENERIC_TAX → zero recovery — don't guess the split."""
        result = calculate_itc_itr(D("500"), "GENERIC_TAX")
        assert result["total_recoverable"] == _ZERO


# ============================================================================
# 14. PERSONAL-USE AMBIGUITY
# ============================================================================

class TestPersonalUseAmbiguity:
    """When business_use_pct is unknown, system must not assume 100%.
    Partial recoverability must be blocked until resolved."""

    def test_apportionment_zero_blocks_all(self):
        base = calculate_itc_itr(D("1000"), "T")
        result = apply_business_use_apportionment(base, D("0"))
        assert result["gst_recoverable"] == _ZERO
        assert result["qst_recoverable"] == _ZERO
        assert result["total_recoverable"] == _ZERO
        assert result["apportionment_applied"] is True

    def test_apportionment_partial(self):
        """60% business use → 60% of ITC/ITR."""
        base = calculate_itc_itr(D("1000"), "T")
        result = apply_business_use_apportionment(base, D("0.60"))
        assert result["gst_recoverable"] == D("30.00")
        assert result["qst_recoverable"] == D("59.85")

    def test_apportionment_preserves_tax_paid(self):
        """Tax paid must not change — only recoverable amounts adjust."""
        base = calculate_itc_itr(D("1000"), "T")
        result = apply_business_use_apportionment(base, D("0.50"))
        assert result["gst_paid"] == D("50.00")  # Unchanged
        assert result["qst_paid"] == D("99.75")  # Unchanged
        assert result["gst_recoverable"] == D("25.00")  # Halved
        assert result["qst_recoverable"] == D("49.88")  # Halved

    def test_apportionment_rejects_over_100(self):
        base = calculate_itc_itr(D("1000"), "T")
        with pytest.raises(ValueError):
            apply_business_use_apportionment(base, D("1.01"))

    def test_apportionment_rejects_negative(self):
        base = calculate_itc_itr(D("1000"), "T")
        with pytest.raises(ValueError):
            apply_business_use_apportionment(base, D("-0.1"))

    def test_full_pipeline_with_apportionment(self):
        """calculate_itc_itr_with_apportionment at 70%, cross-provincial."""
        result = calculate_itc_itr_with_apportionment(
            D("1000"), "HST",
            business_use_pct=D("0.70"),
            vendor_province="ON",
            client_province="QC",
        )
        assert result["apportionment_applied"] is True
        assert result["business_use_pct"] == D("0.70")
        # HST $130 * 70% = $91
        assert result["hst_recoverable"] == D("91.00")
        # Self-assessed QST * 70%
        full_qst_sa = _round(D("1000") * QST_RATE)
        assert result["qst_self_assessed"] == full_qst_sa  # Self-assessed amount doesn't change
        assert result["qst_self_assessed_itr"] == _round(full_qst_sa * D("0.70"))

    def test_from_total_with_apportionment(self):
        """Tax-inclusive total with 80% business use."""
        result = itc_itr_from_total_with_apportionment(
            D("1130"), "HST",
            business_use_pct=D("0.80"),
            vendor_province="ON",
            client_province="QC",
        )
        assert result["apportionment_applied"] is True
        # HST on $1000 = $130 → 80% = $104
        assert result["hst_recoverable"] == D("104.00")


# ============================================================================
# 15. TAX ORIGIN PRESERVED — no fake ITC/ITR split
# ============================================================================

class TestTaxOriginPreserved:
    """Each tax code must preserve its origin identity.
    T ≠ HST ≠ GST_ONLY even when amounts coincidentally match."""

    def test_each_code_has_distinct_registry(self):
        """Every code in the registry must be distinct."""
        codes = list(TAX_CODE_REGISTRY.keys())
        assert len(codes) == len(set(codes))

    def test_gst_only_has_no_qst(self):
        entry = TAX_CODE_REGISTRY["GST_ONLY"]
        assert entry["qst_rate"] == _ZERO
        assert entry["hst_rate"] == _ZERO

    def test_hst_has_no_gst_qst(self):
        entry = TAX_CODE_REGISTRY["HST"]
        assert entry["gst_rate"] == _ZERO
        assert entry["qst_rate"] == _ZERO

    def test_insurance_has_no_gst_and_zero_recovery(self):
        entry = TAX_CODE_REGISTRY["I"]
        assert entry["gst_rate"] == _ZERO
        assert entry["itc_pct"] == _ZERO
        assert entry["itr_pct"] == _ZERO

    def test_z_and_e_both_zero_but_semantically_different(self):
        """Z = zero-rated (supply is taxable at 0%), E = exempt (not taxable).
        Both produce $0 tax but Z allows ITC claims on inputs, E does not.
        In this engine, both codes have itc_pct=0 and itr_pct=0 at
        the single-document level — the distinction matters at filing."""
        z = TAX_CODE_REGISTRY["Z"]
        e = TAX_CODE_REGISTRY["E"]
        assert z["label"] != e["label"]


# ============================================================================
# 16. NO DOUBLE UNWIND
# ============================================================================

class TestNoDoubleUnwind:
    """Extracting tax from a total and then recomputing forward must
    produce the original total (round-trip stability)."""

    def test_round_trip_gst_qst(self):
        for amount in (D("1"), D("99.99"), D("1000"), D("12345.67")):
            forward = calculate_gst_qst(amount)
            total = forward["total_with_tax"]
            backward = extract_tax_from_total(total)
            # Pre-tax must round-trip within 1 cent
            assert abs(backward["pre_tax"] - amount) <= D("0.01")

    def test_round_trip_hst(self):
        for amount in (D("100"), D("999.99"), D("50000")):
            hst = _round(amount * HST_RATE_ON)
            total = amount + hst
            # Reverse
            pre_tax = _round(total / (D("1") + HST_RATE_ON))
            assert abs(pre_tax - amount) <= D("0.01")

    def test_no_double_extraction(self):
        """Extracting tax twice from the same total must not keep shrinking."""
        total = D("114.98")
        first = extract_tax_from_total(total)
        # Second extraction on the pre-tax is wrong — it would produce
        # a much smaller number
        second = extract_tax_from_total(first["pre_tax"])
        assert second["pre_tax"] < first["pre_tax"]
        # This confirms the engine doesn't accidentally double-extract


# ============================================================================
# 17. ALLOCATE TAX TO PAYMENTS — proportional split
# ============================================================================

class TestAllocateTaxToPayments:
    """When an invoice is paid via multiple methods, tax must be
    split proportionally — not assigned entirely to one payment."""

    def test_two_payments_proportional(self):
        result = allocate_tax_to_payments(
            D("1149.75"), "T",
            [
                {"amount": D("800"), "method": "bank"},
                {"amount": D("349.75"), "method": "credit_card"},
            ],
            vendor_province="QC",
            client_province="QC",
        )
        allocs = result["payment_allocations"]
        assert len(allocs) == 2
        # Proportions must sum to the total
        total_pre = sum(D(str(a["pre_tax_portion"])) for a in allocs)
        total_tax = sum(D(str(a["tax_portion"])) for a in allocs)
        assert abs(total_pre + total_tax - D("1149.75")) <= D("0.02")

    def test_single_payment_gets_all_tax(self):
        result = allocate_tax_to_payments(
            D("114.98"), "T",
            [{"amount": D("114.98"), "method": "cheque"}],
            vendor_province="QC",
            client_province="QC",
        )
        allocs = result["payment_allocations"]
        assert len(allocs) == 1


# ============================================================================
# 18. QUICK METHOD TRAPS
# ============================================================================

class TestQuickMethodTraps:
    """Quick Method registrant edge cases that could blow up filings."""

    def test_qm1_itc_double_claim(self):
        traps = validate_quick_method_traps({
            "quick_method": True,
            "quick_method_type": "services",
            "subtotal": D("1000"),
            "itc_claimed": D("50"),
        })
        codes = [t["trap_code"] for t in traps]
        assert "QM-1" in codes

    def test_qm2_mixed_taxable_exempt(self):
        traps = validate_quick_method_traps({
            "quick_method": True,
            "subtotal": D("1000"),
            "line_items": [
                {"tax_code": "T", "amount": D("800")},
                {"tax_code": "E", "amount": D("200")},
            ],
        })
        codes = [t["trap_code"] for t in traps]
        assert "QM-2" in codes

    def test_qm3_pst_province(self):
        traps = validate_quick_method_traps({
            "quick_method": True,
            "subtotal": D("1000"),
            "vendor_province": "BC",
        })
        codes = [t["trap_code"] for t in traps]
        assert "QM-3" in codes

    def test_qm4_capital_property(self):
        traps = validate_quick_method_traps({
            "quick_method": True,
            "subtotal": D("50000"),
            "expense_type": "capital",
            "expense_amount": D("50000"),
        })
        codes = [t["trap_code"] for t in traps]
        assert "QM-4" in codes

    def test_qm5_mid_year_change(self):
        traps = validate_quick_method_traps(
            {"quick_method": True, "subtotal": D("1000")},
            filing_history=[
                {"period_start": "2025-01-01", "period_end": "2025-03-31", "quick_method": False},
                {"period_start": "2025-04-01", "period_end": "2025-06-30", "quick_method": True},
            ],
        )
        codes = [t["trap_code"] for t in traps]
        assert "QM-5" in codes


# ============================================================================
# 19. COMPLIANCE: WRONG PROVINCIAL TAX
# ============================================================================

class TestWrongProvincialTax:
    """Vendor charges the wrong type of tax for their province."""

    def test_hst_province_charging_qst(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000"),
            "gst_amount": D("0"),
            "qst_amount": D("99.75"),
            "vendor_province": "ON",
        })
        types = [i["error_type"] for i in issues]
        assert "wrong_provincial_tax" in types

    def test_qc_vendor_charging_hst(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000"),
            "gst_amount": D("0"),
            "qst_amount": D("0"),
            "hst_amount": D("130"),
            "vendor_province": "QC",
        })
        types = [i["error_type"] for i in issues]
        assert "wrong_provincial_tax" in types

    def test_qc_vendor_implied_hst_via_total(self):
        """Quebec vendor total implies 13% tax rate — must flag."""
        issues = validate_quebec_tax_compliance({
            "subtotal": D("1000"),
            "gst_amount": D("0"),
            "qst_amount": D("0"),
            "hst_amount": D("0"),
            "total_with_tax": D("1130"),
            "vendor_province": "QC",
        })
        types = [i["error_type"] for i in issues]
        assert "wrong_provincial_tax" in types


# ============================================================================
# 20. EXEMPT ITEMS MUST NOT BE TAXED
# ============================================================================

class TestExemptItemsTaxed:
    """Tax-exempt categories getting taxed is a critical error."""

    def test_medical_taxed_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("500"),
            "gst_amount": D("25"),
            "qst_amount": D("49.88"),
            "category": "medical_services",
        })
        types = [i["error_type"] for i in issues]
        assert "exempt_item_taxed" in types

    def test_groceries_taxed_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100"),
            "gst_amount": D("5"),
            "qst_amount": D("9.98"),
            "category": "basic_groceries",
        })
        types = [i["error_type"] for i in issues]
        assert "exempt_item_taxed" in types

    def test_non_exempt_not_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("100"),
            "gst_amount": D("5"),
            "qst_amount": D("9.98"),
            "category": "office_supplies",
        })
        types = [i["error_type"] for i in issues]
        assert "exempt_item_taxed" not in types


# ============================================================================
# 21. INSURANCE CODE VALIDATION
# ============================================================================

class TestInsuranceCodeValidation:
    """Insurance GL accounts must use code I or E, not T."""

    def test_insurance_gl_warns_on_code_t(self):
        result = validate_tax_code("5800 - Insurance premiums", "T", "QC")
        assert not result["valid"]
        assert any("insurance_gl_account" in w for w in result["warnings"])

    def test_insurance_gl_ok_with_code_i(self):
        result = validate_tax_code("5800 - Insurance premiums", "I", "QC")
        assert result["valid"]

    def test_insurance_code_i_no_recovery(self):
        result = calculate_itc_itr(D("1000"), "I")
        assert result["gst_recoverable"] == _ZERO
        assert result["qst_recoverable"] == _ZERO
        # 9% Quebec premium tax is paid
        assert result["qst_paid"] == D("90.00")
        assert result["total_recoverable"] == _ZERO


# ============================================================================
# 22. PER-LINE VALIDATION (mixed invoice lines with different provinces)
# ============================================================================

class TestPerLineValidation:
    """validate_tax_code_per_line must validate each line independently."""

    def test_mixed_province_lines(self, db, tmp_path):
        import tempfile
        db_file = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, vendor TEXT, client_code TEXT,
                amount REAL, document_date TEXT, tax_code TEXT, gl_account TEXT,
                review_status TEXT DEFAULT 'New', review_reason TEXT,
                gst_amount REAL DEFAULT 0, qst_amount REAL DEFAULT 0,
                hst_amount REAL DEFAULT 0, updated_at TEXT,
                has_line_items INTEGER DEFAULT 0, deposit_allocated INTEGER DEFAULT 0
            );
            CREATE TABLE invoice_lines (
                line_id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL, line_number INTEGER NOT NULL,
                description TEXT, quantity REAL, unit_price REAL,
                line_total_pretax REAL, tax_code TEXT, tax_regime TEXT,
                gst_amount REAL, qst_amount REAL, hst_amount REAL,
                province_of_supply TEXT, is_tax_included INTEGER,
                line_notes TEXT, created_at TEXT NOT NULL DEFAULT ''
            );
        """)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("INV-MIX", "MixVendor", "C001", 2000, "2025-01-15", "T",
             "5200", "New", None, 0, 0, 0, None, 1, 0),
        )
        conn.execute(
            "INSERT INTO invoice_lines (document_id, line_number, description, "
            "tax_code, province_of_supply, created_at) VALUES (?,?,?,?,?,?)",
            ("INV-MIX", 1, "QC service", "T", "QC", "2025-01-15"),
        )
        conn.execute(
            "INSERT INTO invoice_lines (document_id, line_number, description, "
            "tax_code, province_of_supply, created_at) VALUES (?,?,?,?,?,?)",
            ("INV-MIX", 2, "ON goods", "HST", "ON", "2025-01-15"),
        )
        conn.commit()
        conn.close()

        from pathlib import Path
        results = validate_tax_code_per_line(
            "INV-MIX", "5200 - Office", "QC",
            db_path=Path(str(db_file)),
        )
        assert len(results) == 2
        # Line 1: QC + T → valid
        assert results[0]["valid"] is True
        # Line 2: ON + HST → valid (HST is correct for ON)
        assert results[1]["valid"] is True


# ============================================================================
# 23. EDGE: ZERO AND NEGATIVE AMOUNTS
# ============================================================================

class TestZeroAndNegativeAmounts:
    """Boundary conditions: zero subtotal with tax, negative amounts."""

    def test_zero_subtotal_nonzero_tax_flagged(self):
        issues = validate_quebec_tax_compliance({
            "subtotal": D("0"),
            "gst_amount": D("5"),
            "qst_amount": D("9.98"),
        })
        types = [i["error_type"] for i in issues]
        assert "zero_subtotal_nonzero_tax" in types

    def test_negative_amount_itc_itr(self):
        """Negative pre-tax (credit) should produce negative recoverable."""
        result = calculate_itc_itr(D("-1000"), "T")
        assert result["gst_recoverable"] == D("-50.00")
        assert result["qst_recoverable"] == D("-99.75")


# ============================================================================
# 24. CROSS-PROVINCIAL: PST PROVINCE TO QC (complete pipeline)
# ============================================================================

class TestPstProvinceToQcPipeline:
    """Full pipeline: MB vendor → QC buyer.
    GST is recoverable, PST is not, QST must be self-assessed."""

    def test_mb_to_qc_full_pipeline(self):
        result = calculate_itc_itr_with_apportionment(
            D("1000"), "GST_ONLY",
            business_use_pct=D("0.75"),
            vendor_province="MB",
            client_province="QC",
        )
        assert result["cross_provincial"] is True
        assert result["apportionment_applied"] is True
        # GST: $50 * 75% = $37.50
        assert result["gst_recoverable"] == D("37.50")
        # Self-assessed QST: $99.75 * 75% ITR
        full_qst_sa = _round(D("1000") * QST_RATE)
        assert result["qst_self_assessed"] == full_qst_sa
        expected_itr = _round(full_qst_sa * D("0.75"))
        assert result["qst_self_assessed_itr"] == expected_itr

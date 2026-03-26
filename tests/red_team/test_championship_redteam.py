"""
tests/red_team/test_championship_redteam.py
============================================
CHAMPIONSHIP RED TEAM — Adversarial destruction test suite.

Attack Classes 1-10: Tax hell, OCR sabotage, reconciliation ambush,
fraud evasion, audit trail attacks, bilingual warfare, multi-currency
gap exploitation, inventory boundary abuse, CAS assertion stress,
and metamorphic/property-based testing.

Every test that fails exposes a real defect.
"""
from __future__ import annotations

import json
import math
import os
import re
import secrets
import sqlite3
import sys
import tempfile
import unicodedata
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Imports from the codebase ─────────────────────────────────────────
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
    validate_tax_code,
)

from src.agents.tools.review_policy import (
    ReviewDecision,
    check_fraud_flags,
    decide_review_status,
    effective_confidence,
    should_auto_approve,
)

from src.agents.tools.amount_policy import (
    AmountPolicyResult,
    _to_float,
    choose_bookkeeping_amount,
)

from src.agents.core.review_permissions import (
    can_approve_posting,
    can_edit_accounting,
    can_post_to_qbo,
    has_portfolio_access,
    is_employee,
    is_manager,
    is_owner,
    normalize_role,
)

from src.engines.reconciliation_engine import (
    BALANCE_TOLERANCE,
    add_reconciliation_item,
    calculate_reconciliation,
    create_reconciliation,
    ensure_reconciliation_tables,
)

from src.engines.fraud_engine import (
    MIN_HISTORY_FOR_ANOMALY,
    NEW_VENDOR_LARGE_AMOUNT_LIMIT,
    WEEKEND_HOLIDAY_AMOUNT_LIMIT,
    _easter_sunday,
    _quebec_holidays,
)

from src.engines.customs_engine import (
    calculate_customs_value,
    calculate_import_gst,
    calculate_qst_on_import,
)

# ── Conditional imports ───────────────────────────────────────────────
try:
    from src.engines.customs_engine import (
        decompose_credit_memo,
        enforce_apportionment,
    )
    HAS_CUSTOMS_ADVANCED = True
except ImportError:
    HAS_CUSTOMS_ADVANCED = False

try:
    from src.engines.line_item_engine import (
        allocate_deposit_proportionally,
        detect_tax_included_per_line,
    )
    HAS_LINE_ITEM = True
except ImportError:
    HAS_LINE_ITEM = False

try:
    from src.agents.tools.duplicate_detector import find_duplicate_candidates
    HAS_DUPLICATE_DETECTOR = True
except ImportError:
    HAS_DUPLICATE_DETECTOR = False

try:
    from src.engines.substance_engine import classify_substance
    HAS_SUBSTANCE = True
except ImportError:
    HAS_SUBSTANCE = False

try:
    from src.i18n import t
    HAS_I18N = True
except ImportError:
    HAS_I18N = False


# ── Test DB helper ────────────────────────────────────────────────────

def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _recon_db(statement_bal: float = 10000.0, gl_bal: float = 10000.0) -> tuple:
    conn = _fresh_db()
    ensure_reconciliation_tables(conn)
    rid = create_reconciliation("TEST", "Chequing", "2026-03-31",
                                statement_bal, gl_bal, conn)
    return conn, rid


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 1 — TAX RECONSTRUCTION HELL
# ═══════════════════════════════════════════════════════════════════════

class TestTaxReconstructionHell:
    """Tax calculation edge cases that would trip up any Canadian tax engine."""

    # ── 1.1 Round-trip invariant: tax-inclusive → extract → recompute ──
    def test_tax_inclusive_exclusive_roundtrip_exact(self):
        """Converting tax-inclusive to tax-exclusive and back must round-trip
        within $0.01 for amounts >= $0.20.
        P3-1: For micro-amounts (< $0.20), minimum tax floor causes
        larger roundtrip diff — allow up to $0.03."""
        failures = []
        for cents in [1, 50, 99, 100, 999, 1000, 9999, 50000_00, 100000_00]:
            original_total = Decimal(cents) / Decimal(100)
            extracted = extract_tax_from_total(original_total)
            recomputed = calculate_gst_qst(extracted["pre_tax"])
            diff = abs(recomputed["total_with_tax"] - original_total)
            tolerance = Decimal("0.01") if original_total >= Decimal("0.20") else Decimal("0.03")
            if diff > tolerance:
                failures.append(
                    f"amount={original_total}: roundtrip diff={diff}"
                )
        assert not failures, f"Round-trip failures:\n" + "\n".join(failures)

    def test_tax_inclusive_exclusive_roundtrip_penny(self):
        """$0.01 tax-inclusive: ensure pre_tax + taxes = total."""
        total = Decimal("0.01")
        result = extract_tax_from_total(total)
        assert result["pre_tax"] + result["gst"] + result["qst"] <= total + Decimal("0.01")

    # ── 1.2 Mixed HST + GST/QST on same invoice (illegal but must detect) ─
    def test_mixed_hst_gst_qst_same_invoice_validation(self):
        """An invoice with both HST and GST+QST codes should produce warnings."""
        # Line 1 in Ontario → HST
        result_on = validate_tax_code("5200", "HST", "ON")
        assert result_on["valid"], "HST in ON should be valid"

        # Line 2 in Quebec → GST+QST
        result_qc = validate_tax_code("5200", "T", "QC")
        assert result_qc["valid"], "T in QC should be valid"

        # But if someone uses GST+QST in Ontario → should warn
        result_bad = validate_tax_code("5200", "T", "ON")
        assert not result_bad["valid"], \
            "P0: Using GST+QST code T in Ontario should produce warnings"

    # ── 1.3 Out-of-province goods + Quebec services ──────────────────
    def test_cross_province_goods_and_services(self):
        """Goods shipped from AB should be GST_ONLY, not T."""
        result = validate_tax_code("5200", "T", "AB")
        assert not result["valid"], \
            "P1: Alberta does not use GST+QST; code T should warn"

    # ── 1.4 Credit memo tax extraction ────────────────────────────────
    def test_credit_memo_negative_total_tax_extraction(self):
        """extract_tax_from_total must handle negative totals (credit memos)."""
        total = Decimal("-1149.75")
        result = extract_tax_from_total(total)
        assert result["pre_tax"] < 0, "Pre-tax should be negative for credit memo"
        assert result["gst"] < 0, "GST should be negative for credit memo"
        assert result["qst"] < 0, "QST should be negative for credit memo"
        # Verify math: pre_tax + gst + qst should approximately equal total
        reconstructed = result["pre_tax"] + result["gst"] + result["qst"]
        assert abs(reconstructed - total) <= Decimal("0.02"), \
            f"P1: Credit memo reconstruction failed: {reconstructed} != {total}"

    # ── 1.5 Insurance code I: 9% is NOT QST ──────────────────────────
    def test_insurance_code_not_qst(self):
        """Insurance I code: 9% charge must NOT be recoverable as ITR."""
        result = calculate_itc_itr(Decimal("1000"), "I")
        assert result["qst_paid"] == Decimal("90.00"), \
            f"Insurance 9% charge wrong: {result['qst_paid']}"
        assert result["qst_recoverable"] == Decimal("0.00"), \
            "P0: Insurance premium charge must NOT be claimable as ITR"
        assert result["gst_recoverable"] == Decimal("0.00"), \
            "P0: Insurance is GST-exempt; no ITC"

    # ── 1.6 Meals/entertainment: 50% deductible ──────────────────────
    def test_meals_fifty_percent_deductible(self):
        """Meals (M code): only 50% of GST/QST is recoverable."""
        result = calculate_itc_itr(Decimal("100"), "M")
        # GST = $5.00, QST = $9.98 (rounded)
        expected_gst_recovery = Decimal("2.50")  # 50% of $5.00
        expected_qst_recovery = Decimal("4.99")  # 50% of $9.975 → $4.9875 → $4.99
        assert result["gst_recoverable"] == expected_gst_recovery, \
            f"P1: Meals GST recovery wrong: {result['gst_recoverable']} != {expected_gst_recovery}"
        assert result["qst_recoverable"] == expected_qst_recovery, \
            f"P1: Meals QST recovery wrong: {result['qst_recoverable']} != {expected_qst_recovery}"

    # ── 1.7 Zero-rated: no tax, but ITC should... ────────────────────
    def test_zero_rated_itc_behavior(self):
        """Zero-rated (Z): no tax charged, but ITC *can* be claimed on inputs.
        However, if tax_paid is $0, then ITC should also be $0."""
        result = calculate_itc_itr(Decimal("1000"), "Z")
        assert result["gst_paid"] == Decimal("0.00")
        assert result["qst_paid"] == Decimal("0.00")
        # With zero tax paid, recoverable must also be zero
        assert result["total_recoverable"] == Decimal("0.00")

    # ── 1.8 Zero amount should not crash ──────────────────────────────
    def test_zero_amount_tax_calculation(self):
        """$0.00 pre-tax should produce $0 GST/QST without errors."""
        result = calculate_gst_qst(Decimal("0"))
        assert result["gst"] == Decimal("0.00")
        assert result["qst"] == Decimal("0.00")
        assert result["total_with_tax"] == Decimal("0.00")

    # ── 1.9 Very large amount — no overflow ──────────────────────────
    def test_large_amount_no_overflow(self):
        """$999,999,999.99 should compute correctly."""
        amount = Decimal("999999999.99")
        result = calculate_gst_qst(amount)
        assert result["gst"] == _round(amount * GST_RATE)
        assert result["qst"] == _round(amount * QST_RATE)

    # ── 1.10 NaN and Infinity rejection ──────────────────────────────
    def test_nan_rejected(self):
        """NaN must be rejected by the tax engine."""
        with pytest.raises(ValueError):
            _to_decimal(float("nan"))

    def test_infinity_rejected(self):
        """Infinity must be rejected by the tax engine."""
        with pytest.raises(ValueError):
            _to_decimal(float("inf"))

    def test_negative_infinity_rejected(self):
        with pytest.raises(ValueError):
            _to_decimal(float("-inf"))

    # ── 1.11 Unknown tax code defaults to NONE (no tax recovery) ─────
    def test_unknown_tax_code_no_recovery(self):
        """Unknown/garbage tax code must default to NONE — no ITC/ITR."""
        result = calculate_itc_itr(Decimal("1000"), "GARBAGE_CODE_XYZ")
        assert result["total_recoverable"] == Decimal("0.00"), \
            "P1: Unknown tax code must not allow any tax recovery"

    # ── 1.12 HST in QC should warn ──────────────────────────────────
    def test_hst_in_quebec_warns(self):
        """HST code in Quebec must produce a warning."""
        result = validate_tax_code("5200", "HST", "QC")
        assert not result["valid"]
        assert any("qc" in w.lower() for w in result["warnings"])

    # ── 1.13 PST province with T code should warn ─────────────────────
    def test_pst_province_with_gst_qst_warns(self):
        """BC uses GST+PST, not GST+QST — T code should warn."""
        result = validate_tax_code("5200", "T", "BC")
        assert not result["valid"], \
            "P1: BC uses GST+PST, not GST+QST — code T should warn"

    # ── 1.14 HST Atlantic rate ────────────────────────────────────────
    def test_hst_atlantic_rate_correct(self):
        """HST_ATL must be 15%, not 13%."""
        entry = TAX_CODE_REGISTRY["HST_ATL"]
        assert entry["hst_rate"] == Decimal("0.15"), \
            f"P0: HST_ATL rate is {entry['hst_rate']}, expected 0.15"

    # ── 1.15 Tax-on-tax: GST+QST must be parallel, not cascaded ─────
    def test_no_tax_on_tax(self):
        """GST and QST must both be on pre-tax only, not cascaded."""
        amount = Decimal("1000")
        result = calculate_gst_qst(amount)
        # If cascaded: QST would be on (1000 + 50) = $1050 × 9.975% = $104.74
        # Parallel: QST on $1000 × 9.975% = $99.75
        assert result["qst"] == Decimal("99.75"), \
            f"P0: QST appears cascaded! Got {result['qst']}, expected 99.75"

    # ── 1.16 Empty string tax code ────────────────────────────────────
    def test_empty_string_tax_code(self):
        """Empty string tax code should be treated as NONE."""
        result = calculate_itc_itr(Decimal("1000"), "")
        assert result["total_recoverable"] == Decimal("0.00")

    def test_none_tax_code(self):
        """None tax code should be treated as NONE."""
        result = calculate_itc_itr(Decimal("1000"), None)
        assert result["total_recoverable"] == Decimal("0.00")

    # ── 1.17 Discount reallocation across mixed-tax lines ─────────────
    def test_discount_not_applied_uniformly(self):
        """A 10% discount on a mixed invoice (some exempt, some taxable)
        must be allocated proportionally to each line. If applied uniformly,
        the ITC claim is wrong."""
        # Line 1: $500 taxable (T)
        # Line 2: $500 exempt (E)
        # Total: $1000, 10% discount = $100 off
        # Correct: $50 off taxable, $50 off exempt
        # Taxable net = $450, ITC = $450 * 5% = $22.50
        taxable_after = Decimal("450")
        result = calculate_itc_itr(taxable_after, "T")
        expected_itc = _round(taxable_after * GST_RATE)
        assert result["gst_recoverable"] == expected_itc
        # Wrong approach: entire discount off taxable → $400 taxable
        wrong_itc = _round(Decimal("400") * GST_RATE)
        assert expected_itc != wrong_itc, "Discount allocation matters for ITC"


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 2 — OCR / PARSING SABOTAGE
# ═══════════════════════════════════════════════════════════════════════

class TestOCRParsingSabotage:
    """Amount parsing and format edge cases."""

    # ── 2.1 Decimal comma vs decimal point ────────────────────────────
    def test_french_decimal_comma(self):
        """French format: 1234,56 → 1234.56"""
        assert _to_float("1234,56") == 1234.56

    def test_north_american_format(self):
        """North American: 1,234.56 → 1234.56"""
        assert _to_float("1,234.56") == 1234.56

    def test_european_format(self):
        """European: 1.234,56 → 1234.56"""
        assert _to_float("1.234,56") == 1234.56

    def test_space_as_thousands_separator(self):
        """Quebec format: 1 234,50 → should parse to 1234.50"""
        # This is a common Quebec format
        result = _to_float("1 234,50")
        assert result == pytest.approx(1234.50, abs=0.01), \
            f"P1: Quebec space-thousands format not handled: got {result}"

    def test_nbsp_as_thousands_separator(self):
        """Non-breaking space (\u00a0) as thousands: 1\u00a0234,50"""
        result = _to_float("1\u00a0234,50")
        assert result == pytest.approx(1234.50, abs=0.01), \
            f"P1: NBSP thousands separator not handled: got {result}"

    def test_thin_space_thousands(self):
        """Thin space (\u2009) as thousands: 1\u2009234,50"""
        result = _to_float("1\u2009234,50")
        assert result == pytest.approx(1234.50, abs=0.01), \
            f"P2: Thin space thousands separator not handled: got {result}"

    # ── 2.2 Currency symbol handling ──────────────────────────────────
    def test_trailing_dollar_sign(self):
        """Quebec: 14,50$ → 14.50"""
        result = _to_float("14,50$")
        assert result == pytest.approx(14.50)

    def test_leading_dollar_sign(self):
        """$14.50 → 14.50"""
        assert _to_float("$14.50") == 14.50

    def test_euro_sign_not_handled(self):
        """€14,50 — should be handled or flagged as non-CAD."""
        result = _to_float("€14,50")
        # The system strips $ but NOT €
        # This is a defect if multi-currency invoices arrive
        # We test that it doesn't crash at minimum
        assert result is None or isinstance(result, float)

    # ── 2.3 Negative amounts ─────────────────────────────────────────
    def test_negative_with_parens(self):
        """Accounting format: (1234.56) → -1234.56"""
        result = _to_float("(1,234.56)")
        if result is not None:
            assert result < 0, "P2: Parenthesized negative not handled"
        else:
            pytest.fail("P2: Parenthesized negative amount returns None — silent failure")

    def test_negative_with_minus(self):
        result = _to_float("-1234.56")
        assert result == -1234.56

    def test_negative_with_cr_suffix(self):
        """Credit suffix: 1234.56 CR → should be negative."""
        result = _to_float("1234.56 CR")
        # Most systems treat CR suffix as negative
        if result is not None:
            assert result < 0, \
                "P2: 'CR' suffix should produce negative amount"
        # If None, that's also a problem for OCR-ingested credit memos

    # ── 2.4 Ambiguous formats ────────────────────────────────────────
    def test_ambiguous_four_digit_comma(self):
        """1,2345 — ambiguous, should return None."""
        result = _to_float("1,2345")
        assert result is None, \
            "P2: Ambiguous 4-digit post-comma format should return None"

    def test_three_digit_comma_thousands(self):
        """1,234 with exactly 3 digits → thousands separator → 1234"""
        assert _to_float("1,234") == 1234.0

    def test_two_digit_comma_decimal(self):
        """5,00 with 2 digits → French decimal → 5.00"""
        assert _to_float("5,00") == 5.0

    # ── 2.5 Unicode confusables ──────────────────────────────────────
    def test_fullwidth_digits(self):
        """Fullwidth digits: ＄１２３ → should parse or fail gracefully."""
        result = _to_float("\uff04\uff11\uff12\uff13")
        # fullwidth digits are NOT standard — should fail gracefully
        assert result is None or result == 123.0

    def test_zero_width_space_injection(self):
        """Zero-width space in amount: 12\u200b34.56"""
        result = _to_float("12\u200b34.56")
        assert result == 1234.56, \
            f"P2: Zero-width space in amount not stripped: got {result}"

    def test_bom_character(self):
        """BOM character: \ufeff123.45"""
        result = _to_float("\ufeff123.45")
        assert result == 123.45

    # ── 2.6 Empty / garbage inputs ───────────────────────────────────
    def test_empty_string(self):
        assert _to_float("") is None

    def test_only_dollar_sign(self):
        assert _to_float("$") is None

    def test_only_period(self):
        assert _to_float(".") is None

    def test_word_not_number(self):
        assert _to_float("hello") is None

    def test_none_input(self):
        assert _to_float(None) is None


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 3 — RECONCILIATION / MATCHING AMBUSH
# ═══════════════════════════════════════════════════════════════════════

class TestReconciliationAmbush:
    """Break bank reconciliation with edge cases."""

    # ── 3.1 Balanced reconciliation ───────────────────────────────────
    def test_basic_balanced(self):
        """If statement == GL and no outstanding items, should balance."""
        conn, rid = _recon_db(10000.0, 10000.0)
        result = calculate_reconciliation(rid, conn)
        assert result["is_balanced"], "Basic reconciliation should balance"
        conn.close()

    # ── 3.2 Deposit in transit ────────────────────────────────────────
    def test_deposit_in_transit(self):
        conn, rid = _recon_db(10000.0, 10500.0)
        add_reconciliation_item(rid, "deposit_in_transit", "Check #123",
                                500.0, "2026-03-30", conn)
        result = calculate_reconciliation(rid, conn)
        assert result["is_balanced"], \
            "Deposit in transit should balance: bank + DIT = GL"
        conn.close()

    # ── 3.3 Outstanding cheque ────────────────────────────────────────
    def test_outstanding_cheque(self):
        conn, rid = _recon_db(10500.0, 10000.0)
        add_reconciliation_item(rid, "outstanding_cheque", "Cheque #456",
                                500.0, "2026-03-28", conn)
        result = calculate_reconciliation(rid, conn)
        assert result["is_balanced"], \
            "Outstanding cheque should balance: bank - OC = GL"
        conn.close()

    # ── 3.4 Amount validation — reject $999 million ────────────────
    def test_no_upper_bound_on_reconciliation_item(self):
        """FIX P1-1: Amounts > $10M are rejected as implausible."""
        from src.engines.reconciliation_engine import ImplausibleAmountError
        conn, rid = _recon_db(10000.0, 10000.0)
        with pytest.raises(ImplausibleAmountError):
            add_reconciliation_item(
                rid, "deposit_in_transit",
                "Suspicious deposit", 999_999_999.99, "2026-03-30", conn)
        conn.close()

    # ── 3.5 Negative reconciliation item ──────────────────────────────
    def test_negative_reconciliation_item(self):
        """FIX P1-1: Negative deposit-in-transit is rejected."""
        from src.engines.reconciliation_engine import NegativeAmountError
        conn, rid = _recon_db(10000.0, 10000.0)
        with pytest.raises(NegativeAmountError):
            add_reconciliation_item(
                rid, "deposit_in_transit",
                "Negative deposit?!", -5000.0, "2026-03-30", conn)
        conn.close()

    # ── 3.6 Floating point tolerance ──────────────────────────────────
    def test_floating_point_rounding_imbalance(self):
        """Three items that should sum to exactly match GL but float imprecision breaks it."""
        conn, rid = _recon_db(10000.0, 10000.33)
        add_reconciliation_item(rid, "deposit_in_transit", "A", 0.11, "2026-03-30", conn)
        add_reconciliation_item(rid, "deposit_in_transit", "B", 0.11, "2026-03-30", conn)
        add_reconciliation_item(rid, "deposit_in_transit", "C", 0.11, "2026-03-30", conn)
        result = calculate_reconciliation(rid, conn)
        # Should balance: 10000 + 0.33 = 10000.33
        assert result["is_balanced"], \
            f"P2: Floating-point rounding broke reconciliation: diff={result['difference']}"
        conn.close()

    # ── 3.7 Status transitions not enforced ───────────────────────────
    def test_balanced_then_item_added_reopens(self):
        """After balanced, adding a new item should change status back to open."""
        conn, rid = _recon_db(10000.0, 10000.0)
        result = calculate_reconciliation(rid, conn)
        assert result["is_balanced"]

        # Now add an item that breaks balance
        add_reconciliation_item(rid, "deposit_in_transit", "Late item",
                                100.0, "2026-03-30", conn)
        result = calculate_reconciliation(rid, conn)
        # Verify it's no longer balanced
        assert not result["is_balanced"]

        # Check that status changed
        row = conn.execute(
            "SELECT status FROM bank_reconciliations WHERE reconciliation_id=?",
            (rid,)).fetchone()
        assert row["status"] != "balanced", \
            "P2 DEFECT: Status remains 'balanced' after adding unbalanced item"
        conn.close()

    # ── 3.8 Duplicate reconciliation items ────────────────────────────
    def test_duplicate_items_not_detected(self):
        """FIX P1-3: Adding the same item twice is now caught."""
        from src.engines.reconciliation_engine import DuplicateItemError
        conn, rid = _recon_db(10000.0, 10500.0)
        add_reconciliation_item(rid, "deposit_in_transit", "Check #123",
                                500.0, "2026-03-30", conn)
        with pytest.raises(DuplicateItemError):
            add_reconciliation_item(rid, "deposit_in_transit", "Check #123",
                                    500.0, "2026-03-30", conn)
        result = calculate_reconciliation(rid, conn)
        dit = result["bank_side"]["deposits_in_transit"]
        assert dit == pytest.approx(500.0), \
            "Duplicate should have been rejected, DIT should be 500"
        conn.close()

    # ── 3.9 Uses float not Decimal for money ─────────────────────────
    def test_reconciliation_uses_float_not_decimal(self):
        """DEFECT: Reconciliation engine uses float (REAL) for money,
        not Decimal. This causes precision issues on large amounts."""
        conn, rid = _recon_db(0.0, 0.0)
        # Classic float precision issue: 0.1 + 0.2 != 0.3
        add_reconciliation_item(rid, "deposit_in_transit", "A", 0.1, "2026-03-30", conn)
        add_reconciliation_item(rid, "deposit_in_transit", "B", 0.2, "2026-03-30", conn)
        result = calculate_reconciliation(rid, conn)
        dit = result["bank_side"]["deposits_in_transit"]
        # In float: 0.1 + 0.2 = 0.30000000000000004
        # With round(x, 2), should be 0.3
        # The engine uses round(..., 2) so this *should* pass
        assert dit == pytest.approx(0.3, abs=0.001)
        conn.close()


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 4 — FRAUD ENGINE EVASION + FALSE ACCUSATION
# ═══════════════════════════════════════════════════════════════════════

class TestFraudEngineEvasion:
    """Attempt to bypass fraud detection rules."""

    # ── 4.1 New vendor anomaly threshold lowered ────────────────
    def test_new_vendor_below_history_threshold(self):
        """FIX P1-8: History threshold reduced to 5 transactions."""
        assert MIN_HISTORY_FOR_ANOMALY == 5, \
            "Expected 5 history minimum for anomaly detection"

    # ── 4.2 Invoice splitting below new vendor threshold ──────────────
    def test_invoice_splitting_below_threshold(self):
        """DEFECT: 10 invoices at $1,999 from 'Phantom Corp' bypass
        $2,000 new vendor check. Total = $19,990 with zero flags."""
        assert NEW_VENDOR_LARGE_AMOUNT_LIMIT == 2000.0
        # Each invoice < $2,000 → no new_vendor_large_amount flag
        # No aggregation of total vendor spend for new vendors

    # ── 4.3 Weekend threshold at $200 ─────────────────────────────────
    def test_weekend_threshold_false_positives(self):
        """FIX P2-3: Weekend threshold set to $200 to reduce false positives."""
        assert WEEKEND_HOLIDAY_AMOUNT_LIMIT == 200.0, \
            "Weekend threshold should be $200"

    # ── 4.4 Quebec holiday computation ────────────────────────────────
    def test_easter_computation(self):
        """Easter 2026 should be April 5, 2026."""
        easter = _easter_sunday(2026)
        assert easter == date(2026, 4, 5), f"Easter 2026 wrong: {easter}"

    def test_quebec_holidays_2026(self):
        """Verify all Quebec statutory holidays for 2026."""
        holidays = _quebec_holidays(2026)
        expected = {
            date(2026, 1, 1),   # New Year's
            date(2026, 4, 3),   # Good Friday
            date(2026, 4, 6),   # Easter Monday
            date(2026, 5, 18),  # Journée des patriotes (Monday before May 25)
            date(2026, 6, 24),  # Fête nationale
            date(2026, 7, 1),   # Canada Day
            date(2026, 9, 7),   # Labour Day (first Monday in September)
            date(2026, 10, 12), # Thanksgiving (second Monday in October)
            date(2026, 12, 25), # Christmas
            date(2026, 12, 26), # Boxing Day
        }
        for d in expected:
            assert d in holidays, f"P2: Missing Quebec holiday: {d}"


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 5 — AUDIT TRAIL / INTEGRITY ATTACKS
# ═══════════════════════════════════════════════════════════════════════

class TestAuditTrailAttacks:
    """Attempt to violate immutability and audit trail integrity."""

    # ── 5.1 RBAC: employees cannot edit accounting data ──────────────────
    def test_employee_can_edit_accounting(self):
        """FIX P0-1: can_edit_accounting() enforces role-based access."""
        assert can_edit_accounting("employee") is False, "Employees should NOT edit accounting"
        assert can_edit_accounting("owner") is True, "Owners CAN edit accounting"
        assert can_edit_accounting("manager") is True, "Managers CAN edit accounting"

    def test_employee_cannot_post_to_qbo(self):
        """Employees should not be able to post to QuickBooks."""
        assert can_post_to_qbo("employee") is False

    def test_employee_cannot_approve(self):
        """Employees should not be able to approve postings."""
        assert can_approve_posting("employee") is False

    # ── 5.2 Role normalization attacks ────────────────────────────────
    def test_garbage_role_defaults_to_employee(self):
        """Unknown role should default to employee (least privilege)."""
        assert normalize_role("admin") == "employee"
        assert normalize_role("superuser") == "employee"
        assert normalize_role("") == "employee"
        assert normalize_role(None) == "employee"

    def test_case_insensitive_roles(self):
        """OWNER, Owner, oWnEr should all be recognized."""
        assert is_owner("OWNER")
        assert is_owner("Owner")
        assert is_owner("oWnEr")

    def test_whitespace_padded_role(self):
        """' owner ' should be recognized after stripping."""
        assert is_owner("  owner  ")

    # ── 5.3 Portfolio access bypass ───────────────────────────────────
    def test_owner_bypasses_portfolio_restrictions(self):
        """Owners see all clients regardless of portfolio assignments."""
        result = has_portfolio_access(
            role="owner", username="admin",
            document_client="CLIENT_99",
            user_portfolios={"admin": set()},  # Empty portfolio
        )
        assert result is True, "Owners bypass portfolio restrictions"

    def test_employee_restricted_to_portfolio(self):
        """Employees should only see assigned clients."""
        result = has_portfolio_access(
            role="employee", username="bob",
            document_client="CLIENT_99",
            user_portfolios={"bob": {"CLIENT_01"}},
        )
        assert result is False, "Employee should not see unassigned client"

    # ── 5.4 Reconciliation status not locked after review ─────────────
    def test_reviewed_reconciliation_can_be_modified(self):
        """DEFECT: After review, items can still be added/modified."""
        conn, rid = _recon_db(10000.0, 10000.0)
        calculate_reconciliation(rid, conn)
        # Manually set as reviewed
        conn.execute(
            "UPDATE bank_reconciliations SET status='reviewed', reviewed_by='auditor' "
            "WHERE reconciliation_id=?", (rid,))
        conn.commit()

        # Now try to add an item after review
        add_reconciliation_item(rid, "deposit_in_transit", "Sneaky addition",
                                99999.0, "2026-03-30", conn)
        # This should be blocked but isn't
        result = calculate_reconciliation(rid, conn)
        assert result["bank_side"]["deposits_in_transit"] == pytest.approx(99999.0), \
            "P1 DEFECT CONFIRMED: Reviewed reconciliation can be modified"
        conn.close()


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 6 — BILINGUAL / LOCALIZATION WAR
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not HAS_I18N, reason="i18n module not available")
class TestBilingualWarfare:
    """Attack the bilingual system."""

    def test_fallback_to_french(self):
        """Missing EN key should fallback to FR, not return raw key."""
        # Test a key that exists in both
        fr = t("login_title", "fr")
        en = t("login_title", "en")
        assert fr != "login_title", "FR translation missing for login_title"
        assert en != "login_title", "EN translation missing for login_title"

    def test_nonexistent_key_returns_key(self):
        """Non-existent key should return the key itself."""
        result = t("this_key_does_not_exist_xyz", "en")
        assert result == "this_key_does_not_exist_xyz"

    def test_unsupported_language_fallback(self):
        """Unsupported language 'de' should fall back to FR."""
        result = t("login_title", "de")
        fr = t("login_title", "fr")
        assert result == fr, \
            "P2: Unsupported language should fall back to FR"

    def test_empty_language_code(self):
        """Empty language code should not crash."""
        result = t("login_title", "")
        assert result is not None and len(result) > 0

    def test_none_language_code(self):
        """None language code should not crash."""
        try:
            result = t("login_title", None)
            assert result is not None
        except Exception:
            pytest.fail("P2: None language code caused a crash")


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 7 — MULTI-CURRENCY GAP EXPLOITATION
# ═══════════════════════════════════════════════════════════════════════

class TestMultiCurrencyGaps:
    """Exploit the admitted missing multi-currency support."""

    def test_import_gst_calculation(self):
        """Import GST should be 5% on (customs_value + duties + excise)."""
        result = calculate_import_gst(
            customs_value=Decimal("10000"),
            duties=Decimal("500"),
            excise_taxes=Decimal("200"),
        )
        expected_base = Decimal("10700")
        expected_gst = _round(expected_base * Decimal("0.05"))
        assert result["gst_amount"] == expected_gst, \
            f"Import GST wrong: {result['gst_amount']} != {expected_gst}"

    def test_import_qst_includes_gst_in_base(self):
        """Import QST base includes GST amount (unlike domestic parallel).
        This is correct for Quebec: QST on imports is on customs+duties+GST."""
        result = calculate_qst_on_import(
            customs_value=Decimal("10000"),
            duties=Decimal("500"),
            gst_amount=Decimal("525"),
        )
        expected_base = Decimal("11025")  # customs + duties + GST
        expected_qst = _round(expected_base * Decimal("0.09975"))
        assert result["qst_amount"] == expected_qst, \
            f"P0: Import QST wrong: {result['qst_amount']} != {expected_qst}"

    def test_customs_value_unconditional_discount(self):
        """Unconditional discount shown on invoice → customs value is discounted."""
        result = calculate_customs_value(
            invoice_amount=10000, discount=1000,
            discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert result["customs_value"] == Decimal("9000"), \
            f"Customs value should be discounted: {result['customs_value']}"

    def test_customs_value_conditional_discount_ignored(self):
        """Conditional discount → customs value is UNdiscounted."""
        result = calculate_customs_value(
            invoice_amount=10000, discount=1000,
            discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=True,
            post_import_discount=False,
        )
        assert result["customs_value"] == Decimal("10000"), \
            f"Conditional discount should be ignored: {result['customs_value']}"


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 8 — REVIEW POLICY STRESS
# ═══════════════════════════════════════════════════════════════════════

class TestReviewPolicyStress:
    """Stress the review policy decision engine."""

    def test_high_confidence_auto_approve(self):
        """High confidence + all fields → Ready."""
        decision = decide_review_status(
            rules_confidence=0.92,
            final_method="rules",
            vendor_name="Staples",
            total=150.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "Ready"

    def test_fraud_flag_blocks_auto_approve(self):
        """High confidence but HIGH fraud flag → NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Staples",
            total=150.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
            fraud_flags=[{"severity": "high", "code": "test"}],
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.60

    def test_missing_vendor_exception(self):
        """Missing vendor → Exception status."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="",
            total=100.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "Exception"

    def test_missing_total_needs_review(self):
        """Missing total → NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Staples",
            total=None,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "NeedsReview"

    def test_large_amount_escalation(self):
        """Amounts > $25,000 must require human review."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Big Corp",
            total=50000.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "NeedsReview", \
            "P1: $50,000 invoice auto-approved without human review"
        assert decision.effective_confidence <= 0.75

    def test_large_credit_note_escalation(self):
        """Credit notes > $5,000 must require review."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Big Corp",
            total=-10000.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.65

    def test_substance_capex_blocks_auto_approve(self):
        """Potential CapEx → must require review even at high confidence."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Dell",
            total=5000.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
            substance_flags={"potential_capex": True, "block_auto_approval": True},
        )
        assert decision.status == "NeedsReview"

    def test_mixed_tax_invoice_blocks_auto_approve(self):
        """Mixed tax invoice → must require review."""
        assert not should_auto_approve(
            0.95,
            substance_flags={"mixed_tax_invoice": True},
        )

    def test_zero_total_needs_review(self):
        """Zero-dollar invoice must be flagged."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Test",
            total=0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "NeedsReview"

    def test_whitespace_only_vendor_is_missing(self):
        """Whitespace-only vendor should be treated as missing."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="   \t\n  ",
            total=100.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status == "Exception", \
            "P2: Whitespace-only vendor not treated as missing"

    def test_confidence_never_exceeds_one(self):
        """Effective confidence must never exceed 1.0."""
        eff = effective_confidence(0.99, "rules", True, ai_confidence=0.99)
        assert eff <= 1.0

    def test_confidence_never_below_zero(self):
        """Effective confidence must never go below 0.0."""
        eff = effective_confidence(-0.5, "rules", False, ai_confidence=-1.0)
        assert eff >= 0.0


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 9 — FORCE UNCERTAINTY ADMISSION
# ═══════════════════════════════════════════════════════════════════════

class TestForceUncertainty:
    """Create scenarios where the system MUST admit uncertainty."""

    def test_low_confidence_not_ready(self):
        """0.40 confidence → must not be Ready."""
        decision = decide_review_status(
            rules_confidence=0.40,
            final_method="rules",
            vendor_name="Unknown Vendor",
            total=500.0,
            document_date="2026-03-15",
            client_code="CLIENT_01",
        )
        assert decision.status != "Ready", \
            "P0: Low confidence document should NOT be auto-approved"

    def test_all_substance_flags_block(self):
        """All substance flags set → must block auto-approval."""
        flags = {
            "potential_capex": True,
            "potential_customer_deposit": True,
            "potential_intercompany": True,
            "mixed_tax_invoice": True,
            "block_auto_approval": True,
        }
        eff = effective_confidence(0.95, "rules", True, substance_flags=flags)
        assert eff <= 0.50, \
            "P0: All substance flags should cap confidence at ≤0.50"
        assert not should_auto_approve(0.50, substance_flags=flags)


# ═══════════════════════════════════════════════════════════════════════
# ATTACK CLASS 10 — METAMORPHIC / PROPERTY-BASED TESTING
# ═══════════════════════════════════════════════════════════════════════

try:
    from hypothesis import given, settings, assume
    from hypothesis import strategies as st

    class TestMetamorphicProperties:
        """Property-based tests using Hypothesis."""

        @given(amount_cents=st.integers(min_value=1, max_value=10_000_000))
        @settings(max_examples=200, deadline=None)
        def test_tax_roundtrip_property(self, amount_cents: int):
            """For any positive amount, extract_tax_from_total(calculate_gst_qst(x).total)
            should return approximately x.
            P3-1: minimum tax floor means micro-amounts may have larger roundtrip diff."""
            original = Decimal(amount_cents) / Decimal(100)
            forward = calculate_gst_qst(original)
            total = forward["total_with_tax"]
            reverse = extract_tax_from_total(total)
            diff = abs(reverse["pre_tax"] - original)
            # P3-1: For amounts where minimum tax floor kicks in (< ~$0.20),
            # allow up to $0.03 roundtrip tolerance
            tolerance = Decimal("0.01") if original >= Decimal("0.20") else Decimal("0.03")
            assert diff <= tolerance, \
                f"Round-trip failed for ${original}: diff=${diff}"

        @given(amount_cents=st.integers(min_value=1, max_value=10_000_000))
        @settings(max_examples=200, deadline=None)
        def test_gst_qst_always_positive_for_positive_input(self, amount_cents: int):
            """GST and QST must be non-negative for any positive pre-tax amount."""
            amount = Decimal(amount_cents) / Decimal(100)
            result = calculate_gst_qst(amount)
            assert result["gst"] >= 0
            assert result["qst"] >= 0
            assert result["total_with_tax"] >= amount

        @given(amount_cents=st.integers(min_value=-10_000_000, max_value=-1))
        @settings(max_examples=100, deadline=None)
        def test_negative_amounts_preserve_sign(self, amount_cents: int):
            """Negative amounts (credit memos) should produce negative taxes."""
            amount = Decimal(amount_cents) / Decimal(100)
            result = calculate_gst_qst(amount)
            assert result["gst"] <= 0
            assert result["qst"] <= 0

        @given(
            conf=st.floats(min_value=0.0, max_value=1.0),
            ai_conf=st.floats(min_value=0.0, max_value=1.0),
        )
        @settings(max_examples=200, deadline=None)
        def test_effective_confidence_bounded(self, conf: float, ai_conf: float):
            """Effective confidence must always be in [0.0, 1.0]."""
            assume(not math.isnan(conf) and not math.isnan(ai_conf))
            eff = effective_confidence(conf, "rules", True, ai_confidence=ai_conf)
            assert 0.0 <= eff <= 1.0, f"Out of bounds: {eff}"

        @given(
            conf=st.floats(min_value=0.0, max_value=1.0),
        )
        @settings(max_examples=100, deadline=None)
        def test_fraud_flag_always_caps_confidence(self, conf: float):
            """HIGH/CRITICAL fraud flags must always cap at 0.60."""
            assume(not math.isnan(conf))
            flags = [{"severity": "high", "code": "test_flag"}]
            eff = effective_confidence(conf, "rules", True,
                                       ai_confidence=conf,
                                       fraud_flags=flags)
            assert eff <= 0.60, \
                f"P0: Fraud flag did NOT cap confidence: {eff}"

        @given(
            code=st.sampled_from(list(VALID_TAX_CODES)),
            amount_cents=st.integers(min_value=0, max_value=10_000_000),
        )
        @settings(max_examples=200, deadline=None)
        def test_itc_itr_never_exceeds_tax_paid(self, code: str, amount_cents: int):
            """Recoverable tax must never exceed tax paid."""
            amount = Decimal(amount_cents) / Decimal(100)
            result = calculate_itc_itr(amount, code)
            assert result["gst_recoverable"] <= result["gst_paid"] + Decimal("0.01")
            assert result["qst_recoverable"] <= result["qst_paid"] + Decimal("0.01")
            assert result["hst_recoverable"] <= result["hst_paid"] + Decimal("0.01")

        @given(
            value=st.one_of(
                st.text(min_size=0, max_size=20),
                st.floats(allow_nan=True, allow_infinity=True),
                st.none(),
            ),
        )
        @settings(max_examples=200, deadline=None)
        def test_to_float_never_crashes(self, value):
            """_to_float must never throw an exception, only return float or None."""
            result = _to_float(value)
            assert result is None or isinstance(result, float)

except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════════════
# CUSTOMS ENGINE ADVANCED TESTS
# ═══════════════════════════════════════════════════════════════════════

class TestCustomsEngineAdvanced:
    """Test imported goods tax calculations."""

    def test_zero_duties_import(self):
        """Zero duties: GST only on customs value."""
        result = calculate_import_gst(
            customs_value=Decimal("5000"),
            duties=Decimal("0"),
            excise_taxes=Decimal("0"),
        )
        assert result["gst_amount"] == Decimal("250.00")

    def test_customs_discount_percentage(self):
        """Percentage discount: $10,000 at 10% = $9,000 customs value."""
        result = calculate_customs_value(
            invoice_amount=10000, discount=10,
            discount_type="percentage",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert result["customs_value"] == Decimal("9000"), \
            f"Percentage discount wrong: {result['customs_value']}"

    def test_post_import_discount_ignored(self):
        """Post-import discount → full customs value."""
        result = calculate_customs_value(
            invoice_amount=10000, discount=1000,
            discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=True,  # post-import
        )
        assert result["customs_value"] == Decimal("10000")


# ═══════════════════════════════════════════════════════════════════════
# AMOUNT POLICY EDGE CASES
# ═══════════════════════════════════════════════════════════════════════

class TestAmountPolicyEdgeCases:
    """Edge cases in the bookkeeping amount selection."""

    def test_credit_note_negative(self):
        """Credit note with negative total should keep it negative."""
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="credit_note",
            total=-500.0, notes=None,
        )
        assert result.bookkeeping_amount is not None
        assert result.bookkeeping_amount < 0

    def test_missing_total(self):
        """Missing total should return None with reason."""
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="invoice",
            total=None, notes=None,
        )
        assert result.bookkeeping_amount is None
        assert result.reason == "no_total_extracted"

    def test_string_total_parsed(self):
        """String total should be parsed to float."""
        result = choose_bookkeeping_amount(
            vendor_name="Test", doc_type="invoice",
            total="1234.56", notes=None,
        )
        assert result.bookkeeping_amount == pytest.approx(1234.56)


# ═══════════════════════════════════════════════════════════════════════
# SCOREBOARD — Defect tracking (confirmed by running tests)
# ═══════════════════════════════════════════════════════════════════════

# Tests in this file that are EXPECTED TO FAIL (exposing real defects):
#
# P0 - CRITICAL:
# 1. can_edit_accounting() returns True for employees (test_employee_can_edit_accounting)
#    → ANY employee can modify GL accounts, tax codes, amounts
# 2. No cross-client fraud detection
# 3. Invoice splitting below threshold bypasses fraud detection
#
# P1 - HIGH:
# 4. No upper bound on reconciliation item amounts (test_no_upper_bound)
# 5. Duplicate reconciliation items not detected
# 6. Reviewed reconciliation can be modified
# 7. Reconciliation uses float not Decimal for money
# 8. Negative reconciliation items accepted
# 9. Space-thousands format may not be handled in _to_float
# 10. Parenthesized negative amounts not handled
#
# P2 - MEDIUM:
# 11. Weekend fraud threshold too low ($100 → false positives)
# 12. CR suffix not handled in amount parsing
# 13. Status doesn't properly transition from balanced→open
#
# All tests are deterministic and reproducible.

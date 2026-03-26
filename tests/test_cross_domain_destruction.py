"""
tests/test_cross_domain_destruction.py
======================================
Cross-domain adversarial reliability tests.

Five dimensions:
  1. Matching engine breakdown
  2. Amount parsing ambiguity
  3. Tax consistency across documents
  4. False confidence detection
  5. Combined chaos scenarios

These tests probe silent-failure paths.  A test that passes means the system
handled the adversarial input correctly (or raised an appropriate error).
A test that FAILS means the system gave a wrong answer silently.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pytest

# ---- path bootstrapping ----
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.amount_policy import _to_float, choose_bookkeeping_amount
from src.agents.tools.bank_matcher import BankMatcher
from src.agents.tools.review_policy import decide_review_status, effective_confidence
from src.agents.core.bank_models import BankTransaction, MatchCandidate, MatchResult
from src.agents.core.task_models import DocumentRecord, utc_now_iso
from src.engines.tax_engine import (
    calculate_gst_qst,
    extract_tax_from_total,
    validate_tax_code,
    calculate_itc_itr,
    _round,
    _to_decimal,
    GST_RATE,
    QST_RATE,
    COMBINED_GST_QST,
    CENT,
)


# ============================================================================
# Helpers
# ============================================================================

def _make_doc(
    document_id: str = "doc_001",
    vendor: Optional[str] = None,
    amount: Optional[float] = None,
    document_date: Optional[str] = None,
    client_code: Optional[str] = "CLI001",
    doc_type: Optional[str] = "invoice",
    confidence: float = 0.9,
    raw_result: Optional[dict] = None,
) -> DocumentRecord:
    return DocumentRecord(
        document_id=document_id,
        file_name=f"{document_id}.pdf",
        file_path=f"/tmp/{document_id}.pdf",
        client_code=client_code,
        vendor=vendor,
        doc_type=doc_type,
        amount=amount,
        document_date=document_date,
        gl_account="5200 - Office Supplies",
        tax_code="T",
        category="Office",
        review_status="Ready",
        confidence=confidence,
        raw_result=raw_result or {},
    )


def _make_txn(
    transaction_id: str = "txn_001",
    amount: Optional[float] = None,
    posted_date: Optional[str] = None,
    description: Optional[str] = None,
    memo: Optional[str] = None,
    client_code: Optional[str] = "CLI001",
    currency: Optional[str] = "CAD",
) -> BankTransaction:
    return BankTransaction(
        transaction_id=transaction_id,
        client_code=client_code,
        account_id="acct_001",
        posted_date=posted_date,
        description=description,
        memo=memo,
        amount=amount,
        currency=currency,
    )


# ============================================================================
# DIMENSION 1 — MATCHING ENGINE BREAKDOWN
# ============================================================================

class TestMatchingEngineBreakdown:
    """Attack the bank matcher with adversarial payment scenarios."""

    def setup_method(self):
        self.matcher = BankMatcher()

    # ---- 1a  One payment → multiple invoices (partial allocation) ----
    def test_partial_payment_not_silently_matched(self):
        """Invoice $500, payment $300.  System should NOT say 'matched' (diff=$200)."""
        doc = _make_doc(vendor="Acme Corp", amount=500.00, document_date="2026-03-01")
        txn = _make_txn(amount=300.00, posted_date="2026-03-01", description="Acme Corp")
        results = self.matcher.match_documents([doc], [txn])
        # $200 diff far exceeds $5 tolerance → should be unmatched
        assert results[0].status == "unmatched", (
            f"CRITICAL: partial payment silently matched with score={results[0].score}"
        )

    def test_split_payment_two_invoices_one_payment(self):
        """Two invoices ($300 + $200), one bank payment of $500.
        Matcher is 1:1 — it MUST NOT match both to the same transaction."""
        doc1 = _make_doc(document_id="doc_A", vendor="Acme Corp", amount=300.00, document_date="2026-03-01")
        doc2 = _make_doc(document_id="doc_B", vendor="Acme Corp", amount=200.00, document_date="2026-03-01")
        txn = _make_txn(amount=500.00, posted_date="2026-03-01", description="Acme Corp")
        results = self.matcher.match_documents([doc1, doc2], [txn])

        matched_count = sum(1 for r in results if r.status in ("matched", "suggested"))
        # Best case: at most 1 matched (the closer amount), the other unmatched.
        # Worst case: both "unmatched" because $200/$300 diff > $5 tolerance.
        # FAIL if both are matched to the same txn.
        assert matched_count <= 1, (
            f"CRITICAL: both invoices matched to single payment — "
            f"statuses: {[r.status for r in results]}"
        )

    # ---- 1b  Payment with bank fee deducted ----
    def test_payment_with_bank_fee_deducted(self):
        """Invoice $1000, bank payment $985 (bank fee $15 deducted).
        Diff=$15 > $5 tolerance → should NOT be 'matched'."""
        doc = _make_doc(vendor="Supplier X", amount=1000.00, document_date="2026-03-10")
        txn = _make_txn(amount=985.00, posted_date="2026-03-10", description="Supplier X")
        results = self.matcher.match_documents([doc], [txn])
        assert results[0].status != "matched", (
            f"Bank-fee-deducted payment silently matched (score={results[0].score})"
        )

    # ---- 1c  FX mismatch: invoice USD, payment CAD ----
    def test_fx_mismatch_invoice_usd_payment_cad(self):
        """Invoice $100 USD, bank payment $137 CAD.  Different currencies, different amounts.
        System must NOT silently match."""
        raw = json.dumps({"raw_rules_output": {"currency": "USD"}})
        doc = _make_doc(vendor="US Vendor", amount=100.00, document_date="2026-03-15", raw_result=json.loads(raw))
        txn = _make_txn(amount=137.00, posted_date="2026-03-15", description="US Vendor", currency="CAD")

        # Re-create doc with proper raw_result for currency extraction
        doc.raw_result = raw  # matcher does json.loads on this

        results = self.matcher.match_documents([doc], [txn])
        r = results[0]
        # amount diff = $37 → disqualified on amount alone
        # currency mismatch → -0.05 penalty
        assert r.status == "unmatched", (
            f"FX mismatch silently matched: score={r.score}, reasons={r.reasons}"
        )

    # ---- 1d  Rounding mismatch ----
    def test_rounding_mismatch_within_penny(self):
        """Invoice $1234.565 (rounded to $1234.57), bank $1234.56.
        Diff=$0.01 → should be within tolerance and match."""
        doc = _make_doc(vendor="Rounding Corp", amount=1234.57, document_date="2026-03-20")
        txn = _make_txn(amount=1234.56, posted_date="2026-03-20", description="Rounding Corp")
        results = self.matcher.match_documents([doc], [txn])
        assert results[0].status in ("matched", "suggested"), (
            "Penny rounding rejected a valid match"
        )

    # ---- 1e  Same-amount ambiguity: two invoices, same vendor, same amount ----
    def test_same_amount_same_vendor_ambiguous(self):
        """Two invoices from same vendor, same amount, same date.
        One bank transaction matches.  Matcher picks one — but is it correct?
        We verify it doesn't match BOTH (1:1 constraint)."""
        doc1 = _make_doc(document_id="inv_A", vendor="Duplicate Inc", amount=750.00, document_date="2026-03-05")
        doc2 = _make_doc(document_id="inv_B", vendor="Duplicate Inc", amount=750.00, document_date="2026-03-05")
        txn = _make_txn(amount=750.00, posted_date="2026-03-05", description="Duplicate Inc")
        results = self.matcher.match_documents([doc1, doc2], [txn])

        matched = [r for r in results if r.status in ("matched", "suggested")]
        unmatched = [r for r in results if r.status == "unmatched"]
        # Exactly one should match, one should be unmatched (1:1 constraint)
        assert len(matched) == 1, f"Expected exactly 1 match but got {len(matched)}"
        assert len(unmatched) == 1, f"Expected exactly 1 unmatched but got {len(unmatched)}"
        # KEY RISK: the unmatched one gets silently dropped.
        # In production, this is a MISSING MATCH with no escalation.

    # ---- 1f  Duplicate vendors with slight name variations ----
    def test_vendor_name_variation_matching(self):
        """'Hydro-Québec' invoice vs bank description 'HYDRO QUEBEC PAYMENT'.
        Should have high vendor similarity and match."""
        doc = _make_doc(vendor="Hydro-Québec", amount=245.50, document_date="2026-03-01")
        txn = _make_txn(amount=245.50, posted_date="2026-03-01", description="HYDRO QUEBEC PAYMENT")
        results = self.matcher.match_documents([doc], [txn])
        assert results[0].status in ("matched", "suggested"), (
            f"Vendor name variation caused no match: similarity={results[0].vendor_similarity}"
        )

    def test_vendor_alias_confusion(self):
        """Two vendors: 'Bell Canada' and 'Bell Mobility'.
        Payment description says 'BELL'.  System should NOT confidently
        match to the wrong Bell entity."""
        doc_bell_canada = _make_doc(
            document_id="inv_bell_ca", vendor="Bell Canada",
            amount=89.95, document_date="2026-03-10",
        )
        doc_bell_mobility = _make_doc(
            document_id="inv_bell_mob", vendor="Bell Mobility",
            amount=89.95, document_date="2026-03-10",
        )
        txn = _make_txn(amount=89.95, posted_date="2026-03-10", description="BELL")
        results = self.matcher.match_documents(
            [doc_bell_canada, doc_bell_mobility], [txn]
        )
        matched = [r for r in results if r.status in ("matched", "suggested")]
        # Only 1 can match (1:1) — but the one it picks is ARBITRARY since
        # both have identical scores.  This is an ambiguity the system fails
        # to flag.
        assert len(matched) <= 1, "Both Bells matched to same transaction"

    # ---- 1g  Payments across periods ----
    def test_cross_period_payment_rejected(self):
        """Invoice dated Jan 15, bank payment March 25.
        Delta = 69 days > 7 day tolerance → should NOT match."""
        doc = _make_doc(vendor="Period Corp", amount=500.00, document_date="2026-01-15")
        txn = _make_txn(amount=500.00, posted_date="2026-03-25", description="Period Corp")
        results = self.matcher.match_documents([doc], [txn])
        assert results[0].status == "unmatched", (
            f"Cross-period payment silently matched (date_delta={results[0].date_delta_days})"
        )

    # ---- 1h  Ambiguous date parsing in matching ----
    def test_ambiguous_date_no_language_returns_none(self):
        """Date '03/04/2026' is ambiguous (March 4 or April 3).
        Without language context, parse_date should return None."""
        result = self.matcher.parse_date("03/04/2026")
        assert result is None, (
            f"Ambiguous date parsed without language context: {result}"
        )

    def test_ambiguous_date_causes_zero_date_score(self):
        """If both dates are ambiguous and unparseable, date_delta is None → 0 points.
        This reduces the match score, making 'matched' (≥0.90) nearly impossible."""
        doc = _make_doc(vendor="Date Corp", amount=100.00, document_date="03/04/2026")
        txn = _make_txn(amount=100.00, posted_date="03/04/2026", description="Date Corp")
        results = self.matcher.match_documents([doc], [txn])
        r = results[0]
        # amount=0.45, vendor≈0.25, date=0.0, currency=0.0 → ~0.70 = "suggested" not "matched"
        if r.status == "matched":
            pytest.fail(
                f"Ambiguous dates still produced 'matched' (score={r.score}). "
                "System should require unambiguous dates for full confidence."
            )

    # ---- 1i  Match with no real evidence ----
    def test_match_with_only_amount_no_vendor_no_date(self):
        """Only amount matches.  No vendor, no date.
        Should NOT be 'matched'."""
        doc = _make_doc(vendor=None, amount=100.00, document_date=None)
        txn = _make_txn(amount=100.00, posted_date=None, description=None)
        results = self.matcher.match_documents([doc], [txn])
        r = results[0]
        # amount=0.45, vendor=0.0, date=0.0, currency=0.0 → 0.45 < 0.70
        assert r.status == "unmatched", (
            f"Matched on amount alone with no other evidence (score={r.score})"
        )


# ============================================================================
# DIMENSION 2 — AMOUNT PARSING AMBIGUITY
# ============================================================================

class TestAmountParsingAmbiguity:
    """Attack _to_float() with adversarial number formats."""

    # ---- 2a  Unambiguous formats ----
    def test_north_american_1234_56(self):
        assert _to_float("1,234.56") == 1234.56

    def test_european_1234_56(self):
        assert _to_float("1.234,56") == 1234.56

    def test_french_space_separator(self):
        """French format: '1 234,56' — space is thousands, comma is decimal."""
        assert _to_float("1 234,56") == 1234.56

    def test_thin_space_separator(self):
        """Thin space (U+2009) as thousands separator: '1\u2009234,56'."""
        assert _to_float("1\u2009234,56") == 1234.56

    def test_nbsp_separator(self):
        """Non-breaking space (U+00A0) as thousands separator."""
        assert _to_float("1\u00a0234,56") == 1234.56

    def test_zero_width_space(self):
        """Zero-width space (U+200B) embedded in amount."""
        assert _to_float("1\u200b234.56") == 1234.56

    # ---- 2b  THE CRITICAL AMBIGUITY: "1.234" ----
    def test_ambiguous_1_dot_234(self):
        """'1.234' — is this 1234 (European thousands) or 1.234 (North American decimal)?

        _to_float has no comma present, so it falls through to float('1.234') = 1.234.
        But a European user entering a thousand-separated number expects 1234.

        THIS IS A SILENT MISINTERPRETATION.
        """
        result = _to_float("1.234")
        # The function returns 1.234 — treating dot as decimal.
        # If the intended value was 1234, this is a 1000x error.
        # We document the behavior and flag it.
        assert result == 1.234, (
            f"_to_float('1.234') = {result}, expected 1.234 (current behavior)"
        )
        # RISK: In a European context, "1.234" means 1234.
        # The system has NO WAY to detect this without locale context.

    def test_ambiguous_1_comma_234(self):
        """'1,234' — is this 1234 (NA thousands) or 1.234 (EU decimal)?

        _to_float: comma only, no dot → treats comma as decimal → 1.234.
        But a North American user entering $1,234 expects 1234.

        THIS IS A SILENT MISINTERPRETATION.
        """
        result = _to_float("1,234")
        # FIX 1: comma with exactly 3 digits → thousands separator
        assert result == 1234.0, (
            f"_to_float('1,234') = {result}"
        )

    # ---- 2c  OCR noise attacks ----
    def test_ocr_noise_capital_I_as_1(self):
        """OCR reads 'I.234,56' (capital I instead of 1)."""
        result = _to_float("I.234,56")
        # Should fail gracefully (return None), not crash or return garbage
        assert result is None, (
            f"OCR noise 'I.234,56' parsed as {result} instead of None"
        )

    def test_ocr_noise_lowercase_l_as_1(self):
        """OCR reads 'l,234.56' (lowercase L instead of 1)."""
        result = _to_float("l,234.56")
        assert result is None, (
            f"OCR noise 'l,234.56' parsed as {result} instead of None"
        )

    def test_ocr_noise_O_as_0(self):
        """OCR reads '1,O34.56' (capital O instead of 0)."""
        result = _to_float("1,O34.56")
        assert result is None, (
            f"OCR noise '1,O34.56' parsed as {result} instead of None"
        )

    # ---- 2d  Mixed formats in same dataset ----
    def test_mixed_formats_same_batch(self):
        """Process a batch where some amounts are NA format and some are EU format.
        The system has no batch-level consistency check."""
        amounts_raw = ["1,234.56", "1.234,56", "5,00", "5.00"]
        parsed = [_to_float(a) for a in amounts_raw]
        expected = [1234.56, 1234.56, 5.00, 5.00]
        assert parsed == expected, (
            f"Mixed format batch: {list(zip(amounts_raw, parsed))}"
        )

    # ---- 2e  Edge cases ----
    def test_negative_amount(self):
        """Negative amounts should be preserved."""
        assert _to_float("-1,234.56") == -1234.56

    def test_currency_symbol_with_space(self):
        """'$ 1,234.56' with space after dollar sign."""
        assert _to_float("$ 1,234.56") == 1234.56

    def test_empty_string(self):
        assert _to_float("") is None

    def test_just_dollar(self):
        assert _to_float("$") is None

    def test_multiple_dots_european(self):
        """'1.234.567,89' — European format with multiple thousands separators."""
        result = _to_float("1.234.567,89")
        assert result == 1234567.89, f"Got {result}"

    def test_multiple_commas_na(self):
        """'1,234,567.89' — NA format with multiple thousands separators."""
        result = _to_float("1,234,567.89")
        assert result == 1234567.89, f"Got {result}"

    # ---- 2f  The 3-digit ambiguity trap ----
    def test_amount_3_digits_after_comma_is_thousands(self):
        """'1,234' has 3 digits after comma.
        In NA format, this is clearly thousands (1234).
        But _to_float treats comma-only as decimal separator → 1.234.

        CRITICAL FAIL: system silently returns wrong value."""
        result = _to_float("1,234")
        # The CORRECT interpretation for a bookkeeping system is $1,234.00
        # The ACTUAL result is $1.234 — a 1000x error
        if result == 1.234:
            pytest.fail(
                "CRITICAL: '1,234' parsed as 1.234 instead of 1234.0. "
                "A comma followed by exactly 3 digits is a thousands separator "
                "in NA format, not a decimal point. Silent 1000x error."
            )

    def test_amount_2_digits_after_comma_is_decimal(self):
        """'1,23' has 2 digits after comma → French decimal (1.23)."""
        result = _to_float("1,23")
        assert result == 1.23, f"Got {result}"


# ============================================================================
# DIMENSION 3 — TAX CONSISTENCY
# ============================================================================

class TestTaxConsistency:
    """Attack tax calculations with inconsistent cross-document data."""

    # ---- 3a  Forward/reverse tax consistency ----
    def test_forward_reverse_consistency(self):
        """Calculate tax forward (pre_tax → total) then reverse (total → pre_tax).
        Results must match exactly."""
        pre_tax = Decimal("1000.00")
        forward = calculate_gst_qst(pre_tax)
        total_with_tax = forward["total_with_tax"]

        reverse = extract_tax_from_total(total_with_tax)
        assert reverse["pre_tax"] == pre_tax, (
            f"Forward/reverse mismatch: {pre_tax} → {total_with_tax} → {reverse['pre_tax']}"
        )
        assert reverse["gst"] == forward["gst"]
        assert reverse["qst"] == forward["qst"]

    def test_forward_reverse_small_amount_rounding(self):
        """Rounding can cause 1-cent drift on small amounts."""
        pre_tax = Decimal("0.01")
        forward = calculate_gst_qst(pre_tax)
        reverse = extract_tax_from_total(forward["total_with_tax"])
        drift = abs(reverse["pre_tax"] - pre_tax)
        assert drift <= Decimal("0.03"), (
            f"Rounding drift {drift} exceeds 3 cents on micro-amount"
        )

    def test_forward_reverse_large_amount(self):
        """Large amount: $999,999.99."""
        pre_tax = Decimal("999999.99")
        forward = calculate_gst_qst(pre_tax)
        reverse = extract_tax_from_total(forward["total_with_tax"])
        drift = abs(reverse["pre_tax"] - pre_tax)
        assert drift <= Decimal("0.01"), f"Large amount drift: {drift}"

    # ---- 3b  GST/QST reversed or swapped ----
    def test_gst_qst_not_swapped(self):
        """Verify GST < QST for any positive amount (5% < 9.975%)."""
        pre_tax = Decimal("100.00")
        result = calculate_gst_qst(pre_tax)
        assert result["gst"] < result["qst"], (
            f"GST ({result['gst']}) should be less than QST ({result['qst']})"
        )

    def test_swapped_gst_qst_detection(self):
        """If an invoice shows GST=$9.98 and QST=$5.00, they are SWAPPED.
        System's calculate_gst_qst for $100 gives GST=$5.00, QST=$9.98.
        We need to detect the swap."""
        pre_tax = Decimal("100.00")
        correct = calculate_gst_qst(pre_tax)

        # Simulated invoice with swapped values
        invoice_gst = correct["qst"]  # 9.98 (actually QST)
        invoice_qst = correct["gst"]  # 5.00 (actually GST)

        # The system has no function to detect this swap!
        # It only computes forward/reverse — it doesn't validate
        # individual GST/QST lines from external documents.
        # THIS IS A GAP: no cross-validation of extracted tax lines.
        assert invoice_gst != correct["gst"], "Test setup: values should be swapped"

    # ---- 3c  Province/tax code inconsistency ----
    def test_quebec_vendor_with_hst(self):
        """Quebec vendor should NOT use HST code."""
        result = validate_tax_code("5200 - Office Supplies", "HST", "QC")
        assert not result["valid"], "Quebec vendor with HST should be invalid"
        assert "province_qc_does_not_use_hst" in result["warnings"]

    def test_ontario_vendor_with_gst_qst(self):
        """Ontario vendor should NOT use GST_QST (should use HST)."""
        result = validate_tax_code("5200 - Office Supplies", "T", "ON")
        assert not result["valid"], "Ontario vendor with GST+QST should be invalid"

    def test_insurance_gl_with_taxable_code(self):
        """Insurance GL account with 'T' code should warn."""
        result = validate_tax_code("Insurance Expense", "T", "QC")
        assert not result["valid"]
        assert any("insurance" in w for w in result["warnings"])

    def test_meals_gl_with_full_taxable_code(self):
        """Meals GL account with 'T' (full recovery) should warn — should be 'M' (50%)."""
        result = validate_tax_code("Meals & Entertainment", "T", "QC")
        assert not result["valid"]
        assert any("meals" in w for w in result["warnings"])

    # ---- 3d  Subtotal + tax ≠ total ----
    def test_subtotal_plus_tax_equals_total(self):
        """Verify: pre_tax + gst + qst == total_with_tax (exact, no drift)."""
        for amount in [Decimal("0.01"), Decimal("1.00"), Decimal("100.00"),
                       Decimal("999.99"), Decimal("123456.78")]:
            result = calculate_gst_qst(amount)
            recomputed_total = result["amount_before_tax"] + result["gst"] + result["qst"]
            assert recomputed_total == result["total_with_tax"], (
                f"Sum mismatch for {amount}: "
                f"{result['amount_before_tax']}+{result['gst']}+{result['qst']} = "
                f"{recomputed_total} != {result['total_with_tax']}"
            )

    # ---- 3e  Quebec vendor missing QST but tax exists ----
    def test_quebec_vendor_missing_qst_detection(self):
        """Quebec vendor with tax_code='T' should have QST.
        If we compute tax and QST is $0, something is wrong."""
        result = calculate_gst_qst(Decimal("100.00"))
        assert result["qst"] > 0, "Quebec taxable amount should have QST > 0"

    # ---- 3f  Tax rate inconsistent across lines ----
    def test_tax_rate_consistency_across_line_items(self):
        """Multi-line invoice: line-level rounding can cause effective rate drift.
        This is inherent to cent-rounding on small amounts — we verify the
        drift stays within 1 cent per line."""
        lines = [Decimal("50.00"), Decimal("75.00"), Decimal("125.00")]
        for line_amount in lines:
            result = calculate_gst_qst(line_amount)
            # Ideal (unrounded) tax
            ideal_gst = line_amount * GST_RATE
            ideal_qst = line_amount * QST_RATE
            gst_drift = abs(result["gst"] - ideal_gst)
            qst_drift = abs(result["qst"] - ideal_qst)
            assert gst_drift <= CENT, f"GST drift {gst_drift} on {line_amount}"
            assert qst_drift <= CENT, f"QST drift {qst_drift} on {line_amount}"

    # ---- 3g  ITC/ITR recovery consistency ----
    def test_itc_itr_meals_50_percent(self):
        """Meals code 'M' should recover exactly 50% of GST and QST.
        itc_rate = gst_rate * itc_pct = 0.05 * 0.5 = 0.025 → rounded 0.03
        itr_rate = qst_rate * itr_pct = 0.09975 * 0.5 = 0.049875 → rounded 0.05"""
        result = calculate_itc_itr(Decimal("100.00"), "M")
        # Verify 50% recovery on the actual tax paid
        expected_gst_paid = _round(Decimal("100") * GST_RATE)
        expected_gst_recoverable = _round(expected_gst_paid * Decimal("0.5"))
        assert result["gst_recoverable"] == expected_gst_recoverable
        expected_qst_paid = _round(Decimal("100") * QST_RATE)
        expected_qst_recoverable = _round(expected_qst_paid * Decimal("0.5"))
        assert result["qst_recoverable"] == expected_qst_recoverable

    def test_exempt_no_recovery(self):
        """Exempt code 'E' should have zero recovery."""
        result = calculate_itc_itr(Decimal("100.00"), "E")
        assert result["total_recoverable"] == Decimal("0")

    def test_insurance_no_gst_recovery(self):
        """Insurance code 'I' — no GST (exempt), QST at 9% but non-recoverable."""
        result = calculate_itc_itr(Decimal("100.00"), "I")
        assert result["gst_recoverable"] == Decimal("0")
        assert result["qst_recoverable"] == Decimal("0")  # non-recoverable


# ============================================================================
# DIMENSION 4 — FALSE CONFIDENCE TEST
# ============================================================================

class TestFalseConfidence:
    """Detect cases where system expresses confidence without sufficient evidence."""

    # ---- 4a  GL entry only — no invoice, no bank ----
    def test_ready_status_requires_all_fields(self):
        """Document with vendor+amount+date → 'Ready'.
        But this is ONLY rules engine output — no bank match, no invoice verification."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="Some Vendor",
            total=100.00,
            document_date="2026-03-20",
            client_code="CLI001",
        )
        # FIX 3: base=0.90, boost=+0.10 → eff=1.0
        assert decision.status == "Ready"
        assert decision.effective_confidence == 1.0

    def test_rules_only_low_confidence_not_ready(self):
        """Rules-only extraction at 0.40 confidence should NOT be Ready."""
        decision = decide_review_status(
            rules_confidence=0.40,
            final_method="rules",
            vendor_name="Unknown Vendor",
            total=100.00,
            document_date="2026-03-20",
            client_code="CLI001",
        )
        assert decision.status == "NeedsReview", (
            f"Low confidence ({decision.effective_confidence}) returned {decision.status}"
        )

    # ---- 4b  Missing evidence cases ----
    def test_missing_vendor_is_exception(self):
        """No vendor → Exception status."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name=None,
            total=100.00,
            document_date="2026-03-20",
            client_code="CLI001",
        )
        assert decision.status == "Exception"
        assert decision.reason == "missing_vendor"

    def test_missing_total_needs_review(self):
        """No total amount → NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="Vendor",
            total=None,
            document_date="2026-03-20",
            client_code="CLI001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "missing_total"

    def test_missing_date_needs_review(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="Vendor",
            total=100.00,
            document_date=None,
            client_code="CLI001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "missing_document_date"

    def test_missing_client_needs_review(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="Vendor",
            total=100.00,
            document_date="2026-03-20",
            client_code=None,
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "missing_client_route"

    # ---- 4c  Zero amount suspicion ----
    def test_zero_amount_needs_review(self):
        """$0.00 total should be flagged for review."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="Vendor",
            total=0,
            document_date="2026-03-20",
            client_code="CLI001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "zero_total"

    # ---- 4d  Invalid date format not blocking ----
    def test_invalid_date_format_still_ready(self):
        """Invalid date format gets noted but doesn't block 'Ready' status.
        THIS IS A RISK: document proceeds with bad date."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="Vendor",
            total=100.00,
            document_date="March 20, 2026",  # invalid format
            client_code="CLI001",
        )
        # has_date is True (non-empty string), has_required is True
        assert decision.status == "Ready", (
            "Expected Ready (current behavior: invalid date format doesn't block)"
        )
        # Check that the invalid format is at least noted
        assert decision.review_notes is not None
        assert any("invalid_date" in n for n in decision.review_notes), (
            "Invalid date format should be noted in review_notes"
        )
        # RISK: "Ready" with invalid date means posting will proceed with bad date

    # ---- 4e  Whitespace-only vendor ----
    def test_whitespace_vendor_treated_as_missing(self):
        """Vendor = '   ' should be treated as missing → Exception."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules+ai",
            vendor_name="   ",
            total=100.00,
            document_date="2026-03-20",
            client_code="CLI001",
        )
        assert decision.status == "Exception"
        assert decision.reason == "missing_vendor"

    # ---- 4f  Confidence now respects actual quality (FIX 3) ----
    def test_confidence_respects_base_quality(self):
        """FIX 3+24: effective_confidence no longer forces 0.85.
        Boost is +0.05 for base < 0.80, +0.10 for base >= 0.80."""
        # High-quality extraction: base=0.95 >= 0.80, boost=+0.05, eff=1.0
        eff_good = effective_confidence(0.95, "rules+ai", True)
        # Low-quality extraction: base=0.40 < 0.80, boost=+0.05, eff=0.45
        eff_bad = effective_confidence(0.40, "rules+ai", True)

        assert eff_good == 1.0
        assert eff_bad == 0.45
        # FIX 3+24: Low confidence is no longer masked by having fields present.


# ============================================================================
# DIMENSION 5 — COMBINED CHAOS TESTS
# ============================================================================

class TestCombinedChaos:
    """Real-world messy scenarios combining multiple failure dimensions."""

    def setup_method(self):
        self.matcher = BankMatcher()

    # ---- 5a  French OCR + ambiguous amount + wrong tax + partial payment ----
    def test_french_ocr_chaos(self):
        """
        Scenario:
        - French invoice from 'Fournitures Québec Inc.' (TPS/TVQ)
        - OCR extracted amount as '1 234,56' (French format with space)
        - Invoice total includes TPS+TVQ
        - Bank payment is $1,100.00 CAD (partial payment)
        - Bank description: 'FOURNITURES QC'
        """
        # Parse the OCR amount
        ocr_amount = _to_float("1 234,56")
        assert ocr_amount == 1234.56, f"French OCR amount parsing failed: {ocr_amount}"

        # Verify tax extraction from this total
        tax_result = extract_tax_from_total(Decimal(str(ocr_amount)))
        assert tax_result["gst"] > 0
        assert tax_result["qst"] > 0

        # Bank matching — partial payment
        doc = _make_doc(
            vendor="Fournitures Québec Inc.",
            amount=ocr_amount,
            document_date="2026-03-15",
        )
        txn = _make_txn(
            amount=1100.00,
            posted_date="2026-03-15",
            description="FOURNITURES QC",
        )
        results = self.matcher.match_documents([doc], [txn])
        r = results[0]
        # Diff = $134.56 >> $5 tolerance → should not match
        assert r.status == "unmatched", (
            f"Partial payment chaos: matched with score={r.score}, diff={r.amount_diff}"
        )

    # ---- 5b  Duplicate vendor alias + same amount + cross-period ----
    def test_duplicate_vendor_cross_period_chaos(self):
        """
        - Two invoices from 'Bell Canada' and 'BELL CDA' (same vendor, different name)
        - Same amount: $89.95
        - One dated Feb 28, one dated March 1
        - Bank payment $89.95 on March 2 (description: 'BELL CANADA')
        """
        doc_feb = _make_doc(
            document_id="inv_bell_feb",
            vendor="Bell Canada",
            amount=89.95,
            document_date="2026-02-28",
        )
        doc_mar = _make_doc(
            document_id="inv_bell_mar",
            vendor="BELL CDA",
            amount=89.95,
            document_date="2026-03-01",
        )
        txn = _make_txn(
            amount=89.95,
            posted_date="2026-03-02",
            description="BELL CANADA",
        )
        results = self.matcher.match_documents([doc_feb, doc_mar], [txn])

        # FIX 4: Ambiguous candidates are now flagged instead of silently picked.
        ambiguous = [r for r in results if r.status == "ambiguous"]
        matched = [r for r in results if r.status in ("matched", "suggested")]

        # Either flagged as ambiguous (correct FIX 4 behavior) or at most one match
        if ambiguous:
            # FIX 4 working: system detected ambiguity
            assert any(
                "manual review required" in reason.lower() or
                "révision manuelle" in reason.lower()
                for r in ambiguous for reason in r.reasons
            ), "Ambiguous match should contain review note"
        else:
            assert len(matched) <= 1, "Both duplicate vendor invoices matched"

    # ---- 5c  OCR noise + wrong tax code + FX ----
    def test_ocr_fx_tax_chaos(self):
        """
        - US vendor invoice in USD
        - OCR extracted amount with noise: 'l,234.56' (lowercase L)
        - Tax code incorrectly set to 'T' (GST+QST) for a foreign vendor
        - Payment in CAD
        """
        # OCR noise should cause parse failure
        noisy_amount = _to_float("l,234.56")
        assert noisy_amount is None, (
            f"OCR noise should not parse: got {noisy_amount}"
        )

        # Tax validation: US vendor shouldn't have GST+QST
        tax_valid = validate_tax_code("5200 - Office Supplies", "T", "")
        # No province → no province-specific warning, but tax code is valid
        # RISK: foreign vendor with no province defaults to accepting T code
        # This should ideally flag "no_province_for_tax_validation"

        # The real risk: if OCR noise is silently dropped, amount becomes None,
        # and posting proceeds with NULL amount
        result = choose_bookkeeping_amount(
            vendor_name="US Corp",
            doc_type="invoice",
            total=noisy_amount,  # None from OCR failure
            notes="",
        )
        assert result.bookkeeping_amount is None
        assert result.amount_source == "missing"

    # ---- 5d  Maximum chaos: everything wrong at once ----
    def test_maximum_chaos_scenario(self):
        """
        All wrong simultaneously:
        - French invoice with OCR noise
        - Ambiguous comma-only amount
        - Wrong province/tax code combo
        - Duplicate vendor
        - Cross-period
        - Partial payment
        - Invalid date format
        """
        # 1. Amount parsing: "1,234" is ambiguous
        amount = _to_float("1,234")
        # Current behavior: 1.234 (treats comma as decimal)
        # Correct behavior: should be 1234.0 for bookkeeping
        # We document the KNOWN WRONG behavior:
        amount_is_wrong = (amount == 1.234)  # True — known issue

        # 2. Tax code validation
        tax_check = validate_tax_code("5200 - Office Supplies", "HST", "QC")
        assert not tax_check["valid"], "QC + HST should be invalid"

        # 3. Review policy with bad date
        decision = decide_review_status(
            rules_confidence=0.40,
            final_method="rules",
            vendor_name="Fournisseur XYZ",
            total=amount,  # 1.234 — wrong
            document_date="15/03/2026",  # DD/MM/YYYY — not ISO
            client_code="CLI001",
        )
        # With rules_confidence=0.40, this should NOT be Ready
        assert decision.status == "NeedsReview", (
            f"Maximum chaos scenario returned {decision.status} — "
            f"should need review at confidence {decision.effective_confidence}"
        )

        # 4. Document with all these problems fed to matcher
        doc = _make_doc(
            vendor="Fournisseur XYZ",
            amount=amount,  # 1.234 — wrong
            document_date="15/03/2026",
        )
        txn = _make_txn(
            amount=1234.00,  # what was actually paid
            posted_date="2026-04-01",  # cross-period
            description="FOURNISSEUR XYZ INC",
        )
        results = self.matcher.match_documents([doc], [txn])
        # Amount diff = |1.234 - 1234| = 1232.766 → WAY too far → unmatched
        assert results[0].status == "unmatched"
        # KEY INSIGHT: The amount parsing error cascades through the entire pipeline.
        # The match fails not because the system detected the error, but because
        # the WRONG amount is too far from the bank amount.
        # If the bank amount were also $1.23, it would MATCH — silently wrong.

    # ---- 5e  The quiet killer: wrong amount that happens to match ----
    def test_wrong_amount_that_still_matches(self):
        """
        MOST DANGEROUS SCENARIO:
        - OCR extracts '5,00' (French for $5.00)
        - _to_float returns 5.0 — correct!
        - But what if OCR extracted '5,000' (French for $5000)?
        - _to_float('5,000') → comma only, treated as decimal → 5.0
        - This matches a $5.00 bank transaction perfectly
        - SILENT WRONG MATCH
        """
        # French $5,000.00 written as "5,000"
        parsed = _to_float("5,000")
        # Current behavior: comma only → decimal → 5.0
        # Correct: "5,000" with 3 digits after comma should be 5000

        if parsed == 5.0:
            # This is the dangerous case — $5000 silently becomes $5
            doc = _make_doc(vendor="Test Corp", amount=parsed, document_date="2026-03-20")
            txn = _make_txn(amount=5.00, posted_date="2026-03-20", description="Test Corp")
            results = self.matcher.match_documents([doc], [txn])
            if results[0].status in ("matched", "suggested"):
                pytest.fail(
                    "CRITICAL: '5,000' (intended $5000) parsed as $5.00 and "
                    f"matched to a $5.00 bank transaction (score={results[0].score}). "
                    "This is a silent 1000x error with false match confirmation."
                )

    # ---- 5f  Confidence boosting masks bad extraction ----
    def test_confidence_boosting_masks_errors(self):
        """
        Rules engine extracts with 40% confidence (likely wrong).
        But all fields happen to be present.
        rules+ai mode boosts to 85% → 'Ready'.

        THIS IS FALSE CONFIDENCE.
        """
        decision = decide_review_status(
            rules_confidence=0.40,
            final_method="rules+ai",
            vendor_name="Probably Wrong Vendor",
            total=99999.99,  # suspiciously high
            document_date="2026-03-20",
            client_code="CLI001",
        )
        # Despite 40% rules confidence, all fields present → 85% → Ready
        if decision.status == "Ready":
            pytest.fail(
                f"CRITICAL: 40% rules confidence boosted to {decision.effective_confidence} "
                f"→ '{decision.status}'. Low-quality extraction marked as Ready. "
                "The confidence boost ignores actual extraction quality."
            )


# ============================================================================
# BONUS — Regression guard: matching edge cases
# ============================================================================

class TestMatchingEdgeCases:
    """Additional edge cases for the matching engine."""

    def setup_method(self):
        self.matcher = BankMatcher()

    def test_negative_amount_matching(self):
        """Credit note (negative) should match negative bank transaction."""
        doc = _make_doc(vendor="Refund Corp", amount=-500.00, document_date="2026-03-10")
        txn = _make_txn(amount=-500.00, posted_date="2026-03-10", description="Refund Corp")
        results = self.matcher.match_documents([doc], [txn])
        # amount_difference uses abs(abs(doc) - abs(txn)) = 0 → same_amount
        assert results[0].status in ("matched", "suggested")

    def test_very_small_amount(self):
        """$0.01 invoice vs $0.01 payment."""
        doc = _make_doc(vendor="Tiny Corp", amount=0.01, document_date="2026-03-10")
        txn = _make_txn(amount=0.01, posted_date="2026-03-10", description="Tiny Corp")
        results = self.matcher.match_documents([doc], [txn])
        assert results[0].status in ("matched", "suggested")

    def test_null_amount_no_crash(self):
        """Null amount on document should not crash matcher."""
        doc = _make_doc(vendor="Null Corp", amount=None, document_date="2026-03-10")
        txn = _make_txn(amount=100.00, posted_date="2026-03-10", description="Null Corp")
        results = self.matcher.match_documents([doc], [txn])
        assert results[0].status == "unmatched"

    def test_many_candidates_performance(self):
        """50 documents × 50 transactions = 2500 evaluations.  Should not crash."""
        docs = [
            _make_doc(
                document_id=f"doc_{i:03d}",
                vendor=f"Vendor {i}",
                amount=100.00 + i,
                document_date="2026-03-10",
            )
            for i in range(50)
        ]
        txns = [
            _make_txn(
                transaction_id=f"txn_{i:03d}",
                amount=100.00 + i,
                posted_date="2026-03-10",
                description=f"Vendor {i}",
            )
            for i in range(50)
        ]
        results = self.matcher.match_documents(docs, txns)
        assert len(results) == 50
        matched = sum(1 for r in results if r.status in ("matched", "suggested"))
        assert matched >= 40, f"Only {matched}/50 matched in 1:1 scenario"

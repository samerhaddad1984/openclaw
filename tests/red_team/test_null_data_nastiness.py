"""
RED-TEAM: Null-Data Nastiness — Missing Field Attacks
======================================================
Feed documents with critical fields missing:
  - vendor tax number (GST/QST registration)
  - invoice number
  - date
  - page 2 (incomplete multi-page extraction)
  - subtotal
  - currency

The system MUST:
  - Block clean posting via uncertainty engine
  - Surface visible evidence gaps (evidence_needed populated)
  - Never default to zeros or safe-looking guesses

Fail if zeros or guesses are silently inserted.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.uncertainty_engine import (
    evaluate_uncertainty,
    evaluate_posting_readiness,
    build_date_resolution,
    reason_tax_registration_incomplete,
    reason_invoice_number_ocr_conflict,
    reason_date_ambiguous,
    reason_allocation_gap,
    UncertaintyReason,
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    SAFE_TO_POST,
)
from src.agents.tools.review_policy import (
    decide_review_status,
    effective_confidence,
)


# ===================================================================
# A. MISSING VENDOR TAX NUMBER
# ===================================================================

class TestMissingVendorTaxNumber:
    """Vendor GST/QST registration number absent — cannot claim ITC/ITR."""

    def test_missing_tax_number_blocks_posting(self):
        """
        Invoice from vendor with no GST/QST number.
        Must block — you cannot claim input tax credits without it.
        """
        reason = reason_tax_registration_incomplete(vendor="Plomberie XYZ")
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "document_date": 0.92,
                "tax_registration": 0.0,  # completely missing
            },
            reasons=[reason],
        )
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_missing_tax_number_evidence_gap_visible(self):
        """The reason must explain exactly what evidence is needed."""
        reason = reason_tax_registration_incomplete(vendor="Plomberie XYZ")
        assert reason.reason_code == "TAX_REGISTRATION_INCOMPLETE"
        assert "GST" in reason.evidence_needed
        assert "QST" in reason.evidence_needed or "CRA" in reason.evidence_needed
        assert reason.evidence_available != ""

    def test_tax_number_not_defaulted_to_zero(self):
        """
        If tax_registration confidence is 0.0, the engine must NOT
        treat it as resolved. Zero confidence = missing data = block.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.95,
                "total": 0.95,
                "tax_registration": 0.0,
            },
        )
        assert state.must_block is True
        assert state.can_post is False
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_partial_tax_number_still_uncertain(self):
        """
        Tax number partially extracted (low confidence) — not good enough
        for clean posting.
        """
        reason = reason_tax_registration_incomplete(vendor="Services ABC")
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "tax_registration": 0.55,  # partial — below 0.60
            },
            reasons=[reason],
        )
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW


# ===================================================================
# B. MISSING INVOICE NUMBER
# ===================================================================

class TestMissingInvoiceNumber:
    """Invoice number absent or unreadable — duplicate detection impossible."""

    def test_missing_invoice_number_blocks_clean_posting(self):
        """
        No invoice number means duplicate detection cannot function.
        Must not allow SAFE_TO_POST.
        """
        reason = reason_invoice_number_ocr_conflict(raw_number="")
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "document_date": 0.92,
                "invoice_number": 0.0,  # completely missing
            },
            reasons=[reason],
        )
        assert state.can_post is False
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_missing_invoice_number_evidence_gap_visible(self):
        """The reason must articulate what's needed to resolve."""
        reason = reason_invoice_number_ocr_conflict(raw_number="")
        assert reason.reason_code == "INVOICE_NUMBER_OCR_CONFLICT"
        assert "No raw number" in reason.evidence_available
        assert reason.evidence_needed != ""

    def test_invoice_number_not_fabricated(self):
        """
        System must not invent an invoice number like "INV-0000" or
        default to empty string with high confidence.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "invoice_number": 0.0,
                "vendor_name": 0.85,
                "total": 0.90,
            },
        )
        # Zero confidence field must trigger block
        assert state.must_block is True
        assert state.posting_recommendation != SAFE_TO_POST

    def test_ambiguous_invoice_number_partial_post(self):
        """
        OCR read something but it's unclear (O vs 0, I vs 1).
        Partial post allowed, but not clean.
        """
        reason = reason_invoice_number_ocr_conflict(raw_number="INV-O1I0")
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "invoice_number": 0.65,  # uncertain but not missing
            },
            reasons=[reason],
        )
        assert state.can_post is False
        assert state.posting_recommendation in (
            PARTIAL_POST_WITH_FLAGS,
            BLOCK_PENDING_REVIEW,
        )


# ===================================================================
# C. MISSING DATE
# ===================================================================

class TestMissingDate:
    """No date extracted — period classification, FX rates, aging all break."""

    def test_missing_date_blocks_posting(self):
        """
        No date at all. Uncertainty engine must block.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "document_date": 0.0,  # completely missing
            },
        )
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_missing_date_review_policy_catches(self):
        """
        Review policy must independently flag missing date.
        """
        decision = decide_review_status(
            rules_confidence=0.80,
            final_method="ai",
            vendor_name="Fournisseur Test",
            total=1500.00,
            document_date=None,  # missing
            client_code="CLI001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "missing_document_date"

    def test_empty_date_not_treated_as_valid(self):
        """Empty string date must not pass as a valid date."""
        date_state = build_date_resolution("")
        assert date_state.resolved_date is None
        assert date_state.date_confidence == 0.0

    def test_null_date_propagates_zero_confidence(self):
        """
        Date confidence 0.0 must cascade: no FX rate, no aging,
        no period classification possible.
        """
        date_state = build_date_resolution("")
        assert date_state.date_confidence == 0.0
        assert date_state.resolved_date is None

    def test_date_not_defaulted_to_today(self):
        """
        System must NEVER silently default missing date to today's date.
        The date_resolution must remain None/unresolved.
        """
        date_state = build_date_resolution("")
        assert date_state.resolved_date is None
        # Also check whitespace-only
        date_state2 = build_date_resolution("   ")
        assert date_state2.resolved_date is None
        assert date_state2.date_confidence == 0.0


# ===================================================================
# D. MISSING PAGE 2 (INCOMPLETE EXTRACTION)
# ===================================================================

class TestMissingPage2:
    """Multi-page document with page 2 not extracted — totals won't reconcile."""

    def test_partial_extraction_low_confidence_blocks(self):
        """
        Only page 1 of 3 extracted. Overall confidence must be low enough
        to trigger blocking.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.85,
                "total": 0.30,       # total from last page, but lines don't add up
                "subtotal": 0.20,    # partial lines only
                "document_date": 0.85,
            },
        )
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_allocation_gap_from_missing_page(self):
        """
        Page 1 items sum to $500 but total says $2,350.
        Allocation gap reason must be raised with evidence.
        """
        reason = reason_allocation_gap(
            invoice_total="$2,350.00",
            documented_value="$500.00",
        )
        assert reason.reason_code == "ALLOCATION_GAP_UNEXPLAINED"
        assert "$2,350" in reason.evidence_available
        assert "$500" in reason.evidence_available
        assert reason.evidence_needed != ""

    def test_missing_page_not_filled_with_zeros(self):
        """
        Missing line items from page 2 must NOT be defaulted to $0.00.
        The gap must remain unexplained.
        """
        reason = reason_allocation_gap(
            invoice_total="$5,000.00",
            documented_value="$1,200.00",
        )
        state = evaluate_uncertainty(
            confidence_by_field={
                "subtotal": 0.25,
                "total": 0.40,
                "vendor_name": 0.90,
            },
            reasons=[reason],
        )
        assert state.must_block is True
        assert len(state.unresolved_reasons) >= 1
        assert state.unresolved_reasons[0].reason_code == "ALLOCATION_GAP_UNEXPLAINED"


# ===================================================================
# E. MISSING SUBTOTAL
# ===================================================================

class TestMissingSubtotal:
    """No subtotal extracted — tax math cannot be verified."""

    def test_missing_subtotal_prevents_safe_posting(self):
        """
        Without subtotal, GST/QST calculation cannot be verified.
        Must not allow SAFE_TO_POST.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.85,
                "document_date": 0.90,
                "subtotal": 0.0,  # completely missing
            },
        )
        assert state.must_block is True
        assert state.can_post is False

    def test_zero_subtotal_not_assumed_valid(self):
        """
        A $0.00 subtotal is suspicious — could be extraction failure.
        Review policy flags zero totals.
        """
        decision = decide_review_status(
            rules_confidence=0.80,
            final_method="ai",
            vendor_name="Test Vendor",
            total=0,  # zero
            document_date="2026-01-15",
            client_code="CLI001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "zero_total"

    def test_subtotal_not_invented_from_total(self):
        """
        System must not compute subtotal by reverse-engineering total.
        If subtotal is absent, it's absent — confidence must reflect that.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "subtotal": 0.0,
                "total": 0.90,
            },
        )
        assert state.must_block is True
        # The system must not upgrade subtotal confidence
        assert state.confidence_by_field["subtotal"] == 0.0


# ===================================================================
# F. MISSING CURRENCY
# ===================================================================

class TestMissingCurrency:
    """No currency extracted — FX conversion, tax regime all uncertain."""

    def test_missing_currency_blocks_clean_posting(self):
        """
        Currency unknown means FX rate selection is impossible.
        Cannot post cleanly.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "document_date": 0.92,
                "currency": 0.0,  # completely missing
            },
        )
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_currency_not_defaulted_to_cad(self):
        """
        System must NEVER silently default currency to CAD.
        Zero confidence on currency = unresolved.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "currency": 0.0,
                "total": 0.95,
            },
        )
        assert state.must_block is True
        assert state.confidence_by_field["currency"] == 0.0
        assert state.posting_recommendation != SAFE_TO_POST

    def test_ambiguous_currency_partial_only(self):
        """
        Currency partially recognized (USD or CAD unclear).
        Partial post at best, never clean.
        """
        reason = UncertaintyReason(
            reason_code="FX_RATE_DATE_AMBIGUOUS",
            description_fr="Devise non confirmée — le taux de change ne peut être déterminé",
            description_en="Currency unconfirmed — FX rate cannot be determined",
            evidence_available="Document mentions '$' symbol without country prefix",
            evidence_needed="Vendor confirmation of invoicing currency or bank statement showing payment currency",
        )
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.90,
                "total": 0.95,
                "currency": 0.65,  # ambiguous
            },
            reasons=[reason],
        )
        assert state.can_post is False
        assert state.posting_recommendation in (
            PARTIAL_POST_WITH_FLAGS,
            BLOCK_PENDING_REVIEW,
        )


# ===================================================================
# G. COMBINED NULL-DATA: EVERYTHING MISSING
# ===================================================================

class TestEverythingMissing:
    """Document where almost nothing was extracted — maximum null nastiness."""

    def test_all_fields_null_blocks_completely(self):
        """
        Document with all critical fields at zero confidence.
        Must block with multiple evidence gaps visible.
        """
        reasons = [
            reason_tax_registration_incomplete(vendor=""),
            reason_invoice_number_ocr_conflict(raw_number=""),
            reason_date_ambiguous(raw_date="", date_range=[]),
        ]
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.0,
                "total": 0.0,
                "document_date": 0.0,
                "invoice_number": 0.0,
                "tax_registration": 0.0,
                "currency": 0.0,
                "subtotal": 0.0,
            },
            reasons=reasons,
        )
        assert state.must_block is True
        assert state.can_post is False
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW
        assert len(state.unresolved_reasons) == 3

    def test_all_null_evidence_gaps_are_visible(self):
        """
        Every unresolved reason must have non-empty evidence_needed
        so reviewers know exactly what to obtain.
        """
        reasons = [
            reason_tax_registration_incomplete(vendor=""),
            reason_invoice_number_ocr_conflict(raw_number=""),
            reason_date_ambiguous(raw_date="", date_range=[]),
            reason_allocation_gap(invoice_total="", documented_value=""),
        ]
        state = evaluate_uncertainty(
            confidence_by_field={"vendor_name": 0.0},
            reasons=reasons,
        )
        for reason in state.unresolved_reasons:
            assert reason.evidence_needed != "", (
                f"Reason {reason.reason_code} must specify evidence_needed"
            )
            assert reason.description_en != "", (
                f"Reason {reason.reason_code} must have English description"
            )
            assert reason.description_fr != "", (
                f"Reason {reason.reason_code} must have French description"
            )

    def test_posting_decision_shows_all_blocked_fields(self):
        """
        PostingDecision must list every blocked field with its
        confidence and status, so nothing is hidden.
        """
        state = evaluate_uncertainty(
            confidence_by_field={
                "vendor_name": 0.0,
                "total": 0.0,
                "document_date": 0.0,
                "currency": 0.0,
            },
        )
        decision = evaluate_posting_readiness(
            document={"id": "test-null-doc"},
            uncertainty_state=state,
        )
        assert decision.outcome == BLOCK_PENDING_REVIEW
        assert decision.can_post is False
        # Every zero-confidence field must appear in blocked_fields
        blocked_field_names = [bf["field"] for bf in decision.blocked_fields]
        for field_name in ["vendor_name", "total", "document_date", "currency"]:
            assert field_name in blocked_field_names, (
                f"Blocked field '{field_name}' must be visible in PostingDecision"
            )

    def test_review_policy_rejects_all_nulls(self):
        """
        Review policy with all critical fields None must return
        NeedsReview or Exception — never Ready.
        """
        decision = decide_review_status(
            rules_confidence=0.0,
            final_method="none",
            vendor_name=None,
            total=None,
            document_date=None,
            client_code=None,
        )
        assert decision.status in ("NeedsReview", "Exception")
        assert decision.effective_confidence < 0.85

    def test_empty_confidence_dict_blocks(self):
        """
        Empty confidence_by_field dict must block — cannot evaluate
        what doesn't exist.
        """
        state = evaluate_uncertainty(confidence_by_field={})
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_no_silent_zero_insertion(self):
        """
        CRITICAL: When fields are missing, the confidence dict must
        preserve the zeros — not silently upgrade them to passing values.
        """
        input_confidences = {
            "vendor_name": 0.0,
            "total": 0.0,
            "subtotal": 0.0,
            "currency": 0.0,
        }
        state = evaluate_uncertainty(
            confidence_by_field=input_confidences,
        )
        # Verify no confidence was silently upgraded
        for field_name, original_conf in input_confidences.items():
            assert state.confidence_by_field[field_name] == original_conf, (
                f"Field '{field_name}' confidence was silently changed from "
                f"{original_conf} to {state.confidence_by_field[field_name]}"
            )

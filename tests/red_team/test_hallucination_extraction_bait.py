"""
RED-TEAM: Hallucination Guard — Extraction Bait Attacks
========================================================
Feed AI-extraction bait that humans would catch but AI might not:
  - handwritten numbers conflicting with typed totals
  - fake tax box in image margin
  - subtotal > total (impossible math)
  - negative amount presented as positive
  - missing page 2 (incomplete extraction)

Fail if fabricated fields pass as structured truth.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.core.hallucination_guard import (
    verify_numeric_totals,
    verify_ai_output,
    build_structured_review_reasons,
    MATH_TOLERANCE,
    CONFIDENCE_THRESHOLD,
)


# ===================================================================
# A. HANDWRITTEN vs TYPED CONFLICT
# ===================================================================

class TestHandwrittenTypedConflict:
    """Handwritten amounts should never silently override typed totals."""

    def test_handwritten_total_contradicts_typed_subtotal(self):
        """
        Scenario: typed subtotal = $1,234.56, typed taxes = $184.27,
        but a handwritten scrawl says total = $1,500.00.
        The math check MUST catch the contradiction.
        """
        result = {
            "subtotal": 1234.56,
            "total": 1500.00,  # handwritten — wrong
            "taxes": [
                {"type": "GST", "amount": 61.73},
                {"type": "QST", "amount": 122.54},
            ],
        }
        r = verify_numeric_totals(result)
        assert r["ok"] is False, (
            "Handwritten total ($1,500) contradicts typed subtotal+tax ($1,418.83) "
            "— must be flagged"
        )
        assert r["delta"] > MATH_TOLERANCE

    def test_handwritten_subtotal_contradicts_typed_total(self):
        """
        Scenario: handwritten subtotal = $900.00, typed total = $1,149.80,
        taxes = GST $50 + QST $99.80.  Real subtotal should be ~$1,000.
        """
        result = {
            "subtotal": 900.00,  # handwritten — wrong
            "total": 1149.80,
            "taxes": [
                {"type": "GST", "amount": 50.00},
                {"type": "QST", "amount": 99.80},
            ],
        }
        r = verify_numeric_totals(result)
        assert r["ok"] is False
        assert r["delta"] >= 100  # massive contradiction

    def test_low_confidence_handwriting_flagged(self):
        """
        Handwriting extraction should report low confidence,
        which the AI output guard should flag.
        """
        result = {
            "vendor_name": "Plomberie Tremblay",
            "total": 475.00,
            "document_date": "2025-06-15",
            "confidence": 0.45,  # low — handwriting uncertainty
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True
        assert any("confidence" in f for f in r["failures"])
        assert r["review_status"] == "NeedsReview"

    def test_ocr_digit_swap_in_total(self):
        """
        OCR reads handwritten 8 as 3.  subtotal=$183.00 but
        total should be $188.00 (taxes $5.00).
        """
        result = {
            "subtotal": 183.00,
            "total": 133.00,  # OCR misread 8→3
            "tax_total": 5.00,
        }
        r = verify_numeric_totals(result)
        assert r["ok"] is False
        assert r["delta"] > 50

    def test_provenance_fields_present_on_mismatch(self):
        """
        When math fails, the result MUST include both computed and
        claimed_total so reviewers see raw vs corrected values.
        """
        result = {
            "subtotal": 200.00,
            "total": 250.00,  # wrong
            "tax_total": 29.97,
        }
        r = verify_numeric_totals(result)
        assert r["ok"] is False
        assert "computed" in r, "Must expose computed value for provenance"
        assert "claimed_total" in r, "Must expose claimed total for provenance"
        assert r["computed"] == 229.97  # subtotal + tax_total
        assert r["claimed_total"] == 250.00


# ===================================================================
# B. FAKE TAX BOX IN MARGIN
# ===================================================================

class TestFakeTaxBox:
    """
    A fake tax summary box inserted in the margin of a document.
    The extraction may pick up contradictory tax amounts.
    """

    def test_duplicate_gst_entries_inflates_total(self):
        """
        Fake margin box duplicates GST.  Two GST entries should
        sum, causing a mismatch with the real total.
        """
        result = {
            "subtotal": 500.00,
            "total": 574.88,  # correct: 500 + 25 + 49.88
            "taxes": [
                {"type": "GST", "amount": 25.00},   # real
                {"type": "GST", "amount": 25.00},   # fake duplicate from margin
                {"type": "QST", "amount": 49.88},
            ],
        }
        r = verify_numeric_totals(result)
        # computed = 500 + 25 + 25 + 49.88 = 599.88, total = 574.88
        assert r["ok"] is False, (
            "Duplicate GST from fake margin box must cause mismatch"
        )
        assert r["delta"] >= 25.00

    def test_phantom_hst_alongside_gst_qst(self):
        """
        Fake box adds HST alongside GST+QST — impossible in Quebec.
        The math check catches the inflated sum.
        """
        result = {
            "subtotal": 1000.00,
            "total": 1149.75,  # correct GST+QST total
            "taxes": [
                {"type": "GST", "amount": 50.00},
                {"type": "QST", "amount": 99.75},
                {"type": "HST", "amount": 130.00},  # impossible in QC
            ],
        }
        r = verify_numeric_totals(result)
        assert r["ok"] is False, (
            "Phantom HST alongside GST/QST must cause mismatch"
        )

    def test_fake_tax_code_rejected(self):
        """Margin box invents a tax code like 'PST' or 'TVP'."""
        result = {
            "vendor_name": "Test Corp",
            "total": 100.00,
            "tax_code": "TVP",  # fabricated
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True
        assert any("tax_code" in f for f in r["failures"])

    def test_unrecognised_tax_type_ignored_in_sum(self):
        """
        A tax entry with type='FAKE' should NOT be included in the sum.
        This means the total may still check out if only real taxes match.
        """
        result = {
            "subtotal": 100.00,
            "total": 114.98,
            "taxes": [
                {"type": "GST", "amount": 5.00},
                {"type": "QST", "amount": 9.98},
                {"type": "FAKE_TAX", "amount": 50.00},  # ignored
            ],
        }
        r = verify_numeric_totals(result)
        # Only GST+QST are summed (FAKE_TAX is not in recognised set)
        assert r["ok"] is True, (
            "Unrecognised tax types are correctly excluded from sum"
        )


# ===================================================================
# C. SUBTOTAL > TOTAL (IMPOSSIBLE MATH)
# ===================================================================

class TestSubtotalGreaterThanTotal:
    """Subtotal exceeding total is physically impossible for standard invoices."""

    def test_subtotal_exceeds_total_flagged(self):
        """Subtotal $500 > Total $450 — taxes can't be negative on a normal invoice."""
        result = {
            "subtotal": 500.00,
            "total": 450.00,
            "taxes": [
                {"type": "GST", "amount": 25.00},
                {"type": "QST", "amount": 49.88},
            ],
        }
        r = verify_numeric_totals(result)
        # computed = 500 + 25 + 49.88 = 574.88, total = 450
        assert r["ok"] is False
        assert r["delta"] > 100

    def test_subtotal_slightly_above_total(self):
        """Subtotal $100.05, total $100.00, tax $0 — small but real contradiction."""
        result = {
            "subtotal": 100.05,
            "total": 100.00,
            "tax_total": 0.00,
        }
        r = verify_numeric_totals(result)
        # computed = 100.05, total = 100.00, delta = 0.05
        assert r["ok"] is False, (
            "Subtotal > total even by $0.05 must be flagged (exceeds $0.02 tolerance)"
        )

    def test_zero_tax_subtotal_equals_total(self):
        """Tax-exempt invoice: subtotal should equal total exactly."""
        result = {
            "subtotal": 250.00,
            "total": 250.00,
            "tax_total": 0.00,
        }
        r = verify_numeric_totals(result)
        assert r["ok"] is True

    def test_inverted_subtotal_total(self):
        """
        OCR swaps subtotal and total fields.
        subtotal = $574.88 (really the total), total = $500.00 (really subtotal).
        """
        result = {
            "subtotal": 574.88,  # swapped
            "total": 500.00,     # swapped
            "taxes": [
                {"type": "GST", "amount": 25.00},
                {"type": "QST", "amount": 49.88},
            ],
        }
        r = verify_numeric_totals(result)
        # computed = 574.88 + 25 + 49.88 = 649.76, total = 500
        assert r["ok"] is False
        assert r["delta"] > 100


# ===================================================================
# D. NEGATIVE AMOUNT PRESENTED AS POSITIVE
# ===================================================================

class TestNegativeAsPositive:
    """
    A credit note or refund with negative amount that gets OCR'd
    as positive — or vice versa.
    """

    def test_credit_note_positive_amount_flagged(self):
        """
        doc_type=credit_note but amount is positive.
        verify_ai_output allows negative for credit_note but should
        at minimum not auto-approve suspicious positives.
        The hallucination guard does not currently block this,
        but the amount range check still applies.
        """
        result = {
            "vendor_name": "Refund Corp",
            "total": 5000.00,
            "document_type": "credit_note",
            "confidence": 0.65,  # low confidence — should flag
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True, (
            "Low-confidence credit note with positive amount must be flagged"
        )

    def test_negative_invoice_flagged(self):
        """Regular invoice with negative amount — always suspicious."""
        result = {
            "vendor_name": "Normal Vendor Inc",
            "total": -350.00,
            "document_type": "invoice",
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True, (
            "Negative amount on a regular invoice must be flagged"
        )

    def test_negative_subtotal_positive_total_mismatch(self):
        """Subtotal is negative but total is positive — impossible."""
        result = {
            "subtotal": -200.00,
            "total": 229.97,
            "tax_total": 29.97,
        }
        r = verify_numeric_totals(result)
        # computed = -200 + 29.97 = -170.03, total = 229.97
        assert r["ok"] is False
        assert r["delta"] > 300

    def test_sign_flip_on_tax(self):
        """Tax amount is negative on a regular invoice — corrupts the math."""
        result = {
            "subtotal": 1000.00,
            "total": 1149.75,
            "taxes": [
                {"type": "GST", "amount": -50.00},  # sign flipped
                {"type": "QST", "amount": 99.75},
            ],
        }
        r = verify_numeric_totals(result)
        # computed = 1000 + (-50) + 99.75 = 1049.75, total = 1149.75
        assert r["ok"] is False
        assert r["delta"] == 100.0


# ===================================================================
# E. MISSING PAGE 2 (INCOMPLETE EXTRACTION)
# ===================================================================

class TestMissingPage:
    """
    Multi-page document where page 2 was not extracted.
    Line items on page 1 won't sum to the total on the last page.
    """

    def test_partial_extraction_math_fails(self):
        """
        Page 1 subtotal = $500 (partial), but total from last page = $2,350.
        The math MUST flag this.
        """
        result = {
            "subtotal": 500.00,       # only page 1 items
            "total": 2350.00,         # from last page
            "tax_total": 350.63,
        }
        r = verify_numeric_totals(result)
        # computed = 500 + 350.63 = 850.63, total = 2350
        assert r["ok"] is False
        assert r["delta"] > 1000, "Missing page causes massive gap"

    def test_missing_page_low_confidence_flagged(self):
        """
        Extraction with suspiciously few fields and low confidence
        should be flagged even if math can't run.
        """
        result = {
            "vendor_name": "Big Supplier Inc",
            "total": 15000.00,
            # No subtotal, no taxes — can't verify math
            "confidence": 0.35,
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True
        assert any("confidence" in f for f in r["failures"])

    def test_missing_page_skips_math_silently(self):
        """
        KNOWN LIMITATION: If only total is present (no subtotal),
        math check is skipped.  This is a real risk for missing-page
        scenarios — document the gap.
        """
        result = {
            "total": 5000.00,
            # no subtotal, no taxes
        }
        r = verify_numeric_totals(result)
        assert r["skipped"] is True, (
            "Math check is skipped when subtotal is missing — "
            "relies on confidence guard as fallback"
        )

    def test_page_count_mismatch_via_low_confidence(self):
        """
        Extractor reports confidence below threshold because it detected
        'Page 1 of 3' but only got 1 page.
        """
        result = {
            "vendor_name": "Multi-Page Vendor",
            "total": 8500.00,
            "document_date": "2025-09-01",
            "confidence": 0.40,  # extractor knows it's incomplete
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True
        assert r["review_status"] == "NeedsReview"


# ===================================================================
# F. PROVENANCE: RAW vs CORRECTED
# ===================================================================

class TestProvenance:
    """
    The system must expose raw extracted values alongside any
    corrected values so reviewers can see what was changed.
    """

    def test_numeric_totals_exposes_both_values(self):
        """verify_numeric_totals must return computed AND claimed_total."""
        result = {
            "subtotal": 100.00,
            "total": 120.00,
            "tax_total": 14.98,
        }
        r = verify_numeric_totals(result)
        assert "computed" in r
        assert "claimed_total" in r
        assert r["computed"] == 114.98
        assert r["claimed_total"] == 120.00

    def test_ai_output_lists_all_failures(self):
        """Every failed check must appear in the failures list."""
        result = {
            "vendor_name": "",
            "total": -999999,
            "document_date": "not-a-date",
            "gl_account": "!@#$%",
            "tax_code": "BOGUS",
            "confidence": 0.1,
        }
        r = verify_ai_output(result)
        assert len(r["failures"]) >= 5, (
            "Each bad field must produce a separate failure entry"
        )
        # Check each category is represented
        failure_text = " ".join(r["failures"])
        assert "vendor" in failure_text.lower()
        assert "amount" in failure_text.lower()
        assert "date" in failure_text.lower() or "document_date" in failure_text.lower()
        assert "gl_account" in failure_text.lower()
        assert "tax_code" in failure_text.lower()

    def test_structured_review_reasons_have_bilingual_descriptions(self):
        """Review reasons must include both EN and FR descriptions."""
        reasons = build_structured_review_reasons(
            ["VENDOR_NAME_CONFLICT", "DATE_AMBIGUOUS"],
            {"VENDOR_NAME_CONFLICT": "OCR read 'Stapies' vs 'Staples'"},
        )
        assert len(reasons) == 2
        for r in reasons:
            assert "description_en" in r
            assert "description_fr" in r
            assert "reason_code" in r
        assert reasons[0]["evidence"] == "OCR read 'Stapies' vs 'Staples'"

    def test_delta_precision(self):
        """Delta must be rounded to 4 decimal places for auditability."""
        result = {
            "subtotal": 99.99,
            "total": 114.97,
            "tax_total": 14.975,  # half-cent
        }
        r = verify_numeric_totals(result)
        # computed = 99.99 + 14.975 = 114.965
        assert isinstance(r["delta"], float)
        # delta string should have at most 4 decimal places
        delta_str = f"{r['delta']:.4f}"
        assert r["delta"] == float(delta_str)


# ===================================================================
# G. COMBINED BAIT ATTACKS
# ===================================================================

class TestCombinedBait:
    """Multi-vector attacks combining several bait types."""

    def test_handwritten_override_plus_fake_tax(self):
        """
        Handwritten total + phantom tax entry.
        Both the math check and field validation should catch issues.
        """
        result = {
            "subtotal": 1000.00,
            "total": 1500.00,  # handwritten — inflated
            "taxes": [
                {"type": "GST", "amount": 50.00},
                {"type": "QST", "amount": 99.75},
                {"type": "HST", "amount": 130.00},  # phantom
            ],
        }
        math_r = verify_numeric_totals(result)
        # computed = 1000 + 50 + 99.75 + 130 = 1279.75 vs total 1500
        assert math_r["ok"] is False

    def test_missing_page_plus_negative_flip(self):
        """
        Partial extraction + sign error.
        Only page 1 extracted, and amount sign flipped.
        """
        result = {
            "vendor_name": "Refund Services",
            "total": 3500.00,  # should be -3500 (refund)
            "document_type": "invoice",  # misclassified
            "confidence": 0.30,
        }
        r = verify_ai_output(result)
        assert r["hallucination_suspected"] is True

    def test_all_bait_vectors_at_once(self):
        """
        Maximum adversarial input: every bait vector present.
        The system MUST flag this — no fabricated field should
        pass as structured truth.
        """
        result = {
            "vendor_name": "XKJLMNTPRW",  # OCR garbage
            "subtotal": 800.00,
            "total": 500.00,       # subtotal > total (impossible)
            "taxes": [
                {"type": "GST", "amount": -25.00},   # sign flip
                {"type": "QST", "amount": 49.88},
                {"type": "HST", "amount": 65.00},     # phantom
            ],
            "document_date": "2099-01-01",  # future date
            "tax_code": "FAKE",
            "gl_account": "!!invalid!!",
            "confidence": 0.15,
        }
        math_r = verify_numeric_totals(result)
        field_r = verify_ai_output(result)

        assert math_r["ok"] is False, "Math must fail"
        assert field_r["hallucination_suspected"] is True, "Fields must be flagged"
        assert len(field_r["failures"]) >= 4, (
            "Multiple failures must be reported — fabricated fields "
            "must NEVER pass as structured truth"
        )

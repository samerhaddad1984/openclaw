"""
tests/test_handwriting_ocr.py
==============================
Pytest tests for the handwriting OCR pipeline:
  - detect_handwriting scoring
  - Quebec amount parsing (_fix_quebec_amount)
  - Quebec date parsing (_fix_quebec_date)
  - Illegible field handling
  - Confidence boosting (math validation, vendor cross-ref)
  - Side-by-side UI trigger logic
  - Post-processing pipeline
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.engines.ocr_engine import (
    _fix_quebec_amount,
    _fix_quebec_date,
    _post_process_handwriting,
    detect_handwriting,
    HANDWRITING_HIGH_THRESHOLD,
    HANDWRITING_LOW_THRESHOLD,
)


# ---------------------------------------------------------------------------
# detect_handwriting scoring
# ---------------------------------------------------------------------------

class TestDetectHandwriting:
    """Tests for the detect_handwriting() heuristic scorer."""

    def test_empty_bytes_scores_high(self):
        """Empty input has no extractable text → score >= 0.4."""
        score = detect_handwriting(b"")
        assert score >= 0.4

    def test_minimal_bytes_scores_high(self):
        """Random non-document bytes produce no words → high score."""
        score = detect_handwriting(b"\x00" * 100)
        assert score >= 0.4

    def test_score_range(self):
        """Score must always be in [0.0, 1.0]."""
        score = detect_handwriting(b"not a real document")
        assert 0.0 <= score <= 1.0

    def test_score_not_negative(self):
        score = detect_handwriting(b"")
        assert score >= 0.0


# ---------------------------------------------------------------------------
# Quebec amount parsing
# ---------------------------------------------------------------------------

class TestFixQuebecAmount:
    """Tests for _fix_quebec_amount() — Quebec format normalisation."""

    def test_comma_decimal(self):
        assert _fix_quebec_amount("14,50") == 14.50

    def test_comma_decimal_trailing_dollar(self):
        assert _fix_quebec_amount("14,50$") == 14.50

    def test_leading_dollar_dot_decimal(self):
        assert _fix_quebec_amount("$14.50") == 14.50

    def test_dot_decimal_trailing_dollar(self):
        assert _fix_quebec_amount("14.50$") == 14.50

    def test_space_thousands(self):
        assert _fix_quebec_amount("1 234,50") == 1234.50

    def test_space_thousands_dollar(self):
        assert _fix_quebec_amount("1 234,50$") == 1234.50

    def test_plain_number(self):
        assert _fix_quebec_amount("99.99") == 99.99

    def test_integer(self):
        assert _fix_quebec_amount("100") == 100.00

    def test_none_returns_none(self):
        assert _fix_quebec_amount(None) is None

    def test_illegible_returns_none(self):
        assert _fix_quebec_amount("illegible") is None

    def test_empty_string_returns_none(self):
        assert _fix_quebec_amount("") is None

    def test_numeric_passthrough(self):
        assert _fix_quebec_amount(42.50) == 42.50

    def test_zero(self):
        assert _fix_quebec_amount("0,00") == 0.00

    def test_large_amount(self):
        assert _fix_quebec_amount("12 345,67$") == 12345.67


# ---------------------------------------------------------------------------
# Quebec date parsing
# ---------------------------------------------------------------------------

class TestFixQuebecDate:
    """Tests for _fix_quebec_date() — Quebec format normalisation."""

    def test_iso_format(self):
        assert _fix_quebec_date("2026-03-19") == "2026-03-19"

    def test_dd_mm_yy_slash(self):
        assert _fix_quebec_date("19/03/26") == "2026-03-19"

    def test_dd_mm_yyyy_dash(self):
        assert _fix_quebec_date("19-03-2026") == "2026-03-19"

    def test_dd_mm_yyyy_slash(self):
        assert _fix_quebec_date("19/03/2026") == "2026-03-19"

    def test_french_month_day_first(self):
        assert _fix_quebec_date("19 mars 2026") == "2026-03-19"

    def test_french_month_name_first(self):
        assert _fix_quebec_date("mars 19 2026") == "2026-03-19"

    def test_french_month_abbreviated(self):
        assert _fix_quebec_date("19 fev 2026") == "2026-02-19"

    def test_french_month_accented(self):
        assert _fix_quebec_date("19 février 2026") == "2026-02-19"

    def test_french_december(self):
        assert _fix_quebec_date("25 décembre 2025") == "2025-12-25"

    def test_none_returns_none(self):
        assert _fix_quebec_date(None) is None

    def test_illegible_returns_none(self):
        assert _fix_quebec_date("illegible") is None

    def test_empty_returns_none(self):
        assert _fix_quebec_date("") is None

    def test_two_digit_year(self):
        assert _fix_quebec_date("01/06/25") == "2025-06-01"


# ---------------------------------------------------------------------------
# Illegible field handling
# ---------------------------------------------------------------------------

class TestIllegibleFieldHandling:
    """Tests that illegible fields are set to None with review notes."""

    def test_illegible_vendor_becomes_none(self):
        result = _post_process_handwriting({
            "vendor_name": "illegible",
            "amount": "25,50$",
            "date": "19 mars 2026",
            "gst_amount": "1,28",
            "qst_amount": "2,54",
            "total": "29,32",
            "payment_method": "cash",
            "confidence": 0.5,
        })
        assert result["vendor_name"] is None
        assert "vendor_name" in result.get("review_notes", "")

    def test_illegible_date_becomes_none(self):
        result = _post_process_handwriting({
            "vendor_name": "Test Vendor",
            "amount": "10.00",
            "date": "illegible",
            "gst_amount": "0.50",
            "qst_amount": "1.00",
            "total": "11.50",
            "payment_method": "debit",
            "confidence": 0.6,
        })
        assert result["document_date"] is None

    def test_multiple_illegible_fields(self):
        result = _post_process_handwriting({
            "vendor_name": "illegible",
            "amount": "illegible",
            "date": "illegible",
            "gst_amount": None,
            "qst_amount": None,
            "total": "illegible",
            "payment_method": "illegible",
            "confidence": 0.3,
        })
        assert result["vendor_name"] is None
        assert result["amount"] is None
        assert result["total"] is None
        assert result.get("handwriting_low_confidence") is True

    def test_no_illegible_no_review_notes(self):
        result = _post_process_handwriting({
            "vendor_name": "Café Montréal",
            "amount": "15.00",
            "date": "2026-01-15",
            "gst_amount": "0.75",
            "qst_amount": "1.50",
            "total": "17.25",
            "payment_method": "cash",
            "confidence": 0.85,
        })
        assert "review_notes" not in result or result["review_notes"] == ""


# ---------------------------------------------------------------------------
# Confidence boosting
# ---------------------------------------------------------------------------

class TestConfidenceBoosting:
    """Tests for math validation and confidence adjustments."""

    def test_math_validation_boost(self):
        """When subtotal + gst + qst ≈ total, confidence gets +0.10."""
        result = _post_process_handwriting({
            "vendor_name": "Test Vendor",
            "amount": "100.00",
            "subtotal": "100.00",
            "date": "2026-03-19",
            "gst_amount": "5.00",
            "qst_amount": "9.98",
            "total": "114.98",
            "payment_method": "cash",
            "confidence": 0.60,
        })
        # 100 + 5 + 9.98 = 114.98 — exact match → +0.10
        assert result["confidence"] >= 0.70
        assert result.get("math_validated") is True

    def test_no_boost_when_math_fails(self):
        """When math doesn't check out, no boost applied."""
        result = _post_process_handwriting({
            "vendor_name": "Test Vendor",
            "amount": "100.00",
            "subtotal": "100.00",
            "date": "2026-03-19",
            "gst_amount": "5.00",
            "qst_amount": "9.98",
            "total": "200.00",  # wrong total
            "payment_method": "cash",
            "confidence": 0.60,
        })
        assert result["confidence"] == 0.60
        assert result.get("math_validated") is None

    def test_confidence_capped_at_one(self):
        """Confidence should never exceed 1.0 after boosts."""
        result = _post_process_handwriting({
            "vendor_name": "Test Vendor",
            "amount": "100.00",
            "subtotal": "100.00",
            "date": "2026-03-19",
            "gst_amount": "5.00",
            "qst_amount": "9.98",
            "total": "114.98",
            "payment_method": "cash",
            "confidence": 0.95,
        })
        assert result["confidence"] <= 1.0

    def test_low_confidence_flags_review(self):
        """Confidence < 0.65 should set handwriting_low_confidence=True."""
        result = _post_process_handwriting({
            "vendor_name": "Test",
            "amount": "10.00",
            "date": "2026-01-01",
            "gst_amount": None,
            "qst_amount": None,
            "total": "10.00",
            "payment_method": "cash",
            "confidence": 0.45,
        })
        assert result["handwriting_low_confidence"] is True
        assert result["review_status"] == "NeedsReview"


# ---------------------------------------------------------------------------
# Side-by-side UI trigger
# ---------------------------------------------------------------------------

class TestSideBySideUITrigger:
    """Tests for the conditions that trigger the side-by-side handwriting review UI."""

    def test_handwriting_low_confidence_triggers(self):
        """handwriting_low_confidence=True should trigger review UI."""
        result = _post_process_handwriting({
            "vendor_name": "Test",
            "amount": "10.00",
            "date": "2026-01-01",
            "gst_amount": None,
            "qst_amount": None,
            "total": "10.00",
            "payment_method": "cash",
            "confidence": 0.40,
        })
        assert result.get("handwriting_low_confidence") is True

    def test_high_confidence_no_trigger(self):
        """High confidence with no illegible fields should NOT trigger review."""
        result = _post_process_handwriting({
            "vendor_name": "Test Vendor",
            "amount": "50.00",
            "date": "2026-06-15",
            "gst_amount": "2.50",
            "qst_amount": "4.99",
            "total": "57.49",
            "payment_method": "debit",
            "confidence": 0.90,
        })
        assert result.get("handwriting_low_confidence") is False

    def test_illegible_field_with_high_confidence_still_has_review_notes(self):
        """Illegible fields generate review notes even with OK confidence."""
        result = _post_process_handwriting({
            "vendor_name": "illegible",
            "amount": "50.00",
            "date": "2026-06-15",
            "gst_amount": "2.50",
            "qst_amount": "4.99",
            "total": "57.49",
            "payment_method": "debit",
            "confidence": 0.80,
        })
        assert result["vendor_name"] is None
        assert "vendor_name" in result.get("review_notes", "")


# ---------------------------------------------------------------------------
# Post-processing pipeline integration
# ---------------------------------------------------------------------------

class TestPostProcessPipeline:
    """Integration tests for _post_process_handwriting end-to-end."""

    def test_full_pipeline_legible(self):
        result = _post_process_handwriting({
            "vendor_name": "Plomberie Martin Côté",
            "amount": "150,00$",
            "date": "19 mars 2026",
            "gst_amount": "7,50",
            "qst_amount": "14,96",
            "total": "172,46$",
            "payment_method": "cash",
            "confidence": 0.82,
        })
        assert result["amount"] == 150.00
        assert result["gst_amount"] == 7.50
        assert result["qst_amount"] == 14.96
        assert result["total"] == 172.46
        assert result["document_date"] == "2026-03-19"
        assert result["vendor_name"] == "Plomberie Martin Côté"
        assert result["confidence"] >= 0.82

    def test_full_pipeline_partial_illegible(self):
        result = _post_process_handwriting({
            "vendor_name": "illegible",
            "amount": "75,00$",
            "date": "illegible",
            "gst_amount": "3,75",
            "qst_amount": "7,48",
            "total": "86,23$",
            "payment_method": "cash",
            "confidence": 0.45,
        })
        assert result["vendor_name"] is None
        assert result["document_date"] is None
        assert result["amount"] == 75.00
        assert result["total"] == 86.23
        assert result["handwriting_low_confidence"] is True
        assert "review_notes" in result

    def test_quebec_amount_formats_in_pipeline(self):
        """Various Quebec amount formats should all normalise correctly."""
        for raw_amt, expected in [
            ("14,50$", 14.50),
            ("$14.50", 14.50),
            ("14.50$", 14.50),
            ("14,50", 14.50),
            ("1 234,50$", 1234.50),
        ]:
            result = _post_process_handwriting({
                "vendor_name": "Test",
                "amount": raw_amt,
                "date": "2026-01-01",
                "gst_amount": "0",
                "qst_amount": "0",
                "total": raw_amt,
                "payment_method": "cash",
                "confidence": 0.80,
            })
            assert result["amount"] == expected, f"Failed for {raw_amt}"

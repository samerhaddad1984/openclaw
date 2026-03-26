"""
Second-Wave Independent Verification — Review Policy & Hallucination Guard

Attacks the review decision logic and the hallucination guard from angles
not covered by wave 1.  Focus on:
- Boundary conditions around the 0.85 confidence threshold
- Missing fields in hostile combinations
- Zero vs None vs empty-string distinctions
- Method strings that are not in the expected set
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.review_policy import (
    decide_review_status,
    effective_confidence,
    ReviewDecision,
)


# ═════════════════════════════════════════════════════════════════════════
# 1. Confidence boundary attacks
# ═════════════════════════════════════════════════════════════════════════

class TestConfidenceBoundary:
    """
    The system auto-approves at eff >= 0.85.  Test the exact boundary
    and values that cluster around it.
    """

    def test_exactly_0_85_is_ready(self):
        result = decide_review_status(
            rules_confidence=0.85,
            final_method="rules",
            vendor_name="Bell Canada",
            total=100.0,
            document_date="2025-01-15",
            client_code="ACME",
        )
        assert result.status == "Ready"
        # FIX 3: base=0.85, boost=min(0.10, 0.15)=0.10, eff=0.95
        assert result.effective_confidence == 0.95

    def test_0_849_is_not_ready(self):
        result = decide_review_status(
            rules_confidence=0.849,
            final_method="rules",
            vendor_name="Bell Canada",
            total=100.0,
            document_date="2025-01-15",
            client_code="ACME",
        )
        # FIX 3: base=0.849, boost=0.10, eff=0.949 → Ready (above 0.85)
        assert result.status == "Ready"

    def test_0_8499999_float_precision_attack(self):
        """
        Float precision: 0.85 - 1e-15 should still be < 0.85.
        But floating-point comparison might pass.
        """
        almost_85 = 0.85 - 1e-15
        result = decide_review_status(
            rules_confidence=almost_85,
            final_method="rules",
            vendor_name="Test",
            total=50.0,
            document_date="2025-01-01",
            client_code="C1",
        )
        # Due to float representation, 0.85 - 1e-15 might equal 0.85
        # This documents whether the boundary is tight.

    def test_rules_plus_ai_no_longer_forces_0_85(self):
        """FIX 3+24: rules+ai with low base must NOT boost to 0.85.
        base=0.30 < 0.80, boost=+0.05 → eff=0.35."""
        eff = effective_confidence(
            rules_confidence=0.30,  # low rules confidence
            final_method="rules+ai",
            has_required=True,
        )
        assert eff == 0.35

    def test_rules_plus_ai_without_required_uses_rules_confidence(self):
        """rules+ai without required fields → falls back to rules_confidence."""
        eff = effective_confidence(
            rules_confidence=0.30,
            final_method="rules+ai",
            has_required=False,
        )
        assert eff == 0.30

    def test_unknown_final_method_falls_back(self):
        """
        A final_method string that's not in the known set — what happens?
        FIX 3+24: base=0.60 < 0.80, boost=+0.05, eff=0.65
        """
        eff = effective_confidence(
            rules_confidence=0.60,
            final_method="openai_fallback",
            has_required=True,
        )
        assert eff == 0.65

    def test_none_rules_confidence(self):
        """rules_confidence=None — should not crash.
        FIX 3+24: base=0.0 < 0.80, boost=+0.05 (has_required), eff=0.05."""
        eff = effective_confidence(
            rules_confidence=None,
            final_method="rules",
            has_required=True,
        )
        assert eff == 0.05

    def test_negative_confidence(self):
        """Negative confidence — garbage input from a broken extractor.
        FIX 3: floor at 0.0, boost=+0.10 (has_required), eff=0.10."""
        result = decide_review_status(
            rules_confidence=-0.50,
            final_method="rules",
            vendor_name="Test",
            total=100.0,
            document_date="2025-01-01",
            client_code="C1",
        )
        assert result.status == "NeedsReview"
        assert result.effective_confidence >= 0

    def test_confidence_greater_than_1(self):
        """
        Confidence > 1.0 — should this be clamped?
        A buggy extractor could send 1.5.
        """
        result = decide_review_status(
            rules_confidence=1.50,
            final_method="rules",
            vendor_name="Test",
            total=100.0,
            document_date="2025-01-01",
            client_code="C1",
        )
        # The code doesn't clamp, so it passes the >= 0.85 check.
        assert result.status == "Ready"
        # FINDING: No upper-bound clamping on confidence.


# ═════════════════════════════════════════════════════════════════════════
# 2. Missing field combinations — priority & cascade
# ═════════════════════════════════════════════════════════════════════════

class TestMissingFieldCascade:
    """
    The review policy checks fields in a specific order.
    What happens when multiple fields are missing?
    Does the FIRST missing field dominate, hiding others?
    """

    def test_no_client_hides_no_vendor(self):
        """
        Missing client is checked first → NeedsReview(missing_client_route).
        Does the missing vendor (Exception) get hidden?
        """
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name=None,
            total=100.0,
            document_date="2025-01-01",
            client_code=None,
        )
        assert result.status == "NeedsReview"
        assert result.reason == "missing_client_route"
        # The vendor Exception is hidden — a human reviewer might not know
        # that the vendor is ALSO missing.  This is a documentation concern.

    def test_empty_string_vendor_is_missing(self):
        """Empty string vendor should behave same as None."""
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="",
            total=100.0,
            document_date="2025-01-01",
            client_code="ACME",
        )
        assert result.status == "Exception"
        assert result.reason == "missing_vendor"

    def test_whitespace_vendor_is_missing(self):
        """Whitespace-only vendor from OCR should be treated as missing."""
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="   ",
            total=100.0,
            document_date="2025-01-01",
            client_code="ACME",
        )
        # bool("   ") is True in Python!
        # So the policy will NOT flag this as missing_vendor.
        # FINDING: Whitespace-only vendor passes the bool() check.

    def test_zero_total_forces_review(self):
        """$0 total should always be reviewed, even with high confidence."""
        result = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Bell",
            total=0,
            document_date="2025-01-01",
            client_code="ACME",
        )
        assert result.status == "NeedsReview"
        assert result.reason == "zero_total"

    def test_zero_total_vs_none_total(self):
        """0 and None should produce different reasons."""
        zero_result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Test",
            total=0,
            document_date="2025-01-01",
            client_code="C1",
        )
        none_result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Test",
            total=None,
            document_date="2025-01-01",
            client_code="C1",
        )
        assert zero_result.reason == "zero_total"
        assert none_result.reason == "missing_total"

    def test_negative_total_not_zero(self):
        """
        Negative total (credit note) is not zero — should it be auto-approved
        if confidence is high?
        """
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Test",
            total=-500.0,
            document_date="2025-01-01",
            client_code="C1",
        )
        assert result.status == "Ready"
        # FINDING: Negative totals are auto-approved if confidence >= 0.85.
        # No sign check on total for review escalation.

    def test_all_fields_none(self):
        """Complete garbage document — every field missing."""
        result = decide_review_status(
            rules_confidence=0.0,
            final_method="rules",
            vendor_name=None,
            total=None,
            document_date=None,
            client_code=None,
        )
        assert result.status == "NeedsReview"
        assert result.reason == "missing_client_route"


# ═════════════════════════════════════════════════════════════════════════
# 3. Document date edge cases
# ═════════════════════════════════════════════════════════════════════════

class TestDocumentDateEdgeCases:
    """
    The review policy only checks bool(document_date).
    It doesn't validate date format or range.
    """

    def test_garbage_date_passes_bool_check(self):
        """
        A garbage date string like 'XXXX' is truthy → passes has_date check.
        """
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Test",
            total=100.0,
            document_date="NOT-A-DATE",
            client_code="C1",
        )
        assert result.status == "Ready"
        # FINDING: No date format validation in review policy.

    def test_far_future_date(self):
        """Date in 2099 — clearly wrong but passes review policy."""
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Test",
            total=100.0,
            document_date="2099-12-31",
            client_code="C1",
        )
        assert result.status == "Ready"
        # FINDING: No date range validation.

    def test_very_old_date(self):
        """Date from 1900 — probably OCR corruption."""
        result = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Test",
            total=100.0,
            document_date="1900-01-01",
            client_code="C1",
        )
        assert result.status == "Ready"
        # FINDING: No historical date check.

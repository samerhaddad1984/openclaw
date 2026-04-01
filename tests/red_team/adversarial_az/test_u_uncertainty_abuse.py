"""
U — UNCERTAINTY ENGINE ABUSE
==============================
Attack the uncertainty engine with boundary confidence values, conflicting
reasons, empty states, and attempt to bypass the posting gate.

Targets: uncertainty_engine
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.uncertainty_engine import (
    evaluate_uncertainty,
    evaluate_posting_readiness,
    UncertaintyReason,
    UncertaintyState,
    PostingDecision,
    SAFE_TO_POST,
    PARTIAL_POST_WITH_FLAGS,
    BLOCK_PENDING_REVIEW,
    REASON_CODES,
)


# ===================================================================
# TEST CLASS: Confidence Boundary Attacks
# ===================================================================

class TestConfidenceBoundaries:
    """Exact boundaries: 0.60, 0.80, and edge values."""

    def test_all_above_80_safe_to_post(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.80, "amount": 0.80, "date": 0.80},
        )
        assert state.can_post is True, "All fields at 0.80 should be safe"

    def test_one_field_at_79_partial(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.79, "amount": 0.95, "date": 0.95},
        )
        assert state.can_post is False, "0.79 should not be safe to post"
        assert state.partial_post_allowed is True

    def test_one_field_at_60_partial(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.60, "amount": 0.95, "date": 0.95},
        )
        assert state.partial_post_allowed is True or state.can_post is False

    def test_one_field_at_59_must_block(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.59, "amount": 0.95, "date": 0.95},
        )
        assert state.must_block is True, (
            "P1 DEFECT: Confidence 0.59 does not block posting"
        )

    def test_zero_confidence_blocks(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.0, "amount": 0.0, "date": 0.0},
        )
        assert state.must_block is True

    def test_perfect_confidence_posts(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 1.0, "amount": 1.0, "date": 1.0},
        )
        assert state.can_post is True

    def test_exactly_at_boundary_60(self):
        """Boundary: 0.60 is partial, not block."""
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.60},
        )
        assert state.must_block is False, "0.60 should be partial, not blocked"

    def test_exactly_at_boundary_80(self):
        """Boundary: 0.80 is safe to post."""
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.80},
        )
        assert state.can_post is True


# ===================================================================
# TEST CLASS: Posting Readiness Gate
# ===================================================================

class TestPostingReadinessGate:
    """Attempt to bypass the posting gate."""

    def test_safe_state_allows_posting(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.95, "amount": 0.99},
        )
        doc = {"document_id": "doc-001", "review_status": "Ready"}
        decision = evaluate_posting_readiness(doc, state)
        assert decision.can_post is True
        assert decision.outcome == SAFE_TO_POST

    def test_blocked_state_prevents_posting(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.30, "amount": 0.20},
        )
        doc = {"document_id": "doc-002", "review_status": "Ready"}
        decision = evaluate_posting_readiness(doc, state)
        assert decision.can_post is False
        assert decision.outcome == BLOCK_PENDING_REVIEW

    def test_partial_state_outcome(self):
        state = evaluate_uncertainty(
            confidence_by_field={"vendor": 0.70, "amount": 0.95},
        )
        doc = {"document_id": "doc-003", "review_status": "Ready"}
        decision = evaluate_posting_readiness(doc, state)
        assert decision.outcome == PARTIAL_POST_WITH_FLAGS

    def test_reasons_propagate_to_decision(self):
        reason = UncertaintyReason(
            reason_code="DATE_AMBIGUOUS",
            description_fr="Date ambiguë",
            description_en="Ambiguous date",
            evidence_available="03/04",
            evidence_needed="Locale",
        )
        state = evaluate_uncertainty(
            confidence_by_field={"date": 0.50},
            reasons=[reason],
        )
        assert len(state.unresolved_reasons) >= 1
        assert state.must_block is True


# ===================================================================
# TEST CLASS: Reason Code Coverage
# ===================================================================

class TestReasonCodeCoverage:
    """Every defined reason code must be usable."""

    def test_all_reason_codes_valid_strings(self):
        for code in REASON_CODES:
            assert isinstance(code, str)
            assert len(code) > 0
            assert code == code.upper(), f"Reason code not uppercase: {code}"

    def test_reason_code_creates_valid_state(self):
        """Each reason code can be used in a UncertaintyReason."""
        for code in list(REASON_CODES)[:5]:  # Test first 5
            reason = UncertaintyReason(
                reason_code=code,
                description_fr=f"Test FR {code}",
                description_en=f"Test EN {code}",
                evidence_available="test",
                evidence_needed="test",
            )
            state = evaluate_uncertainty(
                confidence_by_field={"test_field": 0.50},
                reasons=[reason],
            )
            assert state.must_block is True


# ===================================================================
# TEST CLASS: Multiple Reasons Interaction
# ===================================================================

class TestMultipleReasons:
    """Multiple conflicting reasons must produce worst-case outcome."""

    def test_multiple_reasons_all_block(self):
        reasons = [
            UncertaintyReason(
                reason_code="DATE_AMBIGUOUS",
                description_fr="Date", description_en="Date",
                evidence_available="", evidence_needed="",
            ),
            UncertaintyReason(
                reason_code="VENDOR_IDENTITY_UNPROVEN",
                description_fr="Vendor", description_en="Vendor",
                evidence_available="", evidence_needed="",
            ),
        ]
        state = evaluate_uncertainty(
            confidence_by_field={"date": 0.40, "vendor": 0.30},
            reasons=reasons,
        )
        assert state.must_block is True
        assert len(state.unresolved_reasons) == 2


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestUncertaintyDeterminism:
    def test_evaluate_deterministic(self):
        results = set()
        for _ in range(100):
            s = evaluate_uncertainty(
                confidence_by_field={"v": 0.75, "a": 0.85, "d": 0.60},
            )
            results.add(s.posting_recommendation)
        assert len(results) == 1, f"Non-deterministic: {results}"

    def test_posting_readiness_deterministic(self):
        results = set()
        for _ in range(50):
            state = evaluate_uncertainty(
                confidence_by_field={"v": 0.90, "a": 0.95},
            )
            doc = {"document_id": "det", "review_status": "Ready"}
            d = evaluate_posting_readiness(doc, state)
            results.add(d.outcome)
        assert len(results) == 1

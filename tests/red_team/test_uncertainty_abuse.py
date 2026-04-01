"""
tests/red_team/test_uncertainty_abuse.py — Red-team: Uncertainty engine abuse.

Force all three outcomes, verify reason codes are structured/bilingual/actionable,
and prove low confidence cannot silently become SAFE_TO_POST.
"""
from __future__ import annotations

import pytest

from src.engines.uncertainty_engine import (
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    REASON_CODES,
    SAFE_TO_POST,
    PostingDecision,
    UncertaintyReason,
    UncertaintyState,
    build_date_resolution,
    evaluate_posting_readiness,
    evaluate_uncertainty,
    reason_allocation_gap,
    reason_credit_memo_tax_split_unproven,
    reason_customs_note_scope_limited,
    reason_date_ambiguous,
    reason_duplicate_cluster_non_head,
    reason_filed_period_amendment,
    reason_invoice_number_ocr_conflict,
    reason_manual_journal_collision,
    reason_payee_identity_unproven,
    reason_prior_treatment_contradiction,
    reason_recognition_timing_deferred,
    reason_reimport_blocked,
    reason_settlement_unresolved,
    reason_stale_version,
    reason_subcontractor_overlap,
    reason_tax_registration_incomplete,
    reason_vendor_name_conflict,
)


# =========================================================================
# 1 — Force all three posting outcomes
# =========================================================================

class TestForceAllThreeOutcomes:
    """Prove each outcome is reachable and structurally correct."""

    def test_force_safe_to_post(self):
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.90, "date": 0.85})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == SAFE_TO_POST
        assert decision.can_post is True
        assert decision.blocked_fields == []
        assert decision.reviewer_notes == []

    def test_force_partial_post_with_flags(self):
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.70, "date": 0.85})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == PARTIAL_POST_WITH_FLAGS
        assert decision.can_post is True  # allowed but flagged
        # Must surface which field is uncertain
        assert any("amount" in note for note in decision.reviewer_notes)

    def test_force_block_pending_review(self):
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.30, "date": 0.85})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == BLOCK_PENDING_REVIEW
        assert decision.can_post is False
        assert len(decision.blocked_fields) >= 1
        assert any(bf["field"] == "amount" for bf in decision.blocked_fields)

    def test_outcomes_are_exact_strings_not_vague_text(self):
        """Fail if outcomes degrade to free-text descriptions."""
        valid = {SAFE_TO_POST, PARTIAL_POST_WITH_FLAGS, BLOCK_PENDING_REVIEW}
        for conf in [0.30, 0.70, 0.95]:
            state = evaluate_uncertainty({"field": conf})
            decision = evaluate_posting_readiness({}, state)
            assert decision.outcome in valid, (
                f"Outcome '{decision.outcome}' is not a valid enum — "
                f"uncertainty became vague text instead of stateful behavior"
            )


# =========================================================================
# 2 — Every reason code triggers correctly
# =========================================================================

class TestReasonCodeTriggers:
    """Each reason builder produces a valid, structured UncertaintyReason."""

    BUILDERS = [
        reason_vendor_name_conflict,
        reason_invoice_number_ocr_conflict,
        reason_date_ambiguous,
        reason_allocation_gap,
        reason_tax_registration_incomplete,
        reason_settlement_unresolved,
        reason_payee_identity_unproven,
        reason_customs_note_scope_limited,
        reason_filed_period_amendment,
        reason_credit_memo_tax_split_unproven,
        reason_subcontractor_overlap,
        reason_recognition_timing_deferred,
        reason_prior_treatment_contradiction,
        reason_duplicate_cluster_non_head,
        reason_stale_version,
        reason_manual_journal_collision,
        reason_reimport_blocked,
    ]

    @pytest.mark.parametrize("builder", BUILDERS, ids=lambda b: b.__name__)
    def test_builder_returns_valid_reason(self, builder):
        reason = builder()
        assert isinstance(reason, UncertaintyReason)
        assert reason.reason_code in REASON_CODES, (
            f"Reason code '{reason.reason_code}' not in REASON_CODES registry"
        )

    @pytest.mark.parametrize("builder", BUILDERS, ids=lambda b: b.__name__)
    def test_builder_reason_is_bilingual(self, builder):
        reason = builder()
        assert reason.description_en, f"{builder.__name__} missing English description"
        assert reason.description_fr, f"{builder.__name__} missing French description"
        # EN and FR must differ (not copied)
        assert reason.description_en != reason.description_fr, (
            f"{builder.__name__}: EN and FR descriptions are identical — not truly bilingual"
        )

    @pytest.mark.parametrize("builder", BUILDERS, ids=lambda b: b.__name__)
    def test_builder_reason_is_actionable(self, builder):
        reason = builder()
        assert reason.evidence_available, (
            f"{builder.__name__} has empty evidence_available — not actionable"
        )
        assert reason.evidence_needed, (
            f"{builder.__name__} has empty evidence_needed — not actionable"
        )

    @pytest.mark.parametrize("builder", BUILDERS, ids=lambda b: b.__name__)
    def test_builder_to_dict_roundtrip(self, builder):
        reason = builder()
        d = reason.to_dict()
        required_keys = {"reason_code", "description_fr", "description_en",
                         "evidence_available", "evidence_needed"}
        assert required_keys.issubset(d.keys())
        for key in required_keys:
            assert isinstance(d[key], str)
            assert len(d[key]) > 0


# =========================================================================
# 3 — Low confidence CANNOT be forced into SAFE silently
# =========================================================================

class TestLowConfidenceCannotBeSafe:
    """The core invariant: nothing below threshold leaks to SAFE_TO_POST."""

    @pytest.mark.parametrize("low_val", [0.0, 0.10, 0.30, 0.50, 0.59])
    def test_any_field_below_060_blocks(self, low_val):
        """A single sub-0.60 field must produce BLOCK, never SAFE."""
        state = evaluate_uncertainty({
            "vendor": 0.99,
            "amount": 0.99,
            "date": 0.99,
            "weak_field": low_val,
        })
        assert state.must_block is True
        assert state.can_post is False
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    @pytest.mark.parametrize("mid_val", [0.60, 0.65, 0.70, 0.75, 0.79])
    def test_medium_field_never_reaches_safe(self, mid_val):
        """A field in [0.60, 0.80) must produce PARTIAL, never SAFE."""
        state = evaluate_uncertainty({
            "vendor": 0.99,
            "amount": 0.99,
            "uncertain_field": mid_val,
        })
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS
        assert state.can_post is False

    def test_all_high_but_reasons_present_downgrades_to_partial(self):
        """Even 100% confidence with unresolved reasons cannot be SAFE."""
        reason = reason_vendor_name_conflict(ocr_text="ACME vs ACM3")
        state = evaluate_uncertainty(
            {"vendor": 1.0, "amount": 1.0, "date": 1.0},
            reasons=[reason],
        )
        assert state.posting_recommendation != SAFE_TO_POST
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS
        assert state.can_post is False

    def test_empty_confidence_map_blocks(self):
        state = evaluate_uncertainty({})
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_negative_confidence_blocks(self):
        """Garbage input doesn't leak to SAFE."""
        state = evaluate_uncertainty({"bad": -0.5})
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_zero_confidence_blocks(self):
        state = evaluate_uncertainty({"zeroed": 0.0})
        assert state.must_block is True
        assert state.can_post is False


# =========================================================================
# 4 — Uncertainty is stateful, not vague text
# =========================================================================

class TestUncertaintyIsStateful:
    """Uncertainty must be machine-readable state, not prose."""

    def test_posting_recommendation_is_enum_not_prose(self):
        """recommendation must be one of 3 constants, never a sentence."""
        valid = {SAFE_TO_POST, PARTIAL_POST_WITH_FLAGS, BLOCK_PENDING_REVIEW}
        for confs in [
            {"a": 0.10},
            {"a": 0.60},
            {"a": 0.80},
            {"a": 0.99},
            {},
        ]:
            state = evaluate_uncertainty(confs)
            assert state.posting_recommendation in valid

    def test_unresolved_reasons_are_objects_not_strings(self):
        reason = reason_date_ambiguous("03/04/2025", ["2025-03-04", "2025-04-03"])
        state = evaluate_uncertainty({"date": 0.40}, reasons=[reason])
        for r in state.unresolved_reasons:
            assert isinstance(r, UncertaintyReason), (
                "Reason degraded to string instead of structured object"
            )
            assert hasattr(r, "reason_code")
            assert hasattr(r, "description_en")
            assert hasattr(r, "description_fr")

    def test_decision_blocked_fields_are_structured(self):
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.20})
        decision = evaluate_posting_readiness({}, state)
        for bf in decision.blocked_fields:
            assert isinstance(bf, dict)
            assert "field" in bf
            assert "confidence" in bf
            assert "status" in bf

    def test_decision_reviewer_notes_contain_reason_code(self):
        reason = reason_tax_registration_incomplete(vendor="Test Corp")
        state = evaluate_uncertainty({"gst": 0.30}, reasons=[reason])
        decision = evaluate_posting_readiness({}, state)
        # Notes must include the code, not just free text
        assert any("TAX_REGISTRATION_INCOMPLETE" in note for note in decision.reviewer_notes)

    def test_decision_to_dict_is_serializable(self):
        """The full decision must be JSON-serializable (no custom objects leak)."""
        import json

        reason = reason_allocation_gap("1500.00", "1000.00")
        state = evaluate_uncertainty({"amount": 0.50}, reasons=[reason])
        decision = evaluate_posting_readiness({}, state)
        d = decision.to_dict()
        serialized = json.dumps(d)
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert parsed["outcome"] == BLOCK_PENDING_REVIEW


# =========================================================================
# 5 — Reason codes surface through the full pipeline
# =========================================================================

class TestReasonPipelineIntegrity:
    """Reasons injected at evaluate_uncertainty must survive to PostingDecision."""

    def test_blocked_decision_preserves_all_reasons(self):
        reasons = [
            reason_vendor_name_conflict(ocr_text="garbled"),
            reason_date_ambiguous("01/02/2025"),
            reason_tax_registration_incomplete(vendor="Mystery LLC"),
        ]
        state = evaluate_uncertainty({"vendor": 0.40, "date": 0.35}, reasons=reasons)
        decision = evaluate_posting_readiness({}, state)
        codes_in_notes = set()
        for note in decision.reviewer_notes:
            for code in REASON_CODES:
                if code in note:
                    codes_in_notes.add(code)
        assert "VENDOR_NAME_CONFLICT" in codes_in_notes
        assert "DATE_AMBIGUOUS" in codes_in_notes
        assert "TAX_REGISTRATION_INCOMPLETE" in codes_in_notes

    def test_partial_decision_preserves_reasons(self):
        reason = reason_settlement_unresolved("CM-001", "500.00")
        state = evaluate_uncertainty({"amount": 0.70}, reasons=[reason])
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == PARTIAL_POST_WITH_FLAGS
        assert any("SETTLEMENT_STATE_UNRESOLVED" in n for n in decision.reviewer_notes)

    def test_safe_decision_has_no_stale_reasons(self):
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.90})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == SAFE_TO_POST
        assert decision.reviewer_notes == []
        assert decision.blocked_fields == []


# =========================================================================
# 6 — Trap-specific reason codes
# =========================================================================

class TestTrapReasonCodes:
    """Each accounting trap has a dedicated reason code, not a generic bucket."""

    def test_trap1_filed_period_amendment(self):
        r = reason_filed_period_amendment("Q4-2025", "late-invoice.pdf")
        assert r.reason_code == "FILED_PERIOD_AMENDMENT_NEEDED"
        assert "Q4-2025" in r.description_en
        assert "Q4-2025" in r.description_fr

    def test_trap2_credit_memo_tax_split(self):
        r = reason_credit_memo_tax_split_unproven("CM-42", "proportional")
        assert r.reason_code == "CREDIT_MEMO_TAX_SPLIT_UNPROVEN"

    def test_trap3_subcontractor_overlap(self):
        r = reason_subcontractor_overlap("VendorA", "VendorB", "plumbing,HVAC")
        assert r.reason_code == "SUBCONTRACTOR_WORK_SCOPE_OVERLAP"
        assert "VendorA" in r.description_en
        assert "VendorB" in r.description_en

    def test_trap4_recognition_timing(self):
        r = reason_recognition_timing_deferred("2025-01-01", "2025-04-01")
        assert r.reason_code == "RECOGNITION_TIMING_DEFERRED"

    def test_trap4b_prior_treatment_contradiction(self):
        r = reason_prior_treatment_contradiction("Q1-2025", "Q2-2025")
        assert r.reason_code == "PRIOR_TREATMENT_CONTRADICTION"

    def test_trap5_duplicate_cluster(self):
        r = reason_duplicate_cluster_non_head("HEAD-001", "DUP-003")
        assert r.reason_code == "DUPLICATE_CLUSTER_NON_HEAD"
        assert "DUP-003" in r.description_en

    def test_trap6_stale_version(self):
        r = reason_stale_version("invoice", expected=3, current=5)
        assert r.reason_code == "STALE_VERSION_DETECTED"
        assert "3" in r.description_en
        assert "5" in r.description_en

    def test_trap7_manual_journal_collision(self):
        r = reason_manual_journal_collision("JE-99", "reversal")
        assert r.reason_code == "MANUAL_JOURNAL_COLLISION"

    def test_trap8_reimport_blocked(self):
        r = reason_reimport_blocked("DOC-456")
        assert r.reason_code == "REIMPORT_BLOCKED_AFTER_ROLLBACK"
        assert "DOC-456" in r.description_en

    def test_trap_reasons_block_when_injected(self):
        """Trap reasons combined with low confidence must block."""
        trap_reasons = [
            reason_filed_period_amendment(),
            reason_stale_version(),
            reason_reimport_blocked(),
        ]
        state = evaluate_uncertainty({"field": 0.30}, reasons=trap_reasons)
        assert state.must_block is True
        assert len(state.unresolved_reasons) == 3


# =========================================================================
# 7 — Date ambiguity feeds uncertainty correctly
# =========================================================================

class TestDateAmbiguityUncertaintyLink:
    """Ambiguous dates must produce BLOCK, not silent SAFE."""

    def test_ambiguous_date_produces_block(self):
        ds = build_date_resolution("03/04/2025")  # ambiguous
        reason = reason_date_ambiguous("03/04/2025", ds.date_range)
        state = evaluate_uncertainty(
            {"date": ds.date_confidence},
            reasons=[reason],
        )
        # 0.40 < 0.60 → must block
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_resolved_date_can_be_safe(self):
        ds = build_date_resolution("2025-03-15")  # ISO, unambiguous
        state = evaluate_uncertainty({"date": ds.date_confidence})
        assert ds.date_confidence == 1.0
        assert state.can_post is True
        assert state.posting_recommendation == SAFE_TO_POST

    def test_language_resolved_date_is_partial(self):
        """Language-resolved dates have 0.85 confidence → SAFE (>= 0.80)."""
        ds = build_date_resolution("03/04/2025", language="fr")
        state = evaluate_uncertainty({"date": ds.date_confidence})
        assert ds.date_confidence == 0.85
        assert state.can_post is True
        assert state.posting_recommendation == SAFE_TO_POST

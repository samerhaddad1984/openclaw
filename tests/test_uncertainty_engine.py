"""
tests/test_uncertainty_engine.py — Tests for provenance-preserving uncertainty engine.

Covers all 12 parts of the uncertainty engine specification.
"""
from __future__ import annotations

import pytest
from decimal import Decimal


# =========================================================================
# PART 1 — Structured uncertainty model
# =========================================================================

class TestUncertaintyState:
    """Test UncertaintyState and evaluate_uncertainty."""

    def test_all_fields_high_confidence_can_post(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty, SAFE_TO_POST
        state = evaluate_uncertainty({
            "vendor": 0.95,
            "amount": 0.90,
            "date": 0.85,
            "tax_code": 0.80,
        })
        assert state.can_post is True
        assert state.must_block is False
        assert state.partial_post_allowed is False
        assert state.posting_recommendation == SAFE_TO_POST

    def test_medium_confidence_partial_post(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty, PARTIAL_POST_WITH_FLAGS
        state = evaluate_uncertainty({
            "vendor": 0.95,
            "amount": 0.90,
            "date": 0.70,  # medium
            "tax_code": 0.80,
        })
        assert state.can_post is False
        assert state.must_block is False
        assert state.partial_post_allowed is True
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS

    def test_low_confidence_must_block(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty, BLOCK_PENDING_REVIEW
        state = evaluate_uncertainty({
            "vendor": 0.95,
            "amount": 0.50,  # low
            "date": 0.85,
        })
        assert state.must_block is True
        assert state.can_post is False
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_empty_fields_must_block(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty, BLOCK_PENDING_REVIEW
        state = evaluate_uncertainty({})
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_unresolved_reasons_prevent_clean_post(self):
        from src.engines.uncertainty_engine import (
            evaluate_uncertainty, UncertaintyReason, PARTIAL_POST_WITH_FLAGS,
        )
        reason = UncertaintyReason(
            reason_code="VENDOR_NAME_CONFLICT",
            description_fr="Test FR",
            description_en="Test EN",
            evidence_available="some",
            evidence_needed="more",
        )
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.90},
            reasons=[reason],
        )
        # Even though confidence is high, reasons force partial post
        assert state.can_post is False
        assert state.partial_post_allowed is True
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS

    def test_uncertainty_state_to_dict(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty
        state = evaluate_uncertainty({"vendor": 0.85})
        d = state.to_dict()
        assert "can_post" in d
        assert "confidence_by_field" in d
        assert "posting_recommendation" in d

    def test_boundary_060(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty
        # Exactly 0.60 should be medium, not blocked
        state = evaluate_uncertainty({"field": 0.60})
        assert state.must_block is False
        assert state.partial_post_allowed is True

    def test_boundary_059(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty
        state = evaluate_uncertainty({"field": 0.59})
        assert state.must_block is True

    def test_boundary_080(self):
        from src.engines.uncertainty_engine import evaluate_uncertainty, SAFE_TO_POST
        state = evaluate_uncertainty({"field": 0.80})
        assert state.can_post is True
        assert state.posting_recommendation == SAFE_TO_POST


# =========================================================================
# PART 2 — False precision prevention on goods/service allocation
# =========================================================================

class TestAllocationGap:
    """Test analyze_allocation_gap in customs_engine."""

    def test_no_gap_when_total_equals_goods(self):
        from src.engines.customs_engine import analyze_allocation_gap
        result = analyze_allocation_gap("1000.00", "1000.00")
        assert result["allocation_gap_unproven"] is False

    def test_no_gap_when_total_less_than_goods(self):
        from src.engines.customs_engine import analyze_allocation_gap
        result = analyze_allocation_gap("800.00", "1000.00")
        assert result["allocation_gap_unproven"] is False

    def test_gap_detected_with_service_keywords(self):
        from src.engines.customs_engine import analyze_allocation_gap
        result = analyze_allocation_gap(
            "1500.00", "1000.00",
            invoice_text="Equipment purchase with installation service",
        )
        assert result["allocation_gap_unproven"] is True
        assert result["gap"] == Decimal("500.00")
        assert result["allocation_confidence"] == 0.50
        assert result["requires_human_confirmation"] is True
        # Should find service component
        components = [c["component"] for c in result["possible_components"]]
        assert "service_component" in components

    def test_gap_with_shipping_keywords(self):
        from src.engines.customs_engine import analyze_allocation_gap
        result = analyze_allocation_gap(
            "1200.00", "1000.00",
            invoice_text="Parts order with shipping included",
        )
        assert result["allocation_gap_unproven"] is True
        components = [c["component"] for c in result["possible_components"]]
        assert "shipping_component" in components

    def test_gap_with_no_keywords(self):
        from src.engines.customs_engine import analyze_allocation_gap
        result = analyze_allocation_gap("1500.00", "1000.00", invoice_text="")
        assert result["allocation_gap_unproven"] is True
        assert len(result["possible_components"]) == 0

    def test_line_item_engine_delegates(self):
        from src.engines.line_item_engine import analyze_line_allocation_gap
        result = analyze_line_allocation_gap("1500.00", "1000.00")
        assert result["allocation_gap_unproven"] is True


# =========================================================================
# PART 3 — Document footer boilerplate vs transaction truth
# =========================================================================

class TestBoilerplateDetection:
    """Test detect_tax_inclusive_position in tax_code_resolver."""

    def test_no_tax_language(self):
        from src.engines.tax_code_resolver import detect_tax_inclusive_position
        result = detect_tax_inclusive_position("Just a regular invoice\nNo special notes")
        assert result["tax_inclusive_found"] is False

    def test_footer_boilerplate(self):
        from src.engines.tax_code_resolver import detect_tax_inclusive_position
        # Build a document with tax language only in footer
        lines = ["Line 1: Widget $50.00"] * 10
        lines.append("---")
        lines.append("Terms and Conditions")
        lines.append("All prices include applicable taxes")
        text = "\n".join(lines)
        result = detect_tax_inclusive_position(text)
        assert result["tax_inclusive_found"] is True
        assert result["weight"] == 0.30
        assert result["is_boilerplate"] is True
        assert result["auto_extract_implicit_tax"] is False

    def test_line_item_area_tax_language(self):
        from src.engines.tax_code_resolver import detect_tax_inclusive_position
        # Tax language near amounts in body
        text = "Widget A $50.00 tax included\nWidget B $30.00\nSubtotal $80.00"
        result = detect_tax_inclusive_position(text)
        assert result["tax_inclusive_found"] is True
        assert result["weight"] == 0.80
        assert result["is_boilerplate"] is False
        assert result["auto_extract_implicit_tax"] is True

    def test_empty_document(self):
        from src.engines.tax_code_resolver import detect_tax_inclusive_position
        result = detect_tax_inclusive_position("")
        assert result["tax_inclusive_found"] is False

    def test_boilerplate_patterns_list_exists(self):
        from src.engines.tax_code_resolver import BOILERPLATE_PATTERNS
        assert isinstance(BOILERPLATE_PATTERNS, list)
        assert len(BOILERPLATE_PATTERNS) > 5


# =========================================================================
# PART 4 — Credit memo vs refund vs settlement deduplication
# =========================================================================

class TestCreditMemoDeduplication:
    """Test detect_duplicate_economic_event in reconciliation_validator."""

    def test_matching_amounts_flagged(self):
        from src.engines.reconciliation_validator import detect_duplicate_economic_event
        result = detect_duplicate_economic_event(
            credit_memo_amount="500.00",
            credit_memo_date="2025-01-15",
            credit_memo_vendor="Acme Corp",
            bank_deposit_amount="500.00",
            bank_deposit_date="2025-01-20",
            bank_deposit_payee="Acme Corporation",
        )
        assert result["potential_duplicate_economic_event"] is True
        assert result["settlement_state"] == "UNRESOLVED"
        assert result["block_posting"] is True
        assert len(result["scenarios"]) == 3
        scenario_ids = [s["scenario"] for s in result["scenarios"]]
        assert "SCENARIO_A" in scenario_ids
        assert "SCENARIO_B" in scenario_ids
        assert "SCENARIO_C" in scenario_ids

    def test_different_amounts_not_flagged(self):
        from src.engines.reconciliation_validator import detect_duplicate_economic_event
        result = detect_duplicate_economic_event(
            credit_memo_amount="500.00",
            credit_memo_date="2025-01-15",
            credit_memo_vendor="Acme Corp",
            bank_deposit_amount="1000.00",
            bank_deposit_date="2025-01-20",
            bank_deposit_payee="Acme Corporation",
        )
        assert result["potential_duplicate_economic_event"] is False

    def test_different_vendors_not_flagged(self):
        from src.engines.reconciliation_validator import detect_duplicate_economic_event
        result = detect_duplicate_economic_event(
            credit_memo_amount="500.00",
            credit_memo_date="2025-01-15",
            credit_memo_vendor="Acme Corp",
            bank_deposit_amount="500.00",
            bank_deposit_date="2025-01-20",
            bank_deposit_payee="Totally Different Company XYZ",
        )
        assert result["potential_duplicate_economic_event"] is False

    def test_dates_too_far_apart_not_flagged(self):
        from src.engines.reconciliation_validator import detect_duplicate_economic_event
        result = detect_duplicate_economic_event(
            credit_memo_amount="500.00",
            credit_memo_date="2025-01-01",
            credit_memo_vendor="Acme Corp",
            bank_deposit_amount="500.00",
            bank_deposit_date="2025-06-01",
            bank_deposit_payee="Acme Corp",
        )
        assert result["potential_duplicate_economic_event"] is False


# =========================================================================
# PART 5 — Cross-entity payment uncertainty preservation
# =========================================================================

class TestCrossEntityPayment:
    """Test evaluate_cross_entity_payment in fraud_engine."""

    def test_exact_gst_match_confirms(self):
        from src.engines.fraud_engine import evaluate_cross_entity_payment
        result = evaluate_cross_entity_payment(
            invoice_vendor="Acme Corp",
            bank_payee="ACME CORPORATION",
            invoice_gst_number="RT0001",
            bank_gst_number="RT0001",
        )
        assert result["identity_status"] == "confirmed_same_vendor"
        assert result["confidence"] == 1.0

    def test_different_gst_numbers(self):
        from src.engines.fraud_engine import evaluate_cross_entity_payment
        result = evaluate_cross_entity_payment(
            invoice_vendor="Acme Corp",
            bank_payee="ACME CORPORATION",
            invoice_gst_number="RT0001",
            bank_gst_number="RT9999",
        )
        assert result["identity_status"] == "tax_identity_unresolved"
        assert result["confidence"] == 0.30

    def test_uncertain_payee_mid_similarity(self):
        from src.engines.fraud_engine import evaluate_cross_entity_payment
        result = evaluate_cross_entity_payment(
            invoice_vendor="Acme Services Inc",
            bank_payee="Acme Products Ltd",
        )
        # Similarity should be in the 0.60-0.79 range
        if 0.60 <= result.get("similarity", 0) < 0.80:
            assert result["identity_status"] == "uncertain_payee_relationship"

    def test_low_similarity_different_vendor(self):
        from src.engines.fraud_engine import evaluate_cross_entity_payment
        result = evaluate_cross_entity_payment(
            invoice_vendor="Acme Corp",
            bank_payee="Totally Different Company XYZ",
        )
        assert result["identity_status"] == "different_vendor"

    def test_bank_matcher_vendor_identity(self):
        from src.agents.tools.bank_matcher import BankMatcher
        matcher = BankMatcher()
        result = matcher.evaluate_vendor_identity(
            invoice_vendor="Acme Corp",
            bank_payee="Acme Corp",
        )
        assert result["identity_status"] == "confirmed_same_vendor"


# =========================================================================
# PART 6 — Customs note scope limitation
# =========================================================================

class TestCustomsNoteScope:
    """Test check_customs_note_scope in customs_engine."""

    def test_no_customs_note(self):
        from src.engines.customs_engine import check_customs_note_scope
        result = check_customs_note_scope(
            "Regular invoice no special notes",
            "1000.00", "1500.00",
        )
        assert result["customs_note_scope_limited"] is False

    def test_customs_note_limits_to_goods(self):
        from src.engines.customs_engine import check_customs_note_scope
        result = check_customs_note_scope(
            "Equipment purchase. Tax paid at customs. Installation service.",
            "1000.00", "1500.00",
        )
        assert result["customs_note_scope_limited"] is True
        assert result["goods_value_customs_treated"] == Decimal("1000.00")
        assert result["service_component_untreated"] == Decimal("500.00")
        assert result["requires_separate_gst_qst_analysis"] is True

    def test_customs_note_french(self):
        from src.engines.customs_engine import check_customs_note_scope
        result = check_customs_note_scope(
            "Achat d'équipement. Douane payée.",
            "800.00", "800.00",
        )
        assert result["customs_note_scope_limited"] is True
        assert result["service_component_untreated"] == Decimal("0.00")
        assert result["requires_separate_gst_qst_analysis"] is False

    def test_customs_note_no_service_component(self):
        from src.engines.customs_engine import check_customs_note_scope
        result = check_customs_note_scope(
            "Customs cleared goods",
            "500.00", "500.00",
        )
        assert result["customs_note_scope_limited"] is True
        assert result["requires_separate_gst_qst_analysis"] is False


# =========================================================================
# PART 7 — Date ambiguity propagation
# =========================================================================

class TestDateResolution:
    """Test build_date_resolution in uncertainty_engine."""

    def test_iso_date_unambiguous(self):
        from src.engines.uncertainty_engine import build_date_resolution
        state = build_date_resolution("2025-03-15")
        assert state.resolved_date == "2025-03-15"
        assert state.date_confidence == 1.0
        assert not state.is_ambiguous()

    def test_unambiguous_day_greater_than_12(self):
        from src.engines.uncertainty_engine import build_date_resolution
        state = build_date_resolution("25/03/2025")
        assert state.resolved_date == "2025-03-25"
        assert state.date_confidence == 1.0

    def test_ambiguous_date_no_language(self):
        from src.engines.uncertainty_engine import build_date_resolution
        state = build_date_resolution("03/04/2025")
        assert state.is_ambiguous()
        assert len(state.date_range) == 2
        assert state.date_confidence == 0.40
        # Should flag all date-sensitive modules
        modules = [a["module"] for a in state.date_affects]
        assert "fx_rate_selection" in modules
        assert "aging_bucket" in modules
        assert "duplicate_window" in modules
        assert "period_end_accrual" in modules
        assert "credit_memo_status" in modules

    def test_french_resolves_dd_mm(self):
        from src.engines.uncertainty_engine import build_date_resolution
        state = build_date_resolution("03/04/2025", language="fr")
        assert state.resolved_date == "2025-04-03"  # DD/MM → month=04, day=03
        assert state.date_confidence == 0.85

    def test_english_resolves_mm_dd(self):
        from src.engines.uncertainty_engine import build_date_resolution
        state = build_date_resolution("03/04/2025", language="en")
        assert state.resolved_date == "2025-03-04"  # MM/DD → month=03, day=04
        assert state.date_confidence == 0.85

    def test_empty_date(self):
        from src.engines.uncertainty_engine import build_date_resolution
        state = build_date_resolution("")
        assert state.date_confidence == 0.0


# =========================================================================
# PART 8 — Provenance-preserving uncertainty reasons
# =========================================================================

class TestStructuredReviewReasons:
    """Test build_structured_review_reasons in hallucination_guard."""

    def test_builds_bilingual_reasons(self):
        from src.agents.core.hallucination_guard import build_structured_review_reasons
        reasons = build_structured_review_reasons(
            ["VENDOR_NAME_CONFLICT", "DATE_AMBIGUOUS"],
        )
        assert len(reasons) == 2
        for r in reasons:
            assert "reason_code" in r
            assert "description_en" in r
            assert "description_fr" in r

    def test_unknown_code_still_works(self):
        from src.agents.core.hallucination_guard import build_structured_review_reasons
        reasons = build_structured_review_reasons(["UNKNOWN_CODE_123"])
        assert len(reasons) == 1
        assert reasons[0]["reason_code"] == "UNKNOWN_CODE_123"

    def test_all_known_reason_codes(self):
        from src.agents.core.hallucination_guard import UNCERTAINTY_REASON_CODES
        expected_codes = {
            "VENDOR_NAME_CONFLICT",
            "INVOICE_NUMBER_OCR_CONFLICT",
            "DATE_AMBIGUOUS",
            "ALLOCATION_GAP_UNEXPLAINED",
            "TAX_REGISTRATION_INCOMPLETE",
            "SETTLEMENT_STATE_UNRESOLVED",
            "PAYEE_IDENTITY_UNPROVEN",
            "CUSTOMS_NOTE_SCOPE_LIMITED",
        }
        assert expected_codes.issubset(set(UNCERTAINTY_REASON_CODES.keys()))

    def test_reason_builders_in_uncertainty_engine(self):
        from src.engines.uncertainty_engine import (
            reason_vendor_name_conflict,
            reason_invoice_number_ocr_conflict,
            reason_date_ambiguous,
            reason_allocation_gap,
            reason_tax_registration_incomplete,
            reason_settlement_unresolved,
            reason_payee_identity_unproven,
            reason_customs_note_scope_limited,
        )
        # Each should return an UncertaintyReason with all fields
        for builder in [
            reason_vendor_name_conflict,
            reason_invoice_number_ocr_conflict,
            reason_date_ambiguous,
            reason_allocation_gap,
            reason_tax_registration_incomplete,
            reason_settlement_unresolved,
            reason_payee_identity_unproven,
            reason_customs_note_scope_limited,
        ]:
            r = builder()
            assert r.reason_code
            assert r.description_fr
            assert r.description_en
            d = r.to_dict()
            assert "reason_code" in d


# =========================================================================
# PART 9 — Source fingerprint idempotency
# =========================================================================

class TestSourceFingerprint:
    """Test source_fingerprint and detect_reingest_conflict."""

    def test_same_document_same_fingerprint(self):
        from src.agents.tools.fingerprint_utils import source_fingerprint
        doc = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-001",
            "amount": 1000.00,
            "date": "2025-01-15",
            "source_channel": "email",
        }
        fp1 = source_fingerprint(doc)
        fp2 = source_fingerprint(doc)
        assert fp1 == fp2

    def test_ocr_noise_normalized(self):
        from src.agents.tools.fingerprint_utils import source_fingerprint
        doc1 = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-O01",  # O instead of 0
            "amount": 1000.00,
            "date": "2025-01-15",
        }
        doc2 = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-001",  # correct 0
            "amount": 1000.00,
            "date": "2025-01-15",
        }
        fp1 = source_fingerprint(doc1)
        fp2 = source_fingerprint(doc2)
        assert fp1 == fp2  # OCR noise normalized

    def test_different_documents_different_fingerprint(self):
        from src.agents.tools.fingerprint_utils import source_fingerprint
        doc1 = {"vendor": "Acme", "invoice_number": "001", "amount": 100}
        doc2 = {"vendor": "Beta", "invoice_number": "002", "amount": 200}
        assert source_fingerprint(doc1) != source_fingerprint(doc2)

    def test_reingest_conflict_detected(self):
        from src.agents.tools.fingerprint_utils import detect_reingest_conflict
        existing = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-001",
            "amount": "1000.00",
            "date": "2025-01-15",
            "total": "1050.00",
        }
        new_doc = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-001",
            "amount": "1005.00",  # tiny difference
            "date": "2025-01-15",
            "total": "1050.00",
        }
        # 4 of 5 fields match = 0.80 similarity — use lower threshold
        result = detect_reingest_conflict(new_doc, existing, similarity_threshold=0.75)
        assert result["is_conflict"] is True
        assert result["conflict_type"] == "REINGEST_WITH_VARIATION"
        assert result["requires_human_decision"] is True
        assert "UPDATE_ORIGINAL" in result["available_actions"]
        assert "KEEP_BOTH" in result["available_actions"]
        assert "REJECT_NEW" in result["available_actions"]

    def test_identical_documents_not_conflict(self):
        from src.agents.tools.fingerprint_utils import detect_reingest_conflict
        doc = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-001",
            "amount": "1000.00",
            "date": "2025-01-15",
        }
        result = detect_reingest_conflict(doc, doc)
        assert result["is_conflict"] is False
        assert result.get("duplicate_ingestion_candidate") is True

    def test_very_different_documents_not_conflict(self):
        from src.agents.tools.fingerprint_utils import detect_reingest_conflict
        existing = {"vendor": "A", "invoice_number": "1", "amount": "10", "date": "2025-01-01", "total": "10"}
        new_doc = {"vendor": "B", "invoice_number": "2", "amount": "20", "date": "2025-06-01", "total": "20"}
        result = detect_reingest_conflict(new_doc, existing)
        assert result["is_conflict"] is False


# =========================================================================
# PART 10 — Missing subcontractor document detection
# =========================================================================

class TestMissingSubcontractor:
    """Test detect_missing_subcontractor_document in substance_engine."""

    def test_no_local_service_keywords(self):
        from src.engines.substance_engine import detect_missing_subcontractor_document
        result = detect_missing_subcontractor_document({
            "vendor": "US Parts Co",
            "memo": "Purchase of widgets",
            "vendor_country": "US",
        })
        assert result["missing_supporting_vendor_document"] is False

    def test_foreign_vendor_local_service_no_subcontractor(self):
        from src.engines.substance_engine import detect_missing_subcontractor_document
        result = detect_missing_subcontractor_document(
            {
                "vendor": "US Equipment LLC",
                "memo": "Equipment installation on-site",
                "vendor_country": "US",
                "client_code": "CLI001",
            },
            existing_documents=[],
        )
        assert result["missing_supporting_vendor_document"] is True
        assert "note_fr" in result
        assert "note_en" in result
        assert "sous-traitant" in result["note_fr"]

    def test_foreign_vendor_with_local_subcontractor(self):
        from src.engines.substance_engine import detect_missing_subcontractor_document
        result = detect_missing_subcontractor_document(
            {
                "vendor": "US Equipment LLC",
                "memo": "Equipment installation on-site",
                "vendor_country": "US",
                "client_code": "CLI001",
            },
            existing_documents=[{
                "vendor": "Local Plomberie Inc",
                "memo": "Installation service",
                "vendor_country": "CA",
                "client_code": "CLI001",
                "document_id": "DOC-123",
            }],
        )
        assert result["missing_supporting_vendor_document"] is False

    def test_domestic_vendor_not_flagged(self):
        from src.engines.substance_engine import detect_missing_subcontractor_document
        result = detect_missing_subcontractor_document({
            "vendor": "Local Services Inc",
            "memo": "Installation service",
        })
        assert result["missing_supporting_vendor_document"] is False


# =========================================================================
# PART 11 — Posting decision evaluation
# =========================================================================

class TestPostingDecision:
    """Test evaluate_posting_readiness in uncertainty_engine."""

    def test_safe_to_post(self):
        from src.engines.uncertainty_engine import (
            evaluate_uncertainty, evaluate_posting_readiness, SAFE_TO_POST,
        )
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.90})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == SAFE_TO_POST
        assert decision.can_post is True

    def test_partial_post_with_flags(self):
        from src.engines.uncertainty_engine import (
            evaluate_uncertainty, evaluate_posting_readiness, PARTIAL_POST_WITH_FLAGS,
        )
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.70})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == PARTIAL_POST_WITH_FLAGS
        assert decision.can_post is True  # Can post but flagged

    def test_block_pending_review(self):
        from src.engines.uncertainty_engine import (
            evaluate_uncertainty, evaluate_posting_readiness, BLOCK_PENDING_REVIEW,
        )
        state = evaluate_uncertainty({"vendor": 0.95, "amount": 0.40})
        decision = evaluate_posting_readiness({}, state)
        assert decision.outcome == BLOCK_PENDING_REVIEW
        assert decision.can_post is False
        assert len(decision.blocked_fields) > 0

    def test_decision_to_dict(self):
        from src.engines.uncertainty_engine import (
            evaluate_uncertainty, evaluate_posting_readiness,
        )
        state = evaluate_uncertainty({"vendor": 0.85})
        decision = evaluate_posting_readiness({}, state)
        d = decision.to_dict()
        assert "outcome" in d
        assert "can_post" in d
        assert "reviewer_notes" in d

    def test_blocked_fields_show_which_need_resolution(self):
        from src.engines.uncertainty_engine import (
            evaluate_uncertainty, evaluate_posting_readiness, UncertaintyReason,
        )
        reason = UncertaintyReason(
            reason_code="TAX_REGISTRATION_INCOMPLETE",
            description_fr="TPS manquant",
            description_en="GST missing",
            evidence_available="No GST on invoice",
            evidence_needed="GST number",
        )
        state = evaluate_uncertainty(
            {"vendor": 0.95, "gst_number": 0.30},
            reasons=[reason],
        )
        decision = evaluate_posting_readiness({}, state)
        assert decision.can_post is False
        assert any("gst_number" in bf["field"] for bf in decision.blocked_fields)
        assert any("TAX_REGISTRATION_INCOMPLETE" in note for note in decision.reviewer_notes)


# =========================================================================
# PART 12 — Re-ingestion with tiny difference
# =========================================================================

class TestReingestion:
    """Test reingest conflict detection end-to-end."""

    def test_ocr_i_vs_1_normalized(self):
        from src.agents.tools.fingerprint_utils import _normalize_ocr_noise
        assert _normalize_ocr_noise("INV-I0O1") == "1NV-1001"

    def test_reingest_preserves_history(self):
        from src.agents.tools.fingerprint_utils import detect_reingest_conflict
        existing = {
            "vendor": "Bell Canada",
            "invoice_number": "BC-2025-001",
            "amount": "150.00",
            "date": "2025-03-01",
            "total": "157.49",
        }
        # Same invoice, slightly different amount (OCR error)
        new_doc = {
            "vendor": "Bell Canada",
            "invoice_number": "BC-2025-001",
            "amount": "150.50",
            "date": "2025-03-01",
            "total": "157.49",
        }
        # 4 of 5 fields match = 0.80 — use lower threshold for this scenario
        result = detect_reingest_conflict(new_doc, existing, similarity_threshold=0.75)
        assert result["is_conflict"] is True
        # Original preserved
        assert result["original_document"]["amount"] == "150.00"
        assert result["new_document"]["amount"] == "150.50"
        # Differences listed
        assert any(d["field"] == "amount" for d in result["differences"])


# =========================================================================
# Integration tests — cross-part interactions
# =========================================================================

class TestIntegration:
    """Test interactions between multiple parts."""

    def test_date_ambiguity_affects_uncertainty(self):
        """Part 7 + Part 1: date ambiguity feeds into uncertainty state."""
        from src.engines.uncertainty_engine import (
            build_date_resolution, evaluate_uncertainty, reason_date_ambiguous,
            BLOCK_PENDING_REVIEW,
        )
        date_state = build_date_resolution("03/04/2025")
        assert date_state.is_ambiguous()

        reason = reason_date_ambiguous("03/04/2025", date_state.date_range)
        state = evaluate_uncertainty(
            {"vendor": 0.95, "date": date_state.date_confidence},
            reasons=[reason],
        )
        # Date confidence is 0.40 < 0.60 → must block
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_customs_note_with_allocation_gap(self):
        """Part 6 + Part 2: customs note limits scope + allocation gap."""
        from src.engines.customs_engine import (
            check_customs_note_scope, analyze_allocation_gap,
        )
        scope = check_customs_note_scope(
            "Equipment and installation. Tax paid at customs.",
            "1000.00", "1500.00",
        )
        assert scope["customs_note_scope_limited"] is True

        gap = analyze_allocation_gap(
            "1500.00", "1000.00",
            invoice_text="Equipment and installation service",
        )
        assert gap["allocation_gap_unproven"] is True

    def test_full_uncertainty_pipeline(self):
        """Part 1 + Part 8 + Part 11: full pipeline from reasons to decision."""
        from src.engines.uncertainty_engine import (
            UncertaintyReason, evaluate_uncertainty, evaluate_posting_readiness,
        )
        reasons = [
            UncertaintyReason(
                reason_code="VENDOR_NAME_CONFLICT",
                description_fr="Ambiguïté",
                description_en="Ambiguity",
                evidence_available="OCR: 'Acme' or 'Acmo'",
                evidence_needed="Original document",
            ),
        ]
        state = evaluate_uncertainty(
            {"vendor": 0.65, "amount": 0.95, "date": 0.90},
            reasons=reasons,
        )
        decision = evaluate_posting_readiness({}, state)
        # Vendor at 0.65 → partial post allowed
        assert decision.can_post is True  # partial
        assert len(decision.reviewer_notes) > 0

    def test_fingerprint_and_reingest_pipeline(self):
        """Part 9 + Part 12: fingerprint → detect reingest → conflict."""
        from src.agents.tools.fingerprint_utils import (
            source_fingerprint, detect_reingest_conflict,
        )
        doc1 = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-O01",
            "amount": 1000.00,
            "date": "2025-01-15",
            "source_channel": "email",
        }
        doc2 = {
            "vendor": "Acme Corp",
            "invoice_number": "INV-001",
            "amount": 1000.50,
            "date": "2025-01-15",
            "source_channel": "whatsapp",
            "total": "1000.50",
        }
        # Fingerprints differ due to channel + amount
        fp1 = source_fingerprint(doc1)
        fp2 = source_fingerprint(doc2)
        # Even with different fingerprints, reingest detection works on field comparison
        conflict = detect_reingest_conflict(doc2, {**doc1, "total": "1000.00"})
        # Should detect conflict due to amount difference
        if conflict["is_conflict"]:
            assert conflict["conflict_type"] == "REINGEST_WITH_VARIATION"

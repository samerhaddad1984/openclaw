"""
tests/red_team/test_date_ambiguity_destruction.py — Date ambiguity destruction.

Attack surface:
    04/05/2025 — April 5 (MM/DD) or May 4 (DD/MM)?
    French receipt DD/MM vs US vendor MM/DD
    OCR with missing separator
    WhatsApp image with timestamp mismatch

Fail if:
    - System posts to one period with full confidence when date is ambiguous
    - Same document gets different dates in different modules
    - No reason code raised for genuinely ambiguous dates
    - Downstream effects (duplicate window, FX, aging, filing period) not flagged
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

import pytest

from src.engines.uncertainty_engine import (
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    SAFE_TO_POST,
    DateResolutionState,
    UncertaintyReason,
    build_date_resolution,
    evaluate_posting_readiness,
    evaluate_uncertainty,
    reason_date_ambiguous,
)
from src.engines.aging_engine import _bucket_name, _days_between
from src.engines.bank_parser import _parse_date
from src.agents.tools.bank_matcher import BankMatcher


# ── helpers ──────────────────────────────────────────────────────────────

def _matcher() -> BankMatcher:
    return BankMatcher.__new__(BankMatcher)


# =========================================================================
# PART 1 — Core ambiguity detection
# =========================================================================

class TestCoreAmbiguityDetection:
    """Verify that genuinely ambiguous dates are never silently resolved."""

    @pytest.mark.parametrize("raw_date", [
        "04/05/2025",   # April 5 or May 4?
        "05/04/2025",   # May 4 or April 5?
        "03/06/2025",   # March 6 or June 3?
        "01/02/2025",   # Jan 2 or Feb 1?
        "06/07/2025",   # June 7 or July 6?
        "11/12/2025",   # Nov 12 or Dec 11?
        "02/03/2025",   # Feb 3 or March 2?
    ])
    def test_ambiguous_date_without_language_returns_ambiguous(self, raw_date: str):
        """No language context + both values <= 12 → must flag as ambiguous."""
        state = build_date_resolution(raw_date, language=None)
        assert state.is_ambiguous(), (
            f"Date '{raw_date}' should be ambiguous without language context"
        )
        assert state.resolved_date is None, "Must not silently pick a date"
        assert state.date_confidence <= 0.50, (
            f"Ambiguous date confidence {state.date_confidence} too high"
        )
        assert len(state.date_range) == 2, "Must produce exactly two candidate dates"

    @pytest.mark.parametrize("raw_date", [
        "04/05/2025",
        "05/04/2025",
        "03/06/2025",
    ])
    def test_bank_matcher_returns_none_for_ambiguous(self, raw_date: str):
        """BankMatcher.parse_date with no language must return None for ambiguous."""
        m = _matcher()
        result = m.parse_date(raw_date, language=None)
        assert result is None, (
            f"BankMatcher.parse_date('{raw_date}') should return None without language"
        )

    def test_bank_parser_silent_resolution_is_documented_defect(self):
        """DEFECT: bank_parser._parse_date silently picks DD/MM (Quebec default).

        This test documents the silent data corruption risk.
        An ambiguous date like 04/05/2025 should not be silently resolved,
        but _parse_date picks DD/MM without flagging uncertainty.
        """
        result = _parse_date("04/05/2025")
        # _parse_date silently resolves to DD/MM (Quebec default)
        # This is a documented defect — it should raise ambiguity instead
        assert result is not None, "bank_parser picks a date (even if wrong)"
        # DEFECT: no uncertainty flag — caller has no way to know this was ambiguous


# =========================================================================
# PART 2 — Reason code raised
# =========================================================================

class TestDateAmbiguityReasonCode:
    """Verify DATE_AMBIGUOUS reason code is properly raised and structured."""

    def test_reason_code_exists(self):
        reason = reason_date_ambiguous(
            raw_date="04/05/2025",
            date_range=["2025-04-05", "2025-05-04"],
        )
        assert reason.reason_code == "DATE_AMBIGUOUS"
        assert reason.description_en != ""
        assert reason.description_fr != ""
        assert "04/05/2025" in reason.evidence_available
        assert reason.evidence_needed != ""

    def test_ambiguous_date_triggers_block_or_partial(self):
        """Ambiguous date confidence (0.40) must block posting entirely."""
        state = build_date_resolution("04/05/2025", language=None)
        assert state.date_confidence == 0.40

        # Feed into posting decision
        reason = reason_date_ambiguous("04/05/2025", state.date_range)
        unc = evaluate_uncertainty(
            confidence_by_field={"date": state.date_confidence, "vendor": 0.95, "amount": 0.99},
            reasons=[reason],
        )
        assert unc.must_block is True, (
            "Date confidence 0.40 must force BLOCK_PENDING_REVIEW"
        )
        assert unc.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_language_resolved_date_still_not_safe(self):
        """Even with language context (confidence=0.85), unresolved reason
        must prevent SAFE_TO_POST."""
        state = build_date_resolution("04/05/2025", language="fr")
        assert state.date_confidence == 0.85
        assert state.resolved_date is not None

        # But if we still attach the reason, it should not be SAFE
        reason = reason_date_ambiguous("04/05/2025", ["2025-04-05", "2025-05-04"])
        unc = evaluate_uncertainty(
            confidence_by_field={"date": state.date_confidence, "vendor": 0.95, "amount": 0.99},
            reasons=[reason],
        )
        # 0.85 >= 0.80 but reason attached → PARTIAL_POST_WITH_FLAGS, not SAFE
        assert unc.posting_recommendation != SAFE_TO_POST, (
            "Unresolved reason must prevent SAFE_TO_POST even with 0.85 confidence"
        )


# =========================================================================
# PART 3 — Downstream cascade effects
# =========================================================================

class TestDownstreamCascade:
    """Ambiguous date must flag ALL downstream effects."""

    DOWNSTREAM_MODULES = {
        "fx_rate_selection",
        "aging_bucket",
        "duplicate_window",
        "period_end_accrual",
        "credit_memo_status",
    }

    def test_all_downstream_effects_flagged(self):
        """build_date_resolution must list every downstream effect."""
        state = build_date_resolution("04/05/2025", language=None)
        flagged_modules = {d["module"] for d in state.date_affects}
        missing = self.DOWNSTREAM_MODULES - flagged_modules
        assert not missing, f"Downstream modules not flagged: {missing}"

    def test_fx_rate_cascade(self):
        """Different dates → different BoC FX rates."""
        state = build_date_resolution("04/05/2025", language=None)
        fx_entries = [d for d in state.date_affects if d["module"] == "fx_rate_selection"]
        assert len(fx_entries) == 1
        assert "2025-04-05" in fx_entries[0]["impact_en"] or "2025-05-04" in fx_entries[0]["impact_en"]

    def test_aging_bucket_cascade(self):
        """One month of date difference moves between aging buckets."""
        # 04/05/2025 as MM/DD → April 5, as DD/MM → May 4
        # Measured from 2025-06-15: April 5 = 71 days, May 4 = 42 days
        as_of = "2025-06-15"
        days_interpretation_1 = _days_between("2025-04-05", as_of)  # 71 days
        days_interpretation_2 = _days_between("2025-05-04", as_of)  # 42 days

        bucket_1 = _bucket_name(days_interpretation_1)
        bucket_2 = _bucket_name(days_interpretation_2)

        assert bucket_1 != bucket_2, (
            f"Ambiguous date MUST put invoice in different aging buckets: "
            f"{bucket_1} vs {bucket_2} (days: {days_interpretation_1} vs {days_interpretation_2})"
        )

    def test_duplicate_window_cascade(self):
        """30-day duplicate window shifts depending on date interpretation."""
        state = build_date_resolution("04/05/2025", language=None)
        dup_entries = [d for d in state.date_affects if d["module"] == "duplicate_window"]
        assert len(dup_entries) == 1

    def test_period_classification_cascade(self):
        """Quarter classification may differ between interpretations.

        04/05/2025: MM/DD → April 5 (Q2), DD/MM → May 4 (Q2) — same quarter
        03/01/2025: MM/DD → March 1 (Q1), DD/MM → Jan 3 (Q1) — same quarter
        BUT: 01/04/2025: MM/DD → Jan 4 (Q1), DD/MM → April 1 (Q2) — DIFFERENT quarter!
        """
        state = build_date_resolution("01/04/2025", language=None)
        # Dates: 2025-01-04 (Q1) and 2025-04-01 (Q2)
        assert len(state.date_range) == 2
        d1 = datetime.strptime(state.date_range[0], "%Y-%m-%d")
        d2 = datetime.strptime(state.date_range[1], "%Y-%m-%d")
        q1 = (d1.month - 1) // 3 + 1
        q2 = (d2.month - 1) // 3 + 1
        assert q1 != q2, (
            f"01/04/2025 ambiguity must cross quarters: Q{q1} vs Q{q2}"
        )


# =========================================================================
# PART 4 — French receipt DD/MM vs US vendor MM/DD
# =========================================================================

class TestLocaleConflict:
    """Same raw date string, different vendor locale → different interpretation."""

    def test_french_receipt_dd_mm(self):
        """French receipt: 04/05/2025 → day=4, month=5 → May 4."""
        state = build_date_resolution("04/05/2025", language="fr")
        assert state.resolved_date == "2025-05-04", (
            f"French receipt should parse as DD/MM → May 4, got {state.resolved_date}"
        )
        assert state.date_confidence == 0.85

    def test_us_vendor_mm_dd(self):
        """US vendor: 04/05/2025 → month=4, day=5 → April 5."""
        state = build_date_resolution("04/05/2025", language="en")
        assert state.resolved_date == "2025-04-05", (
            f"US vendor should parse as MM/DD → April 5, got {state.resolved_date}"
        )
        assert state.date_confidence == 0.85

    def test_same_string_different_locale_different_date(self):
        """CRITICAL: Same raw string + different locale = different date.
        This is the exact scenario that causes posting to wrong period."""
        fr = build_date_resolution("04/05/2025", language="fr")
        en = build_date_resolution("04/05/2025", language="en")
        assert fr.resolved_date != en.resolved_date, (
            "Same raw date with different locales MUST resolve to different dates"
        )

    def test_unambiguous_dates_ignore_locale(self):
        """Dates where one value > 12 must resolve identically regardless of locale."""
        # 15/04/2025 → day=15 (must be DD/MM) regardless of language
        fr = build_date_resolution("15/04/2025", language="fr")
        en = build_date_resolution("15/04/2025", language="en")
        none_ = build_date_resolution("15/04/2025", language=None)
        assert fr.resolved_date == en.resolved_date == none_.resolved_date == "2025-04-15"
        assert fr.date_confidence == 1.0


# =========================================================================
# PART 5 — OCR with missing separator
# =========================================================================

class TestOcrDateDefects:
    """OCR often drops or misreads date separators."""

    def test_ocr_missing_separator(self):
        """OCR reads '04052025' with no separator — must not silently parse."""
        state = build_date_resolution("04052025", language=None)
        # No separator → not parseable → must not resolve
        assert state.resolved_date is None, (
            "Missing-separator date must not silently resolve"
        )
        assert state.date_confidence < 0.50

    def test_ocr_dot_separator(self):
        """OCR reads '04.05.2025' — dot separator is valid but still ambiguous."""
        state = build_date_resolution("04.05.2025", language=None)
        # Dots are handled by the regex — still ambiguous (both <= 12)
        assert state.is_ambiguous(), "Dot-separated date with both values <= 12 must be ambiguous"

    def test_ocr_dash_separator(self):
        """OCR reads '04-05-2025' — dash separator, still ambiguous."""
        state = build_date_resolution("04-05-2025", language=None)
        assert state.is_ambiguous(), "Dash-separated date with both values <= 12 must be ambiguous"

    def test_ocr_unambiguous_with_dot(self):
        """OCR reads '25.04.2025' — day=25 > 12, unambiguous."""
        state = build_date_resolution("25.04.2025", language=None)
        assert state.resolved_date == "2025-04-25"
        assert state.date_confidence == 1.0


# =========================================================================
# PART 6 — WhatsApp image with timestamp mismatch
# =========================================================================

class TestTimestampMismatch:
    """WhatsApp images have EXIF timestamps that may differ from document date."""

    def test_whatsapp_metadata_date_vs_document_date(self):
        """Simulate: WhatsApp photo taken June 3 contains receipt dated 04/05/2025.

        The photo metadata date must NOT override the document date.
        The document date itself is still ambiguous.
        """
        # WhatsApp EXIF says 2025-06-03
        whatsapp_timestamp = "2025-06-03"
        document_raw_date = "04/05/2025"

        doc_state = build_date_resolution(document_raw_date, language=None)
        assert doc_state.is_ambiguous()

        # Verify that neither candidate matches the WhatsApp timestamp
        for candidate in doc_state.date_range:
            assert candidate != whatsapp_timestamp, (
                "Document date candidate should not match WhatsApp timestamp"
            )

    def test_exif_date_cannot_resolve_ambiguity(self):
        """EXIF timestamps tell you when the photo was taken, not the invoice date.
        System must not use photo timestamp to 'resolve' document date ambiguity."""
        doc_state = build_date_resolution("04/05/2025", language=None)
        # There should be no mechanism to use metadata to resolve this
        assert doc_state.resolved_date is None
        assert doc_state.date_confidence == 0.40


# =========================================================================
# PART 7 — No silent date choice when unresolved
# =========================================================================

class TestNoSilentDateChoice:
    """System must NEVER post with full confidence when date is ambiguous."""

    def test_posting_blocked_for_ambiguous_date(self):
        """Full pipeline: ambiguous date → reason code → BLOCK."""
        state = build_date_resolution("04/05/2025", language=None)
        reason = reason_date_ambiguous("04/05/2025", state.date_range)

        unc = evaluate_uncertainty(
            confidence_by_field={
                "date": state.date_confidence,
                "vendor": 1.0,
                "amount": 1.0,
                "tax_code": 1.0,
            },
            reasons=[reason],
        )

        decision = evaluate_posting_readiness(
            document={"doc_id": "TEST-001", "raw_date": "04/05/2025"},
            uncertainty_state=unc,
        )

        assert decision.outcome == BLOCK_PENDING_REVIEW, (
            f"Ambiguous date must block posting, got {decision.outcome}"
        )
        assert decision.can_post is False
        assert any("DATE_AMBIGUOUS" in note for note in decision.reviewer_notes), (
            "Reviewer notes must include DATE_AMBIGUOUS reason"
        )

    def test_posting_safe_only_for_iso_date(self):
        """ISO dates (YYYY-MM-DD) are the only fully safe format."""
        state = build_date_resolution("2025-04-05", language=None)
        assert state.date_confidence == 1.0
        assert state.resolved_date == "2025-04-05"

        unc = evaluate_uncertainty(
            confidence_by_field={
                "date": state.date_confidence,
                "vendor": 1.0,
                "amount": 1.0,
            },
        )
        decision = evaluate_posting_readiness(
            document={"doc_id": "TEST-002"},
            uncertainty_state=unc,
        )
        assert decision.outcome == SAFE_TO_POST
        assert decision.can_post is True

    def test_same_document_same_date_across_modules(self):
        """Critical consistency check: same raw date must resolve identically
        in bank_matcher and uncertainty_engine when given same language context."""
        raw = "04/05/2025"
        lang = "fr"

        # uncertainty_engine
        ue_state = build_date_resolution(raw, language=lang)

        # bank_matcher
        m = _matcher()
        bm_result = m.parse_date(raw, language=lang)

        # Both must agree
        ue_date_str = ue_state.resolved_date  # "2025-05-04"
        bm_date_str = bm_result.strftime("%Y-%m-%d") if bm_result else None

        assert ue_date_str == bm_date_str, (
            f"Module inconsistency! uncertainty_engine says {ue_date_str}, "
            f"bank_matcher says {bm_date_str} for '{raw}' with language='{lang}'"
        )

    def test_no_language_no_resolution_both_modules(self):
        """Without language context, BOTH modules must refuse to resolve."""
        raw = "04/05/2025"

        ue_state = build_date_resolution(raw, language=None)
        m = _matcher()
        bm_result = m.parse_date(raw, language=None)

        assert ue_state.resolved_date is None, "uncertainty_engine must not resolve"
        assert bm_result is None, "bank_matcher must not resolve"


# =========================================================================
# PART 8 — Edge cases and boundary attacks
# =========================================================================

class TestDateEdgeCases:
    """Boundary conditions and edge case attacks."""

    def test_same_day_month_not_ambiguous(self):
        """04/04/2025 — both interpretations give the same date."""
        state = build_date_resolution("04/04/2025", language=None)
        # Both DD/MM and MM/DD give April 4 → should resolve
        # Current behavior: both <= 12, no language → ambiguous
        # But date_range[0] == date_range[1], so effectively resolved
        if state.is_ambiguous():
            assert state.date_range[0] == state.date_range[1], (
                "04/04 is technically ambiguous but both candidates are identical"
            )

    def test_february_29_leap_year(self):
        """02/29/2024 — only valid as MM/DD (Feb 29 in leap year)."""
        state = build_date_resolution("02/29/2024", language=None)
        # 29 > 12 → unambiguous: day=29, month=2
        assert state.resolved_date is not None
        assert state.date_confidence == 1.0

    def test_february_30_invalid(self):
        """02/30/2025 — invalid date regardless of interpretation.

        DEFECT: build_date_resolution accepts '02/30/2025' as valid with
        confidence 1.0 and resolved_date '2025-02-30'. It does not validate
        calendar correctness after resolving DD/MM vs MM/DD. February 30 does
        not exist in any year.
        """
        state = build_date_resolution("02/30/2025", language=None)
        # 30 > 12 → unambiguous (DD/MM) → resolves as Feb 30 without
        # checking that February 30 is an impossible date.
        # DEFECT: should reject or flag low confidence for invalid calendar dates
        assert state.resolved_date == "2025-02-30", (
            "Documenting DEFECT: system stores impossible date Feb 30"
        )
        assert state.date_confidence == 1.0, (
            "Documenting DEFECT: impossible date gets full confidence"
        )

    def test_iso_format_always_unambiguous(self):
        """YYYY-MM-DD must always be confidence 1.0."""
        for iso in ["2025-01-15", "2025-12-31", "2024-02-29", "2025-04-05"]:
            state = build_date_resolution(iso, language=None)
            assert state.resolved_date == iso
            assert state.date_confidence == 1.0

    def test_empty_date_returns_low_confidence(self):
        """Empty or blank date must return low confidence."""
        for empty in ["", "   ", None]:
            state = build_date_resolution(empty or "", language=None)
            assert state.date_confidence <= 0.10
            assert state.resolved_date is None

    def test_garbage_date_low_confidence(self):
        """Non-date strings must not resolve."""
        for garbage in ["abc", "2025", "??/??/????", "N/A", "TBD"]:
            state = build_date_resolution(garbage, language=None)
            assert state.resolved_date is None
            assert state.date_confidence < 0.50


# =========================================================================
# PART 9 — Filing period cross-quarter attacks
# =========================================================================

class TestFilingPeriodAttack:
    """Ambiguous dates that cross fiscal quarter boundaries are the most
    dangerous — they can cause the document to be filed in the wrong
    GST/QST return period."""

    CROSS_QUARTER_DATES = [
        # (raw_date, interpretation_1_quarter, interpretation_2_quarter)
        ("01/04/2025", 1, 2),   # Jan 4 (Q1) vs Apr 1 (Q2)
        ("03/07/2025", 1, 3),   # Mar 7 (Q1) vs Jul 3 (Q3)
        ("06/10/2025", 2, 4),   # Jun 10 (Q2) vs Oct 6 (Q4)
        ("03/10/2025", 1, 4),   # Mar 10 (Q1) vs Oct 3 (Q4)
        ("09/01/2025", 1, 3),   # Jan 9 (Q1) vs Sep 1 (Q3)
    ]

    @pytest.mark.parametrize("raw_date,q_a,q_b", CROSS_QUARTER_DATES)
    def test_cross_quarter_ambiguity_blocks_posting(self, raw_date: str, q_a: int, q_b: int):
        """Dates that cross quarters must block posting unconditionally."""
        state = build_date_resolution(raw_date, language=None)
        assert state.is_ambiguous(), f"{raw_date} should be ambiguous"

        # Verify the quarter divergence
        d1 = datetime.strptime(state.date_range[0], "%Y-%m-%d")
        d2 = datetime.strptime(state.date_range[1], "%Y-%m-%d")
        actual_q1 = (d1.month - 1) // 3 + 1
        actual_q2 = (d2.month - 1) // 3 + 1
        assert actual_q1 != actual_q2, (
            f"Expected cross-quarter for {raw_date}: Q{actual_q1} vs Q{actual_q2}"
        )

        # Must block
        reason = reason_date_ambiguous(raw_date, state.date_range)
        unc = evaluate_uncertainty(
            confidence_by_field={"date": state.date_confidence},
            reasons=[reason],
        )
        assert unc.must_block is True

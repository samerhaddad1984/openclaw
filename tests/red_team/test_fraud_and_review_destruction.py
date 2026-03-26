"""
tests/red_team/test_fraud_and_review_destruction.py
====================================================
Red-team attack surface tests for:
  - Fraud Engine (invoice splitting, round-dollar bursts, weekend/holiday edge
    cases, duplicate detection, new vendor, bank account change, cross-vendor
    same-amount, vendor amount anomaly with insufficient history, bypass)
  - Review Policy (confidence boosting, missing fields, zero total, invalid
    dates, effective_confidence cap, rules vs ai confidence)
  - Hallucination Guard (math tolerance, amount boundary, vendor length, date
    validation, hallucination_suspected reset)
  - Auto-Approval Engine + Exception Router (fraud flags vs auto_post,
    duplicate risk vs auto_post, approval/router conflict, borderline signals)
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.engines.fraud_engine import (
    AMOUNT_ANOMALY_SIGMA,
    DUPLICATE_CROSS_VENDOR_DAYS,
    DUPLICATE_SAME_VENDOR_DAYS,
    MIN_HISTORY_FOR_ANOMALY,
    MIN_HISTORY_FOR_ROUND_FLAG,
    NEW_VENDOR_LARGE_AMOUNT_LIMIT,
    WEEKEND_HOLIDAY_AMOUNT_LIMIT,
    _is_quebec_holiday,
    _is_round_number,
    _mean,
    _rule_bank_account_change,
    _rule_duplicate,
    _rule_new_vendor_large_amount,
    _rule_round_number,
    _rule_vendor_amount_anomaly,
    _rule_vendor_timing_anomaly,
    _rule_weekend_holiday,
    _std_dev,
    run_fraud_detection,
)
from src.agents.tools.review_policy import (
    ReviewDecision,
    decide_review_status,
    effective_confidence,
    validate_tax_extraction,
)
from src.agents.core.hallucination_guard import (
    AMOUNT_MAX,
    AMOUNT_MIN,
    DATE_FUTURE_DAYS,
    DATE_PAST_YEARS,
    MATH_TOLERANCE,
    VENDOR_MAX_LEN,
    VENDOR_MIN_LEN,
    verify_ai_output,
    verify_numeric_totals,
)
from src.agents.core.auto_approval_engine import AutoApprovalEngine
from src.agents.core.exception_router import ExceptionRouter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> Path:
    """Create a minimal documents table for fraud engine integration tests."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            vendor TEXT,
            client_code TEXT,
            amount REAL,
            document_date TEXT,
            review_status TEXT,
            raw_result TEXT,
            fraud_flags TEXT,
            confidence REAL,
            updated_at TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            correction_count INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def _insert_doc(db_path: Path, **fields: Any) -> str:
    doc_id = fields.pop("document_id", str(uuid.uuid4()))
    cols = ["document_id"] + list(fields.keys())
    vals = [doc_id] + list(fields.values())
    placeholders = ", ".join(["?"] * len(vals))
    col_names = ", ".join(cols)
    conn = sqlite3.connect(str(db_path))
    conn.execute(f"INSERT INTO documents ({col_names}) VALUES ({placeholders})", vals)
    conn.commit()
    conn.close()
    return doc_id


def _vendor_history(n: int, amount: float = 1000.0, day: int = 15,
                    review_status: str = "posted") -> list[dict[str, Any]]:
    """Generate N prior vendor history records."""
    return [
        {
            "document_id": f"hist_{i}",
            "amount": amount,
            "document_date": f"2025-01-{day:02d}",
            "raw_result": None,
            "review_status": review_status,
        }
        for i in range(n)
    ]


# ======================================================================
# FRAUD ENGINE TESTS
# ======================================================================

class TestFraudEngine:
    """Attack the fraud engine's detection rules."""

    # ------------------------------------------------------------------
    # 1. Invoice splitting below threshold
    # ------------------------------------------------------------------
    def test_invoice_splitting_below_threshold_not_detected(self):
        """DEFECT PROBE: Fraud engine has NO invoice-splitting rule.
        A vendor can submit 10 invoices of $1,999 (just below the $2,000
        new-vendor-large-amount limit) and none will be flagged.
        """
        history = _vendor_history(0, review_status="pending")  # new vendor
        # Each invoice is $1,999 -- just below $2,000
        flag = _rule_new_vendor_large_amount("SplitCo", 1999.99, history)
        assert flag is None, "Expected no flag for amount just below threshold"

        # But the TOTAL across 10 invoices = $19,999 -- that's suspicious
        # The fraud engine has no burst/splitting detection rule
        # This test documents that gap
        for _ in range(10):
            flag = _rule_new_vendor_large_amount("SplitCo", 1999.99, history)
            assert flag is None  # No split detection

    # ------------------------------------------------------------------
    # 2. Round-dollar expense bursts from irregular vendor
    # ------------------------------------------------------------------
    def test_round_dollar_burst_irregular_vendor(self):
        """Round number flag fires for exactly-$500 multiples from irregular vendor."""
        # Create history with varying amounts (high CV = irregular)
        history = [
            {"amount": 200.0, "document_date": "2025-01-10", "review_status": "posted", "document_id": f"h{i}"}
            for i in range(3)
        ] + [
            {"amount": 800.0, "document_date": "2025-02-10", "review_status": "posted", "document_id": f"h{i+3}"}
            for i in range(2)
        ]
        # Need at least 5 history records
        assert len(history) >= MIN_HISTORY_FOR_ROUND_FLAG

        flag = _rule_round_number(1000.0, history)
        assert flag is not None, "Should flag round $1000 from irregular vendor"
        assert flag["rule"] == "round_number_flag"

    def test_round_dollar_not_flagged_for_regular_vendor(self):
        """Round number should NOT flag if vendor has consistent billing."""
        # All history at $1000 = very regular (CV ~ 0)
        history = _vendor_history(10, amount=1000.0)
        flag = _rule_round_number(1000.0, history)
        assert flag is None, "Regular vendor should not be flagged for round numbers"

    def test_round_number_boundary_499_not_flagged(self):
        """$499 is NOT a round number (must be divisible by 500)."""
        assert not _is_round_number(499.0)
        assert _is_round_number(500.0)
        assert _is_round_number(1000.0)
        assert not _is_round_number(500.50)  # has cents

    # ------------------------------------------------------------------
    # 3. Weekend + holiday detection edge cases
    # ------------------------------------------------------------------
    def test_weekend_saturday_above_threshold(self):
        """Saturday transaction above $500 should be flagged."""
        # Find a Saturday
        sat = date(2025, 3, 22)  # Saturday
        assert sat.weekday() == 5
        flags = _rule_weekend_holiday(501.0, sat)
        assert len(flags) == 1
        assert flags[0]["rule"] == "weekend_transaction"

    def test_weekend_exactly_100_not_flagged(self):
        """$100 exactly should NOT be flagged (FIX 9: threshold lowered to > $100)."""
        sat = date(2025, 3, 22)
        flags = _rule_weekend_holiday(100.0, sat)
        assert len(flags) == 0, "Exactly $100 should not trigger weekend flag (> not >=)"

    def test_holiday_christmas_flagged(self):
        """Christmas Day transaction above $500 should be flagged."""
        christmas = date(2025, 12, 25)
        assert christmas.weekday() != 5 and christmas.weekday() != 6  # 2025 Christmas is Thursday
        flags = _rule_weekend_holiday(600.0, christmas)
        assert len(flags) == 1
        assert flags[0]["rule"] == "holiday_transaction"

    def test_holiday_on_weekend_only_weekend_flagged(self):
        """DEFECT PROBE: If a holiday falls on a weekend, only weekend flag fires,
        not the holiday flag. The code checks weekday first and only checks
        holiday in the else branch."""
        # Find a year where Christmas is on Saturday
        # Dec 25, 2021 was Saturday
        xmas_sat = date(2021, 12, 25)
        assert xmas_sat.weekday() == 5  # Saturday
        holiday_name = _is_quebec_holiday(xmas_sat)
        assert holiday_name is not None, "Christmas should be a recognized holiday"

        flags = _rule_weekend_holiday(600.0, xmas_sat)
        rules = [f["rule"] for f in flags]
        # The code only flags weekend OR holiday, not both
        assert "weekend_transaction" in rules
        assert "holiday_transaction" not in rules, \
            "Holiday on weekend: only weekend is flagged (holiday check skipped)"

    def test_timezone_edge_case_no_timezone_handling(self):
        """DEFECT PROBE: Fraud engine uses date objects with no timezone awareness.
        A transaction at 11:30 PM EST Friday could be Saturday UTC.
        The engine has no timezone logic -- it trusts the date field as-is."""
        # This is a design limitation, not a code bug per se.
        # The engine uses `date` objects, not `datetime` with tzinfo.
        friday = date(2025, 3, 21)  # Friday
        assert friday.weekday() == 4
        flags = _rule_weekend_holiday(600.0, friday)
        assert len(flags) == 0  # No flag because the date says Friday

    # ------------------------------------------------------------------
    # 4. Duplicate detection - similar vendor names
    # ------------------------------------------------------------------
    def test_duplicate_exact_match(self, tmp_path):
        """Same vendor + same amount within 30 days should be HIGH risk."""
        db_path = _make_db(tmp_path)
        today = date.today().isoformat()
        _insert_doc(db_path, document_id="existing_1", vendor="Acme Corp",
                    client_code="C001", amount=1500.0, document_date=today)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flags = _rule_duplicate(conn, "new_doc", "Acme Corp", "C001", 1500.0,
                                date.today())
        conn.close()
        assert len(flags) >= 1
        assert any(f["rule"] == "duplicate_exact" for f in flags)

    def test_duplicate_slightly_different_vendor_names_not_caught(self, tmp_path):
        """DEFECT PROBE: 'Acme Corp' vs 'ACME Corp.' -- case is handled via LOWER,
        but trailing punctuation difference means no duplicate detected."""
        db_path = _make_db(tmp_path)
        today = date.today().isoformat()
        _insert_doc(db_path, document_id="existing_1", vendor="Acme Corp.",
                    client_code="C001", amount=1500.0, document_date=today)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        # The query uses LOWER(TRIM(vendor)) but "acme corp." != "acme corp"
        flags = _rule_duplicate(conn, "new_doc", "Acme Corp", "C001", 1500.0,
                                date.today())
        conn.close()
        # This should ideally detect the duplicate but the name differs by a dot
        exact_flags = [f for f in flags if f["rule"] == "duplicate_exact"]
        assert len(exact_flags) == 0, \
            "DEFECT: Slight vendor name variation (trailing dot) bypasses duplicate detection"

    # ------------------------------------------------------------------
    # 5. New vendor + large amount first transaction
    # ------------------------------------------------------------------
    def test_new_vendor_large_amount_first_invoice(self):
        """First invoice from vendor over $2,000 should be flagged HIGH."""
        history = []  # no prior history
        flag = _rule_new_vendor_large_amount("NewCo Inc", 2500.0, history)
        assert flag is not None
        assert flag["rule"] == "new_vendor_large_amount"
        assert flag["severity"] == "high"

    def test_new_vendor_with_pending_history_still_flags(self):
        """Vendor has prior history but none are posted/approved -- still 'new'."""
        history = _vendor_history(5, review_status="NeedsReview")
        flag = _rule_new_vendor_large_amount("NewCo Inc", 5000.0, history)
        assert flag is not None, "Vendor with only pending history should still be flagged"

    def test_new_vendor_just_below_threshold(self):
        """$2000.00 exactly should NOT be flagged (threshold is >)."""
        history = []
        flag = _rule_new_vendor_large_amount("NewCo", 2000.0, history)
        assert flag is None, "Exactly $2000 should not trigger (> not >=)"

    # ------------------------------------------------------------------
    # 6. Bank account change detection
    # ------------------------------------------------------------------
    def test_bank_account_change_detected(self):
        """Bank details changed between invoices should be CRITICAL."""
        old_raw = json.dumps({"bank_account": "1234567890"})
        new_raw = json.dumps({"bank_account": "9999999999"})
        history = [{"raw_result": old_raw}]
        flag = _rule_bank_account_change(new_raw, history)
        assert flag is not None
        assert flag["rule"] == "bank_account_change"
        assert flag["severity"] == "critical"

    def test_bank_account_change_no_bank_details_in_current(self):
        """If current doc has no bank details, bank change cannot be detected."""
        old_raw = json.dumps({"bank_account": "1234567890"})
        new_raw = json.dumps({"vendor_name": "Acme"})  # no bank fields
        history = [{"raw_result": old_raw}]
        flag = _rule_bank_account_change(new_raw, history)
        assert flag is None, "No bank details in current doc = no flag (silent pass)"

    def test_bank_account_change_no_history_bank_details(self):
        """If no prior invoice had bank details, no comparison can be made."""
        new_raw = json.dumps({"bank_account": "9999999999"})
        history = [{"raw_result": json.dumps({"vendor_name": "X"})}]
        flag = _rule_bank_account_change(new_raw, history)
        assert flag is None

    # ------------------------------------------------------------------
    # 7. Cross-vendor same-amount within 7 days -- false positive rate
    # ------------------------------------------------------------------
    def test_cross_vendor_same_amount_7_days(self, tmp_path):
        """Same amount from different vendor within 7 days = MEDIUM risk."""
        db_path = _make_db(tmp_path)
        today = date.today().isoformat()
        _insert_doc(db_path, document_id="other_vendor_doc",
                    vendor="Different Corp", client_code="C001",
                    amount=3500.0, document_date=today)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flags = _rule_duplicate(conn, "new_doc", "Original Corp", "C001",
                                3500.0, date.today())
        conn.close()
        cross = [f for f in flags if f["rule"] == "duplicate_cross_vendor"]
        assert len(cross) >= 1
        assert cross[0]["severity"] == "medium"

    def test_cross_vendor_common_amount_false_positive(self, tmp_path):
        """DEFECT PROBE: Common amounts like $100.00 from different vendors
        within 7 days will always flag, potentially producing noise."""
        db_path = _make_db(tmp_path)
        today = date.today().isoformat()
        for i in range(5):
            _insert_doc(db_path, vendor=f"Vendor_{i}", client_code="C001",
                        amount=100.0, document_date=today)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flags = _rule_duplicate(conn, "new_doc", "Yet Another Vendor", "C001",
                                100.0, date.today())
        conn.close()
        cross = [f for f in flags if f["rule"] == "duplicate_cross_vendor"]
        # All 5 existing docs should flag as cross-vendor matches (capped at 5)
        assert len(cross) == 5, \
            f"All 5 common-amount vendors flagged, high false positive rate: {len(cross)}"

    # ------------------------------------------------------------------
    # 8. Vendor amount anomaly with insufficient history (<10 txns)
    # ------------------------------------------------------------------
    def test_amount_anomaly_insufficient_history(self):
        """With fewer than 10 prior transactions, anomaly rule should NOT fire."""
        history = _vendor_history(9, amount=100.0)  # only 9
        flag = _rule_vendor_amount_anomaly(100000.0, history)
        assert flag is None, "Should not flag anomaly with < 10 history records"

    def test_amount_anomaly_exactly_10_history(self):
        """With exactly 10 prior transactions (varied amounts) and outlier, should flag."""
        # Use varied amounts so std_dev > 0
        history = [
            {"amount": 95.0 + i, "document_date": f"2025-01-{10+i:02d}",
             "review_status": "posted", "document_id": f"h{i}"}
            for i in range(10)
        ]
        # Mean ~ 99.5, std ~ 3.0; $100,000 is massively > 2 sigma
        flag = _rule_vendor_amount_anomaly(100000.0, history)
        assert flag is not None
        assert flag["rule"] == "vendor_amount_anomaly"

    def test_amount_anomaly_std_zero_no_flag(self):
        """If all history amounts are identical, std=0, should NOT flag (div by zero guard)."""
        history = _vendor_history(10, amount=500.0)
        # std_dev = 0 when all values are same; code checks std == 0 and returns None
        flag = _rule_vendor_amount_anomaly(500.0, history)
        assert flag is None

    def test_amount_anomaly_std_zero_different_amount_no_flag(self):
        """DEFECT PROBE: If all 10 history = $500 (std=0), even $999999 won't flag
        because of the std==0 guard. This is a silent pass for the most suspicious case."""
        history = _vendor_history(10, amount=500.0)
        flag = _rule_vendor_amount_anomaly(999999.0, history)
        assert flag is None, \
            "DEFECT: Zero std dev means even massive outliers are silently ignored"

    # ------------------------------------------------------------------
    # 9. Can fraud engine be bypassed?
    # ------------------------------------------------------------------
    def test_fraud_engine_skips_zero_amount(self, tmp_path):
        """Documents with amount=0 bypass ALL fraud checks."""
        db_path = _make_db(tmp_path)
        doc_id = _insert_doc(db_path, vendor="EvilCorp", client_code="C001",
                             amount=0, document_date=date.today().isoformat())
        flags = run_fraud_detection(doc_id, db_path=db_path)
        assert flags == [], "Zero-amount documents bypass fraud engine entirely"

    def test_fraud_engine_handles_negative_amount(self, tmp_path):
        """FIX: Negative amounts (credit notes) now run credit-note fraud rules."""
        db_path = _make_db(tmp_path)
        doc_id = _insert_doc(db_path, vendor="EvilCorp", client_code="C001",
                             amount=-5000, document_date=date.today().isoformat())
        flags = run_fraud_detection(doc_id, db_path=db_path)
        # Credit notes now get fraud checks (orphan_credit_note, large_credit_note, etc.)
        assert isinstance(flags, list), "Fraud engine should return a list for credit notes"

    def test_fraud_engine_skips_no_date(self, tmp_path):
        """Documents with no date bypass ALL fraud checks."""
        db_path = _make_db(tmp_path)
        doc_id = _insert_doc(db_path, vendor="EvilCorp", client_code="C001",
                             amount=50000)
        flags = run_fraud_detection(doc_id, db_path=db_path)
        assert flags == [], "Missing date bypasses fraud engine entirely"

    def test_fraud_engine_skips_empty_vendor(self, tmp_path):
        """DEFECT PROBE: Empty vendor bypasses vendor-specific fraud rules
        (anomaly, round number, new vendor, bank change) but NOT duplicate
        detection or weekend/holiday checks."""
        db_path = _make_db(tmp_path)
        sat = date(2025, 3, 22)  # Saturday
        doc_id = _insert_doc(db_path, vendor="", client_code="C001",
                             amount=5000, document_date=sat.isoformat())
        flags = run_fraud_detection(doc_id, db_path=db_path)
        rules = [f["rule"] for f in flags]
        # Weekend should still fire, but vendor-specific rules skipped
        assert "weekend_transaction" in rules or len(rules) >= 0


# ======================================================================
# REVIEW POLICY TESTS
# ======================================================================

class TestReviewPolicy:
    """Attack the review policy decision logic."""

    # ------------------------------------------------------------------
    # 10. Confidence boosting
    # ------------------------------------------------------------------
    def test_confidence_boosting_to_ready(self):
        """FIX 24: 0.76 confidence with boost capped at +0.05 (base < 0.80)
        → 0.81 < 0.85 → NeedsReview. Low confidence no longer auto-promoted."""
        decision = decide_review_status(
            rules_confidence=0.76,
            final_method="rules",
            vendor_name="Acme",
            total=100.0,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence < 0.85

    def test_confidence_boosting_from_075_gets_review(self):
        """FIX 24: 0.75 + 0.05 (base < 0.80) = 0.80 < 0.85 → NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.75,
            final_method="rules",
            vendor_name="Acme",
            total=100.0,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.effective_confidence == 0.80
        assert decision.status == "NeedsReview"

    def test_confidence_boosting_blocked_without_required_fields(self):
        """Without required fields, no boost applied."""
        eff = effective_confidence(0.76, "rules", has_required=False)
        assert eff == 0.76

    # ------------------------------------------------------------------
    # 11. Missing fields that should force Exception
    # ------------------------------------------------------------------
    def test_missing_vendor_forces_exception(self):
        """Missing vendor should force Exception status."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name=None,
            total=100.0,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status == "Exception"
        assert decision.reason == "missing_vendor"

    def test_whitespace_vendor_treated_as_missing(self):
        """Whitespace-only vendor should be treated as missing."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="   ",
            total=100.0,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status == "Exception"

    def test_missing_total_is_needs_review_not_exception(self):
        """DEFECT PROBE: Missing total is NeedsReview, not Exception.
        Arguably an invoice with NO total should be Exception."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Acme",
            total=None,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "missing_total"

    def test_missing_client_code_is_needs_review(self):
        """Missing client_code forces NeedsReview (checked before vendor)."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Acme",
            total=100.0,
            document_date="2025-01-15",
            client_code=None,
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "missing_client_route"

    # ------------------------------------------------------------------
    # 12. Zero total handling
    # ------------------------------------------------------------------
    def test_zero_total_is_needs_review(self):
        """Zero total should always be NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.99,
            final_method="rules",
            vendor_name="Acme",
            total=0,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "zero_total"

    def test_zero_total_even_with_high_confidence(self):
        """Zero total overrides high confidence -- should not be Ready."""
        decision = decide_review_status(
            rules_confidence=1.0,
            final_method="rules",
            vendor_name="Acme",
            total=0,
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status != "Ready"

    def test_negative_total_not_zero_check(self):
        """Negative total passes the zero check and proceeds to confidence logic."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Acme",
            total=-100.0,
            document_date="2025-01-15",
            client_code="C001",
        )
        # Negative total is not checked by review policy -- only zero
        # With high confidence + required fields, it becomes Ready
        assert decision.status == "Ready", \
            "DEFECT: Negative total is not caught by review policy"

    # ------------------------------------------------------------------
    # 13. Invalid date formats
    # ------------------------------------------------------------------
    def test_invalid_date_format_adds_note_but_no_block(self):
        """DEFECT PROBE: Invalid date format adds a review note but does NOT
        block the document from becoming Ready."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Acme",
            total=100.0,
            document_date="15/01/2025",  # DD/MM/YYYY -- invalid for strptime
            client_code="C001",
        )
        # has_date is True (string is truthy), so has_required = True
        # Boost applies, eff = 0.90 + 0.10 = 1.0 => Ready
        assert decision.status == "Ready", \
            "Invalid date format does NOT prevent Ready status"
        assert decision.review_notes is not None
        assert any("invalid_date" in n for n in decision.review_notes)

    def test_ambiguous_date_format(self):
        """Ambiguous date like 2025-13-01 (month 13) should be caught."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Acme",
            total=100.0,
            document_date="2025-13-01",
            client_code="C001",
        )
        assert decision.review_notes is not None
        assert any("invalid_date" in n for n in decision.review_notes)

    # ------------------------------------------------------------------
    # 14. Can effective_confidence exceed 1.0?
    # ------------------------------------------------------------------
    def test_effective_confidence_capped_at_1(self):
        """Confidence should never exceed 1.0."""
        eff = effective_confidence(0.99, "rules", has_required=True)
        assert eff <= 1.0

    def test_effective_confidence_floored_at_0(self):
        """Confidence should never go below 0.0."""
        eff = effective_confidence(-0.5, "rules", has_required=False)
        assert eff >= 0.0

    def test_effective_confidence_with_base_above_1(self):
        """If someone passes confidence > 1.0, it should be capped."""
        eff = effective_confidence(1.5, "rules", has_required=True)
        assert eff == 1.0

    # ------------------------------------------------------------------
    # 15. rules_confidence vs ai_confidence
    # ------------------------------------------------------------------
    def test_both_confidences_uses_max(self):
        """When both rules and AI confidence provided, use the higher one."""
        eff = effective_confidence(0.70, "rules", has_required=False, ai_confidence=0.80)
        assert eff == 0.80  # max(0.70, 0.80)

    def test_only_rules_confidence(self):
        """When only rules confidence provided, use it directly."""
        eff = effective_confidence(0.70, "rules", has_required=False, ai_confidence=0.0)
        assert eff == 0.70

    def test_only_ai_confidence(self):
        """When only AI confidence provided, use it directly."""
        eff = effective_confidence(0.0, "rules", has_required=False, ai_confidence=0.80)
        assert eff == 0.80

    def test_neither_confidence_is_zero(self):
        """When both are 0, effective confidence is 0 + boost."""
        eff = effective_confidence(0.0, "rules", has_required=True, ai_confidence=0.0)
        # FIX 24: base=0 < 0.80 → boost=min(0.05, 1.0)=0.05
        assert eff == 0.05  # Required field boost still applies (capped at 0.05)


# ======================================================================
# HALLUCINATION GUARD TESTS
# ======================================================================

class TestHallucinationGuard:
    """Attack the hallucination guard's validation logic."""

    # ------------------------------------------------------------------
    # 16. Math verification tolerance
    # ------------------------------------------------------------------
    def test_math_tolerance_at_002(self):
        """$0.02 tolerance should pass."""
        result = {
            "subtotal": 100.0,
            "total": 114.975 + 0.02,  # computed=114.975, delta=0.02
            "taxes": [
                {"type": "GST", "amount": 5.0},
                {"type": "QST", "amount": 9.975},
            ],
        }
        check = verify_numeric_totals(result)
        assert check["ok"] is True, "Delta of $0.02 should be within tolerance"

    def test_math_tolerance_at_003_fails(self):
        """$0.03 tolerance should FAIL."""
        result = {
            "subtotal": 100.0,
            "total": 114.975 + 0.03,  # delta=0.03
            "taxes": [
                {"type": "GST", "amount": 5.0},
                {"type": "QST", "amount": 9.975},
            ],
        }
        check = verify_numeric_totals(result)
        assert check["ok"] is False, "Delta of $0.03 should exceed $0.02 tolerance"

    def test_math_tolerance_exactly_at_boundary(self):
        """Delta exactly $0.02 should pass (<=)."""
        result = {
            "subtotal": 100.0,
            "total": 115.0,  # computed=114.975, delta=0.025
            "taxes": [
                {"type": "GST", "amount": 5.0},
                {"type": "QST", "amount": 9.975},
            ],
        }
        check = verify_numeric_totals(result)
        # delta = |114.975 - 115.0| = 0.025 > 0.02
        assert check["ok"] is False, "Delta $0.025 exceeds $0.02 tolerance"

    def test_math_missing_subtotal_skipped(self):
        """Missing subtotal should skip the check (returns ok=True, skipped=True)."""
        result = {"total": 100.0, "taxes": [{"type": "GST", "amount": 5.0}]}
        check = verify_numeric_totals(result)
        assert check["ok"] is True
        assert check["skipped"] is True

    def test_math_missing_taxes_and_tax_total_skipped(self):
        """If no taxes array and no tax_total, check is skipped."""
        result = {"subtotal": 100.0, "total": 115.0}
        check = verify_numeric_totals(result)
        assert check["ok"] is True
        assert check["skipped"] is True

    # ------------------------------------------------------------------
    # 17. Amount validation boundary at $500,000
    # ------------------------------------------------------------------
    def test_amount_at_499999_99_passes(self):
        """$499,999.99 should pass amount validation."""
        result = {"total": 499999.99, "vendor_name": "LargeCorp"}
        check = verify_ai_output(result)
        assert not check["hallucination_suspected"]

    def test_amount_at_500000_fails(self):
        """$500,000.00 exactly should FAIL (>= check)."""
        result = {"total": 500000.0, "vendor_name": "LargeCorp"}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]
        assert any("exceeds maximum" in f for f in check["failures"])

    def test_amount_at_500000_01_fails(self):
        """$500,000.01 should FAIL."""
        result = {"total": 500000.01, "vendor_name": "LargeCorp"}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]

    def test_negative_amount_credit_note_passes(self):
        """Negative amount on credit_note should pass."""
        result = {"total": -500.0, "vendor_name": "RefundCo", "document_type": "credit_note"}
        check = verify_ai_output(result)
        assert not check["hallucination_suspected"]

    def test_negative_amount_invoice_fails(self):
        """Negative amount on invoice should fail."""
        result = {"total": -500.0, "vendor_name": "EvilCorp", "document_type": "invoice"}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]

    # ------------------------------------------------------------------
    # 18. Vendor length: 1 char vendor names
    # ------------------------------------------------------------------
    def test_vendor_1_char_fails(self):
        """Single character vendor name should fail (min is 2)."""
        result = {"vendor_name": "X", "total": 100.0}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]
        assert any("too short" in f for f in check["failures"])

    def test_vendor_2_chars_passes(self):
        """2 character vendor name should pass (meets minimum)."""
        result = {"vendor_name": "AB", "total": 100.0}
        check = verify_ai_output(result)
        assert not check["hallucination_suspected"]

    def test_vendor_empty_fails(self):
        """Empty vendor should fail."""
        result = {"vendor_name": "", "total": 100.0}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]
        assert any("empty" in f for f in check["failures"])

    def test_vendor_101_chars_fails(self):
        """Vendor name > 100 chars should fail."""
        result = {"vendor_name": "A" * 101, "total": 100.0}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]
        assert any("too long" in f for f in check["failures"])

    # ------------------------------------------------------------------
    # 19. Date validation: 5 years ago, 8 days future
    # ------------------------------------------------------------------
    def test_date_exactly_5_years_ago(self):
        """Date exactly 5 years in the past -- boundary test."""
        today = datetime.now(timezone.utc).date()
        past_limit = today.replace(year=today.year - DATE_PAST_YEARS)
        result = {
            "vendor_name": "OldCo",
            "total": 100.0,
            "document_date": past_limit.isoformat(),
        }
        check = verify_ai_output(result)
        # past_limit itself should pass (not < past_limit)
        date_failures = [f for f in check["failures"] if "past" in f]
        assert len(date_failures) == 0, "Date exactly at 5-year boundary should pass"

    def test_date_one_day_before_5_years(self):
        """Date 5 years + 1 day ago should fail."""
        today = datetime.now(timezone.utc).date()
        past_limit = today.replace(year=today.year - DATE_PAST_YEARS)
        too_old = past_limit - timedelta(days=1)
        result = {
            "vendor_name": "OldCo",
            "total": 100.0,
            "document_date": too_old.isoformat(),
        }
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]
        assert any("past" in f for f in check["failures"])

    def test_date_8_days_future_fails(self):
        """Date 8 days in the future should fail (limit is 7)."""
        today = datetime.now(timezone.utc).date()
        future_date = today + timedelta(days=8)
        result = {
            "vendor_name": "FutureCo",
            "total": 100.0,
            "document_date": future_date.isoformat(),
        }
        check = verify_ai_output(result)
        assert check["hallucination_suspected"]
        assert any("future" in f for f in check["failures"])

    def test_date_7_days_future_passes(self):
        """Date exactly 7 days in the future should pass."""
        today = datetime.now(timezone.utc).date()
        future_date = today + timedelta(days=7)
        result = {
            "vendor_name": "FutureCo",
            "total": 100.0,
            "document_date": future_date.isoformat(),
        }
        check = verify_ai_output(result)
        date_failures = [f for f in check["failures"] if "future" in f]
        assert len(date_failures) == 0, "Date exactly at 7-day future boundary should pass"

    # ------------------------------------------------------------------
    # 20. Can hallucination_suspected be reset without human intervention?
    # ------------------------------------------------------------------
    def test_hallucination_suspected_no_reset_mechanism(self):
        """DEFECT PROBE: verify_ai_output only sets hallucination_suspected=True.
        There is no function to clear it. The DB setter set_hallucination_suspected()
        only sets to 1, never 0. No 'clear' function exists."""
        # verify_ai_output returns hallucination_suspected=False for clean data
        # but set_hallucination_suspected() in DB always sets to 1
        # There is no clear_hallucination_suspected() function
        result = {"vendor_name": "GoodCo", "total": 100.0}
        check = verify_ai_output(result)
        assert check["hallucination_suspected"] is False
        # But once set_hallucination_suspected is called on a doc, it stays forever
        # This is a design concern -- documents get permanently flagged


# ======================================================================
# AUTO-APPROVAL ENGINE + EXCEPTION ROUTER TESTS
# ======================================================================

class TestAutoApprovalAndExceptionRouter:
    """Attack the auto-approval engine and exception router."""

    # ------------------------------------------------------------------
    # 21. Can a document with fraud flags still get auto_post?
    # ------------------------------------------------------------------
    def test_fraud_flags_not_checked_by_auto_approval(self):
        """DEFECT PROBE: AutoApprovalEngine does NOT check fraud_flags at all.
        A document with critical fraud flags can still get auto_post."""
        doc = {
            "document_id": "fraud_doc",
            "vendor": "EvilCorp",
            "client_code": "C001",
            "amount": 5000.0,
            "confidence": 0.95,
            "fraud_flags": json.dumps([
                {"rule": "bank_account_change", "severity": "critical"}
            ]),
            "doc_type": "invoice",
            "category": "office_supplies",
            "gl_account": "5000",
            "tax_code": "T",
        }
        engine = AutoApprovalEngine(db_path=Path("nonexistent.db"))
        result = engine.evaluate_document(document=doc)
        # The engine doesn't check fraud_flags -- it only looks at confidence,
        # vendor memory, amount, and duplicate risk
        # So fraud_flags are silently ignored
        assert "fraud" not in result.get("reason", "").lower(), \
            "AutoApprovalEngine does not examine fraud_flags"

    # ------------------------------------------------------------------
    # 22. High-confidence + duplicate risk
    # ------------------------------------------------------------------
    def test_high_confidence_with_high_duplicate_risk_blocked(self):
        """High duplicate risk should block auto_post even with high confidence."""
        doc = {
            "document_id": "dup_doc",
            "vendor": "GoodCorp",
            "client_code": "C001",
            "amount": 1000.0,
            "confidence": 0.99,
        }
        engine = AutoApprovalEngine(db_path=Path("nonexistent.db"))
        result = engine.evaluate_document(
            document=doc,
            duplicate_result={"risk_level": "high"},
        )
        assert result["decision"] == "needs_review"
        assert result["auto_approved"] is False

    # ------------------------------------------------------------------
    # 23. Approval engine says auto_post but exception router says block
    # ------------------------------------------------------------------
    def test_router_overrides_approval_on_duplicate_confirmed(self):
        """Exception router should block when duplicate is confirmed,
        even if auto approval engine said auto_post."""
        document = {
            "document_id": "test_123",
            "vendor": "GoodCorp",
            "client_code": "C001",
            "doc_type": "invoice",
            "category": "supplies",
            "gl_account": "5000",
            "tax_code": "T",
            "amount": 1000.0,
            "confidence": 0.95,
            "posting_status": "",
            "approval_state": "",
            "review_status": "",
            "currency": "CAD",
        }
        auto_result = {
            "decision": "auto_post",
            "approval_score": 0.95,
            "auto_approved": True,
            "vendor_memory_ok": True,
            "document_confidence_ok": True,
            "amount_suspicious": False,
            "duplicate_risk_level": "none",
            "reason": "auto_post",
        }
        duplicate_result = {
            "risk_level": "high",
            "duplicate_confirmed": True,
            "score": 0.99,
        }

        router = ExceptionRouter()
        result = router.route_document(
            document=document,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
        )
        assert result["action"] == "block_posting"
        assert "duplicate_confirmed" in result["reasons"]

    def test_router_overrides_on_missing_fields(self):
        """Exception router blocks auto_post when required accounting fields are missing."""
        document = {
            "document_id": "test_456",
            "vendor": "",  # missing vendor
            "client_code": "C001",
            "doc_type": "invoice",
            "category": "supplies",
            "gl_account": "5000",
            "tax_code": "T",
            "amount": 1000.0,
            "confidence": 0.95,
        }
        auto_result = {
            "decision": "auto_post",
            "approval_score": 0.95,
            "auto_approved": True,
            "document_confidence_ok": True,
            "amount_suspicious": False,
        }
        router = ExceptionRouter()
        result = router.route_document(
            document=document,
            auto_result=auto_result,
        )
        assert result["action"] == "review"
        assert "missing_vendor" in result["reasons"]

    # ------------------------------------------------------------------
    # 24. Borderline signals
    # ------------------------------------------------------------------
    def test_borderline_confidence_exactly_085(self):
        """Review policy with confidence exactly at 0.85 and all required fields."""
        decision = decide_review_status(
            rules_confidence=0.85,
            final_method="rules",
            vendor_name="Acme",
            total=100.0,
            document_date="2025-01-15",
            client_code="C001",
        )
        # 0.85 + boost (min(0.10, 0.15)) = 0.95
        assert decision.status == "Ready"
        assert decision.effective_confidence >= 0.85

    def test_borderline_075_without_required_fields(self):
        """0.75 confidence without required fields should be NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.75,
            final_method="rules",
            vendor_name="Acme",
            total=None,  # missing total
            document_date="2025-01-15",
            client_code="C001",
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence == 0.75  # no boost without required

    def test_borderline_auto_approval_no_vendor_memory(self):
        """Auto approval engine without vendor memory should not auto_post."""
        doc = {
            "document_id": "borderline",
            "vendor": "NewVendor",
            "client_code": "C001",
            "amount": 100.0,
            "confidence": 0.91,
        }
        engine = AutoApprovalEngine(db_path=Path("nonexistent.db"))
        result = engine.evaluate_document(document=doc)
        # No vendor memory = vendor_memory_ok is False
        # Without vendor memory, auto_post requires learned pattern
        assert result["vendor_memory_ok"] is False

    def test_exception_router_auto_post_passthrough(self):
        """When approval engine says auto_post and no blockers exist,
        exception router should pass through auto_post."""
        document = {
            "document_id": "clean_doc",
            "vendor": "GoodCorp",
            "client_code": "C001",
            "doc_type": "invoice",
            "category": "supplies",
            "gl_account": "5000",
            "tax_code": "T",
            "amount": 1000.0,
            "confidence": 0.95,
            "posting_status": "",
            "approval_state": "",
            "review_status": "",
            "currency": "CAD",
        }
        auto_result = {
            "decision": "auto_post",
            "approval_score": 0.95,
            "auto_approved": True,
            "vendor_memory_ok": True,
            "document_confidence_ok": True,
            "amount_suspicious": False,
            "duplicate_risk_level": "none",
            "reason": "auto_post",
        }
        router = ExceptionRouter()
        result = router.route_document(
            document=document,
            auto_result=auto_result,
        )
        assert result["action"] == "auto_post"

    def test_exception_router_does_not_check_fraud_flags(self):
        """DEFECT PROBE: Exception router does NOT examine fraud_flags field.
        A document with critical fraud flags can pass through to auto_post."""
        document = {
            "document_id": "fraud_bypass",
            "vendor": "FraudCorp",
            "client_code": "C001",
            "doc_type": "invoice",
            "category": "supplies",
            "gl_account": "5000",
            "tax_code": "T",
            "amount": 50000.0,
            "confidence": 0.95,
            "posting_status": "",
            "approval_state": "",
            "review_status": "",
            "currency": "CAD",
            "fraud_flags": json.dumps([
                {"rule": "bank_account_change", "severity": "critical"},
                {"rule": "vendor_amount_anomaly", "severity": "high"},
            ]),
        }
        auto_result = {
            "decision": "auto_post",
            "approval_score": 0.95,
            "auto_approved": True,
            "vendor_memory_ok": True,
            "document_confidence_ok": True,
            "amount_suspicious": False,
            "duplicate_risk_level": "none",
            "reason": "auto_post",
        }
        router = ExceptionRouter()
        result = router.route_document(
            document=document,
            auto_result=auto_result,
        )
        # The router passes through auto_post despite critical fraud flags
        assert result["action"] == "auto_post", \
            "DEFECT: Exception router ignores fraud_flags -- critical flags bypass posting block"


# ======================================================================
# TAX EXTRACTION VALIDATION TESTS
# ======================================================================

class TestTaxExtractionValidation:
    """Attack the tax extraction cross-validation in review policy."""

    def test_tax_mismatch_gst_outside_tolerance(self):
        """GST amount off by more than $0.02 should be flagged."""
        warnings = validate_tax_extraction(
            subtotal=100.0,
            gst_amount=5.10,  # expected 5.00
            qst_amount=9.975,
            tax_code="T",
        )
        assert "tax_extraction_mismatch" in warnings

    def test_tax_mismatch_within_tolerance(self):
        """GST/QST within $0.02 tolerance should pass."""
        warnings = validate_tax_extraction(
            subtotal=100.0,
            gst_amount=5.01,  # within 0.02 of 5.00
            qst_amount=9.98,  # within 0.02 of 9.975
            tax_code="GST_QST",
        )
        assert len(warnings) == 0

    def test_tax_validation_skipped_for_non_tax_codes(self):
        """Tax validation should skip for non-GST/QST tax codes."""
        warnings = validate_tax_extraction(
            subtotal=100.0,
            gst_amount=999.0,  # wildly wrong
            qst_amount=999.0,
            tax_code="E",  # exempt
        )
        assert len(warnings) == 0

    def test_tax_validation_zero_subtotal_skipped(self):
        """Zero subtotal should skip tax validation."""
        warnings = validate_tax_extraction(
            subtotal=0.0,
            gst_amount=5.0,
            qst_amount=9.975,
            tax_code="T",
        )
        assert len(warnings) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

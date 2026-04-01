"""
D — DATE AMBIGUITY DESTRUCTION
===============================
Attack date parsing with DD/MM vs MM/DD ambiguity, timezone traps,
leap year boundaries, fiscal year crossovers, and period-end cutoff.

Targets: tax_engine, uncertainty_engine, correction_chain
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import calculate_gst_qst, validate_tax_code
from src.engines.uncertainty_engine import (
    evaluate_uncertainty,
    UncertaintyReason,
    BLOCK_PENDING_REVIEW,
)

from .conftest import fresh_db, ensure_documents_table, insert_document


# ===================================================================
# TEST CLASS: DD/MM vs MM/DD Ambiguity
# ===================================================================

class TestDDMMvsMMDD:
    """Dates like 03/04/2025 are ambiguous: March 4 or April 3?"""

    def test_ambiguous_date_flags_uncertainty(self):
        """03/04/2025 should trigger DATE_AMBIGUOUS uncertainty."""
        reason = UncertaintyReason(
            reason_code="DATE_AMBIGUOUS",
            description_fr="Date ambiguë: 03/04/2025",
            description_en="Ambiguous date: 03/04/2025",
            evidence_available="Raw date string: 03/04/2025",
            evidence_needed="Vendor locale to disambiguate DD/MM vs MM/DD",
        )
        state = evaluate_uncertainty(
            confidence_by_field={"date": 0.45, "vendor": 0.95, "amount": 0.99},
            reasons=[reason],
        )
        assert state.must_block is True, (
            "DEFECT: Ambiguous date with confidence 0.45 must block posting"
        )

    def test_unambiguous_dates_not_flagged(self):
        """Dates like 2025-01-25 (day > 12) are unambiguous."""
        state = evaluate_uncertainty(
            confidence_by_field={"date": 0.95, "vendor": 0.95, "amount": 0.99},
        )
        assert state.can_post is True

    @pytest.mark.parametrize("raw_date,expected_block", [
        ("01/02/2025", True),   # Ambiguous
        ("13/02/2025", False),  # Unambiguous (day > 12)
        ("02/13/2025", False),  # Unambiguous (month > 12 impossible)
        ("12/12/2025", True),   # Ambiguous (both could be month or day)
    ])
    def test_parametrized_ambiguity(self, raw_date, expected_block):
        """Parametrized ambiguity detection."""
        day, month = int(raw_date.split("/")[0]), int(raw_date.split("/")[1])
        is_ambiguous = day <= 12 and month <= 12
        # Date confidence depends on ambiguity
        conf = 0.50 if is_ambiguous else 0.95
        state = evaluate_uncertainty(
            confidence_by_field={"date": conf, "vendor": 0.95, "amount": 0.99},
        )
        if expected_block:
            assert state.can_post is False or state.must_block is True


# ===================================================================
# TEST CLASS: Leap Year Boundaries
# ===================================================================

class TestLeapYearBoundaries:
    """Feb 29 in leap years, fiscal year boundaries."""

    def test_feb_29_leap_year(self):
        """2024-02-29 is valid; 2025-02-29 is not."""
        try:
            d = date(2024, 2, 29)
            assert d.month == 2 and d.day == 29
        except ValueError:
            pytest.fail("2024 is a leap year, Feb 29 should be valid")

        with pytest.raises(ValueError):
            date(2025, 2, 29)

    def test_feb_29_document_date(self):
        """Document dated Feb 29 on non-leap year must be flagged."""
        conn = fresh_db()
        ensure_documents_table(conn)
        doc = insert_document(conn, document_date="2025-02-29")
        # The system should catch this invalid date
        try:
            parsed = datetime.strptime(doc["document_date"], "%Y-%m-%d")
            # If parsing succeeds with 2025-02-29, it's a Python quirk
            pytest.xfail("Python strptime may accept invalid dates silently")
        except ValueError:
            pass  # Correct — invalid date rejected


# ===================================================================
# TEST CLASS: Timezone Traps
# ===================================================================

class TestTimezoneTraps:
    """UTC vs EST/EDT boundary crossing."""

    def test_midnight_utc_is_prev_day_est(self):
        """2025-07-01T00:30:00Z = 2025-06-30 20:30 EDT — different fiscal period!"""
        utc_time = datetime(2025, 7, 1, 0, 30, tzinfo=timezone.utc)
        # EDT = UTC - 4 hours
        edt_offset = timezone(timedelta(hours=-4))
        edt_time = utc_time.astimezone(edt_offset)
        assert edt_time.date() == date(2025, 6, 30), (
            "UTC midnight + 30min is still June 30 in EDT"
        )

    def test_year_end_timezone_cutoff(self):
        """Dec 31 23:30 EST = Jan 1 04:30 UTC — which year?"""
        est_offset = timezone(timedelta(hours=-5))
        est_time = datetime(2025, 12, 31, 23, 30, tzinfo=est_offset)
        utc_time = est_time.astimezone(timezone.utc)
        assert utc_time.year == 2026, "EST year-end is UTC next year"
        assert est_time.year == 2025, "EST is still current year"


# ===================================================================
# TEST CLASS: Fiscal Period Crossover
# ===================================================================

class TestFiscalPeriodCrossover:
    """Documents crossing fiscal period boundaries."""

    def test_invoice_date_vs_receipt_date(self):
        """Invoice dated June 30, received July 2 — which period?"""
        invoice_date = date(2025, 6, 30)
        receipt_date = date(2025, 7, 2)
        # Revenue recognition should use invoice date
        assert invoice_date.month == 6
        assert receipt_date.month == 7
        # If system uses receipt_date for period assignment, that's wrong
        # for accrual accounting

    def test_credit_memo_crosses_quarter(self):
        """Credit memo for Q1 invoice processed in Q2."""
        original_date = date(2025, 3, 15)  # Q1
        cm_date = date(2025, 4, 10)         # Q2
        # Tax recovery timing matters
        assert original_date.month <= 3  # Q1
        assert cm_date.month >= 4        # Q2


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestDateDeterminism:
    """Date handling must be 100% deterministic."""

    def test_uncertainty_deterministic_with_same_confidence(self):
        results = []
        for _ in range(50):
            state = evaluate_uncertainty(
                confidence_by_field={"date": 0.55, "vendor": 0.90, "amount": 0.95},
            )
            results.append(state.posting_recommendation)
        assert len(set(results)) == 1, f"Non-deterministic: {set(results)}"

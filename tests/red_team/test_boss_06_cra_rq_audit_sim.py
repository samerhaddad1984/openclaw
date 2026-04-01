"""
tests/red_team/test_boss_06_cra_rq_audit_sim.py
================================================
BOSS FIGHT 6 — CRA/RQ Audit Simulation.

Separate regulator positions, amendment flags, contradiction timeline,
exposure ranges. Federal (CRA) vs Provincial (Revenu Québec) divergence.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    CENT,
    GST_RATE,
    QST_RATE,
    HST_RATE_ON,
    VALID_TAX_CODES,
    calculate_gst_qst,
    calculate_itc_itr,
    extract_tax_from_total,
    validate_tax_code,
)
from src.engines.amendment_engine import (
    flag_amendment_needed,
    get_open_amendment_flags,
    is_period_filed,
    resolve_amendment_flag,
)
from src.engines.uncertainty_engine import (
    evaluate_uncertainty,
    reason_filed_period_amendment,
    reason_tax_registration_incomplete,
)

_ROUND = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _setup_audit_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS gst_filings (
            filing_id TEXT PRIMARY KEY, client_code TEXT NOT NULL,
            period_label TEXT NOT NULL, filed_at TEXT, notes TEXT
        );
        CREATE TABLE IF NOT EXISTS amendment_flags (
            flag_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT NOT NULL,
            filed_period TEXT NOT NULL,
            trigger_document_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL DEFAULT 'credit_memo',
            reason_en TEXT NOT NULL DEFAULT '',
            reason_fr TEXT NOT NULL DEFAULT '',
            original_filing_id TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            resolved_by TEXT, resolved_at TEXT,
            amendment_filing_id TEXT,
            created_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT '',
            UNIQUE(client_code, filed_period, trigger_document_id)
        );
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, client_code TEXT,
            vendor TEXT, amount REAL, gl_account TEXT,
            tax_code TEXT, document_date TEXT, doc_type TEXT,
            review_status TEXT, confidence REAL, raw_result TEXT,
            file_name TEXT, file_path TEXT, submitted_by TEXT,
            client_note TEXT, fraud_flags TEXT, category TEXT
        );
    """)
    conn.commit()


class TestCRARQAuditSimulation:
    """CRA (federal) vs RQ (provincial) audit scenarios."""

    # ----- ITC/ITR Exposure Calculations -----

    def test_itc_exposure_gst_on_taxable(self):
        """CRA audit: verify ITC claim on taxable purchases."""
        amount = Decimal("10000")
        result = calculate_itc_itr(amount, "T")
        expected_itc = _ROUND(amount * GST_RATE)
        assert Decimal(str(result["gst_recoverable"])) == expected_itc

    def test_itr_exposure_qst_on_taxable(self):
        """RQ audit: verify ITR claim on taxable purchases."""
        amount = Decimal("10000")
        result = calculate_itc_itr(amount, "T")
        expected_itr = _ROUND(amount * QST_RATE)
        assert Decimal(str(result["qst_recoverable"])) == expected_itr

    def test_meals_50pct_recovery_cra_and_rq(self):
        """Both CRA and RQ: meals only 50% recoverable."""
        amount = Decimal("1000")
        result = calculate_itc_itr(amount, "M")
        expected_itc = _ROUND(amount * GST_RATE * Decimal("0.5"))
        expected_itr = _ROUND(amount * QST_RATE * Decimal("0.5"))
        assert Decimal(str(result["gst_recoverable"])) == expected_itc
        assert Decimal(str(result["qst_recoverable"])) == expected_itr

    def test_insurance_no_gst_recovery(self):
        """CRA: no ITC on insurance. RQ: no ITR (9% is not QST)."""
        amount = Decimal("5000")
        result = calculate_itc_itr(amount, "I")
        assert Decimal(str(result["gst_recoverable"])) == Decimal("0")
        assert Decimal(str(result["qst_recoverable"])) == Decimal("0")

    def test_exempt_zero_exposure(self):
        """Both CRA and RQ: exempt purchases = zero recovery."""
        for code in ("E", "Z"):
            result = calculate_itc_itr(Decimal("50000"), code)
            assert Decimal(str(result["gst_recoverable"])) == Decimal("0")
            assert Decimal(str(result["qst_recoverable"])) == Decimal("0")

    # ----- Province / Tax Code Contradiction -----

    def test_qc_vendor_using_hst_flagged(self):
        """Quebec vendor charging HST is a contradiction."""
        result = validate_tax_code("5200 - Supplies", "HST", "QC")
        assert not result["valid"]
        assert any("qc" in w.lower() and "hst" in w.lower() for w in result["warnings"])

    def test_on_vendor_using_gst_qst_flagged(self):
        """Ontario vendor using GST+QST instead of HST is wrong."""
        result = validate_tax_code("5200 - Supplies", "T", "ON")
        assert not result["valid"]
        assert any("hst" in w.lower() for w in result["warnings"])

    def test_all_provinces_have_valid_tax_regime(self):
        """Every province should have at least one valid tax code path."""
        provinces = ["QC", "ON", "NB", "NS", "NL", "PE", "AB", "BC", "SK", "MB", "NT", "NU", "YT"]
        for prov in provinces:
            # Find a valid code for this province
            found_valid = False
            for code in VALID_TAX_CODES:
                result = validate_tax_code("5200 - General", code, prov)
                if result["valid"]:
                    found_valid = True
                    break
            assert found_valid, f"No valid tax code found for province {prov}"

    # ----- Exposure Range Calculations -----

    def test_exposure_range_best_worst_case(self):
        """Calculate min/max tax exposure for a disputed invoice."""
        disputed_amount = Decimal("25000")

        # Best case: fully taxable, full recovery
        best = calculate_itc_itr(disputed_amount, "T")
        best_exposure = Decimal(str(best["gst_recoverable"])) + Decimal(str(best["qst_recoverable"]))

        # Worst case: exempt, no recovery
        worst = calculate_itc_itr(disputed_amount, "E")
        worst_exposure = Decimal(str(worst["gst_recoverable"])) + Decimal(str(worst["qst_recoverable"]))

        assert best_exposure > Decimal("0"), "Taxable should have recovery"
        assert worst_exposure == Decimal("0"), "Exempt should have zero recovery"
        # Exposure range = best - worst
        exposure_range = best_exposure - worst_exposure
        assert exposure_range > Decimal("0")

    def test_hst_vs_gst_qst_exposure_difference(self):
        """HST (13%) vs GST+QST (14.975%) — different federal/provincial split."""
        amount = Decimal("10000")

        hst_result = calculate_itc_itr(amount, "HST")
        gst_qst_result = calculate_itc_itr(amount, "T")

        # HST: ITC = full HST amount, ITR = 0 (no QST in HST provinces)
        hst_total = Decimal(str(hst_result["hst_recoverable"])) + Decimal(str(hst_result["qst_recoverable"]))
        # GST+QST: ITC = GST portion, ITR = QST portion
        gst_qst_total = Decimal(str(gst_qst_result["gst_recoverable"])) + Decimal(str(gst_qst_result["qst_recoverable"]))

        # Both should be meaningful amounts
        assert hst_total > Decimal("0")
        assert gst_qst_total > Decimal("0")

    # ----- Amendment Flags for Audit Period -----

    def test_amendment_flag_for_cra_audit_period(self):
        """CRA audit finding must trigger amendment for affected period."""
        conn = _fresh_db()
        _setup_audit_db(conn)

        conn.execute(
            "INSERT INTO gst_filings VALUES (?,?,?,?,?)",
            ("f1", "AUDIT_CO", "2025-Q4", datetime.now(timezone.utc).isoformat(), None),
        )
        conn.commit()

        result = flag_amendment_needed(
            conn,
            client_code="AUDIT_CO",
            filed_period="2025-Q4",
            trigger_document_id="EXP-042",
            trigger_type="cra_reassessment",
            reason_en="CRA reassessment: ITC disallowed on personal expenses",
        )
        assert result["status"] == "amendment_flag_raised"
        flags = get_open_amendment_flags(conn, "AUDIT_CO")
        assert len(flags) >= 1

    def test_tax_registration_uncertainty_blocks(self):
        """Vendor with unproven tax registration must block posting."""
        reason = reason_tax_registration_incomplete()
        state = evaluate_uncertainty(
            {"vendor": 0.85, "amount": 0.90, "tax_registration": 0.40},
            reasons=[reason],
        )
        assert state.must_block  # tax_registration < 0.60

    # ----- Extract and Verify Round-Trip for Audit Trail -----

    def test_extract_and_recompute_for_all_amounts(self):
        """CRA/RQ auditors verify: extract → recompute must match within $0.02."""
        amounts = [
            Decimal("100.00"), Decimal("1149.75"), Decimal("999.99"),
            Decimal("50000.00"), Decimal("0.01"), Decimal("12345.67"),
        ]
        for total in amounts:
            extracted = extract_tax_from_total(total)
            recomputed = calculate_gst_qst(extracted["pre_tax"])
            diff = abs(recomputed["total_with_tax"] - total)
            assert diff <= Decimal("0.02"), \
                f"Audit trail broken for ${total}: diff=${diff}"

    def test_annual_itc_itr_reconciliation(self):
        """Reconcile annual ITC/ITR claims across multiple invoices."""
        invoices = [
            (Decimal("5000"), "T"),
            (Decimal("2000"), "M"),
            (Decimal("3000"), "T"),
            (Decimal("1000"), "E"),
            (Decimal("8000"), "T"),
        ]
        total_itc = Decimal("0")
        total_itr = Decimal("0")

        for amount, code in invoices:
            r = calculate_itc_itr(amount, code)
            total_itc += Decimal(str(r["gst_recoverable"]))
            total_itr += Decimal(str(r["qst_recoverable"]))

        # Taxable total: 5000 + 3000 + 8000 = 16000 (full), 2000 (50%)
        expected_itc_taxable = _ROUND(Decimal("16000") * GST_RATE)
        expected_itc_meals = _ROUND(Decimal("2000") * GST_RATE * Decimal("0.5"))
        expected_total_itc = expected_itc_taxable + expected_itc_meals

        assert abs(total_itc - expected_total_itc) <= Decimal("0.02")

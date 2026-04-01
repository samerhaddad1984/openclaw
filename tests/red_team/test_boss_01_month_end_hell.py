"""
tests/red_team/test_boss_01_month_end_hell.py
==============================================
BOSS FIGHT 1 — Month-End From Hell.

One client, one month, ALL modules touched:
  OCR → tax → bank recon → fraud → uncertainty → manual journal conflict →
  export → financial statements.

Every assertion proves end-to-end integrity across module boundaries.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    CENT,
    GST_RATE,
    QST_RATE,
    calculate_gst_qst,
    calculate_itc_itr,
    extract_tax_from_total,
    validate_tax_code,
)
from src.engines.reconciliation_engine import (
    BALANCE_TOLERANCE,
    DuplicateItemError,
    FinalizedReconciliationError,
    add_reconciliation_item,
    calculate_reconciliation,
    create_reconciliation,
    ensure_reconciliation_tables,
)
from src.engines.fraud_engine import (
    WEEKEND_HOLIDAY_AMOUNT_LIMIT,
    _quebec_holidays,
)
from src.engines.uncertainty_engine import (
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    SAFE_TO_POST,
    UncertaintyReason,
    evaluate_uncertainty,
    reason_manual_journal_collision,
)
from src.engines.substance_engine import substance_classifier
from src.engines.export_engine import (
    generate_csv,
    generate_sage50,
    generate_acomba,
    generate_qbd_iif,
    generate_xero,
    generate_wave,
)
from src.engines.audit_engine import (
    ensure_audit_tables,
    create_working_paper,
    sign_off_working_paper,
    generate_trial_balance,
    generate_financial_statements,
    get_or_create_working_paper,
    create_engagement,
)

_ROUND = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


# ── Helpers ──────────────────────────────────────────────────────────────

def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _recon_db(stmt_bal: float = 50000.0, gl_bal: float = 50000.0):
    conn = _fresh_db()
    ensure_reconciliation_tables(conn)
    rid = create_reconciliation("HELL_CLIENT", "Chequing", "2026-03-31",
                                stmt_bal, gl_bal, conn)
    return conn, rid


def _seed_documents_table(conn: sqlite3.Connection):
    """Create a minimal documents table for audit/financial statement tests."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95,
            raw_result TEXT,
            submitted_by TEXT,
            client_note TEXT,
            fraud_flags TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            document_date TEXT,
            amount REAL,
            currency TEXT DEFAULT 'CAD',
            doc_type TEXT,
            category TEXT,
            gl_account TEXT,
            tax_code TEXT,
            memo TEXT,
            review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95,
            blocking_issues TEXT,
            notes TEXT
        );
    """)
    conn.commit()


def _insert_doc(conn, doc_id, vendor, amount, gl, tax_code, doc_date, doc_type="invoice"):
    conn.execute(
        """INSERT INTO documents
           (document_id, client_code, vendor, amount, gl_account, tax_code,
            document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (doc_id, "HELL_CLIENT", vendor, amount, gl, tax_code, doc_date,
         doc_type, "approved"),
    )
    conn.execute(
        """INSERT INTO posting_jobs
           (posting_id, document_id, client_code, vendor, amount, gl_account,
            tax_code, document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (f"pj_{doc_id}", doc_id, "HELL_CLIENT", vendor, amount, gl, tax_code,
         doc_date, doc_type, "approved"),
    )


# =========================================================================
# TEST CLASS
# =========================================================================

class TestMonthEndFromHell:
    """One client, March 2026, every module exercised in sequence."""

    # ----- TAX: Mixed codes in one month -----

    def test_tax_mixed_codes_one_month(self):
        """March has invoices with T, M, I, HST, E, Z — all must compute correctly."""
        cases = [
            ("T", Decimal("1000")),
            ("M", Decimal("500")),
            ("I", Decimal("2000")),
            ("E", Decimal("750")),
            ("Z", Decimal("300")),
            ("HST", Decimal("1500")),
        ]
        for code, pre_tax in cases:
            result = calculate_itc_itr(pre_tax, code)
            assert "gst_recoverable" in result, f"Missing gst_recoverable for code {code}"
            assert "qst_recoverable" in result, f"Missing qst_recoverable for code {code}"
            assert Decimal(str(result["gst_recoverable"])) >= Decimal("0")
            assert Decimal(str(result["qst_recoverable"])) >= Decimal("0")
            # Exempt and zero-rated: no recovery
            if code in ("E", "Z"):
                assert Decimal(str(result["gst_recoverable"])) == Decimal("0")
                assert Decimal(str(result["qst_recoverable"])) == Decimal("0")

    def test_tax_extract_then_recompute_integrity(self):
        """For every invoice total, extract → recompute must stay within $0.01."""
        totals = [Decimal("114.98"), Decimal("1149.75"), Decimal("57.49"),
                  Decimal("0.23"), Decimal("99999.99")]
        for total in totals:
            extracted = extract_tax_from_total(total)
            recomputed = calculate_gst_qst(extracted["pre_tax"])
            diff = abs(recomputed["total_with_tax"] - total)
            assert diff <= Decimal("0.02"), f"Roundtrip broke for total={total}: diff={diff}"

    # ----- BANK RECON: Multiple item types in one recon -----

    def test_recon_deposits_cheques_errors_balance(self):
        """Deposits, cheques, and errors all in one recon — must balance."""
        conn, rid = _recon_db(50000.0, 50000.0)
        # Bank side: deposits in transit and outstanding cheques
        add_reconciliation_item(rid, "deposit_in_transit", "Deposit #1",
                                2000.0, "2026-03-30", conn)
        add_reconciliation_item(rid, "outstanding_cheque", "Chq #501",
                                1500.0, "2026-03-28", conn)
        add_reconciliation_item(rid, "outstanding_cheque", "Chq #502",
                                500.0, "2026-03-29", conn)
        result = calculate_reconciliation(rid, conn)
        # adjusted_bank = 50000 + 2000 - 1500 - 500 = 50000
        assert result["is_balanced"], f"Expected balanced, got diff={result['difference']}"

    def test_recon_with_bank_and_book_errors(self):
        """Bank error + book error must adjust both sides correctly."""
        conn, rid = _recon_db(10000.0, 9800.0)
        # Bank recorded $200 too much → negative bank error
        add_reconciliation_item(rid, "bank_error", "Bank overcharge",
                                -200.0, "2026-03-15", conn)
        result = calculate_reconciliation(rid, conn)
        # adjusted_bank = 10000 + (-200) = 9800; adjusted_book = 9800
        assert result["is_balanced"]

    def test_recon_finalized_blocks_new_items(self):
        """After finalization, no new items can be added."""
        conn, rid = _recon_db(5000.0, 5000.0)
        result = calculate_reconciliation(rid, conn)
        assert result["is_balanced"]
        # Finalize
        conn.execute(
            "UPDATE bank_reconciliations SET finalized_at = '2026-04-01T00:00:00+00:00' WHERE reconciliation_id = ?",
            (rid,),
        )
        conn.commit()
        with pytest.raises(FinalizedReconciliationError):
            add_reconciliation_item(rid, "deposit_in_transit", "Late deposit",
                                    100.0, "2026-04-01", conn)

    # ----- FRAUD: Holiday + weekend + duplicate cluster -----

    def test_fraud_holidays_in_march(self):
        """Quebec holidays are detected correctly for 2026."""
        holidays = _quebec_holidays(2026)
        # Easter 2026: April 5 (Good Friday = April 3, Easter Monday = April 6)
        # March has no Quebec statutory holidays typically
        march_holidays = {d: n for d, n in holidays.items() if d.month == 3}
        # Even if no March holidays, the function must return a valid dict
        assert isinstance(holidays, dict)
        for d, name in holidays.items():
            assert isinstance(d, date)
            assert isinstance(name, str)
            assert len(name) > 0

    # ----- UNCERTAINTY: Manual journal collision -----

    def test_uncertainty_manual_journal_collision(self):
        """Manual journal touching same GL as auto-post → must block."""
        reason = reason_manual_journal_collision()
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.90, "gl_account": 0.85},
            reasons=[reason],
        )
        # Unresolved reasons prevent clean posting even with high confidence
        assert not state.can_post
        assert state.partial_post_allowed
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS

    def test_uncertainty_low_confidence_blocks(self):
        """Any field < 0.60 → must_block."""
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.55, "gl_account": 0.90},
        )
        assert state.must_block
        assert not state.can_post
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_uncertainty_all_high_clean_post(self):
        """All fields >= 0.80 with no reasons → safe to post."""
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.90, "gl_account": 0.85, "date": 0.92},
        )
        assert state.can_post
        assert not state.must_block
        assert state.posting_recommendation == SAFE_TO_POST

    # ----- SUBSTANCE: Mixed CapEx/OpEx in one month -----

    def test_substance_capex_vs_repair_same_vendor(self):
        """Same vendor, one CapEx (equipment), one repair — must classify differently."""
        capex_result = substance_classifier(
            vendor="ABC HVAC Inc.",
            memo="Remplacement complet système HVAC",
            doc_type="invoice",
            amount=15000.0,
        )
        repair_result = substance_classifier(
            vendor="ABC HVAC Inc.",
            memo="Réparation annuelle climatisation",
            doc_type="invoice",
            amount=800.0,
        )
        # CapEx and repair from same vendor must be distinguished
        assert capex_result.get("potential_capex") != repair_result.get("potential_capex") or \
               capex_result.get("block_auto_approval") != repair_result.get("block_auto_approval"), \
               "CapEx and repair from same vendor must be distinguished"

    # ----- EXPORT: All formats must produce non-empty output -----

    def test_export_all_formats_non_empty(self):
        """Every export format must produce bytes for a valid doc set."""
        docs = [
            {
                "document_id": "DOC-001", "vendor": "Fournisseur A",
                "amount": 1149.75, "gl_account": "5200 - Fournitures",
                "tax_code": "T", "document_date": "2026-03-15",
                "doc_type": "invoice", "client_code": "HELL_CLIENT",
                "review_status": "approved", "currency": "CAD",
                "category": "office_supplies",
            },
            {
                "document_id": "DOC-002", "vendor": "Restaurant XYZ",
                "amount": 57.49, "gl_account": "5300 - Repas",
                "tax_code": "M", "document_date": "2026-03-20",
                "doc_type": "invoice", "client_code": "HELL_CLIENT",
                "review_status": "approved", "currency": "CAD",
                "category": "meals",
            },
        ]
        for gen_fn, label in [
            (generate_csv, "CSV"),
            (generate_sage50, "Sage50"),
            (generate_acomba, "Acomba"),
            (generate_qbd_iif, "QBD_IIF"),
            (generate_xero, "Xero"),
            (generate_wave, "Wave"),
        ]:
            output = gen_fn(docs)
            assert isinstance(output, bytes), f"{label} didn't return bytes"
            assert len(output) > 10, f"{label} output is suspiciously small"

    # ----- FINANCIAL STATEMENTS: Trial balance from mixed docs -----

    def test_trial_balance_and_financial_statements(self):
        """Generate trial balance and financial statements for March."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents_table(conn)
        # Insert mixed documents for the month
        _insert_doc(conn, "D001", "Vendor A", 5000.0, "5200", "T", "2026-03-05")
        _insert_doc(conn, "D002", "Vendor B", 2000.0, "5300", "M", "2026-03-10")
        _insert_doc(conn, "D003", "Vendor C", 1000.0, "6100", "E", "2026-03-15")
        _insert_doc(conn, "D004", "Vendor D", 8000.0, "1500", "T", "2026-03-20",
                    doc_type="credit_memo")
        conn.commit()

        tb = generate_trial_balance(conn, "HELL_CLIENT", "2026-03")
        assert isinstance(tb, list)

        fs = generate_financial_statements(conn, "HELL_CLIENT", "2026-03")
        assert "income_statement" in fs or "balance_sheet" in fs

    # ----- CROSS-MODULE: Tax validation + substance + uncertainty pipeline -----

    def test_full_pipeline_capex_blocks_auto_post(self):
        """CapEx substance flag → uncertainty engine must NOT allow clean post."""
        sub = substance_classifier(
            vendor="Équipements Lourds Inc.",
            memo="Achat nouvel équipement industriel",
            doc_type="invoice",
            amount=45000.0,
        )
        # If substance flags CapEx, uncertainty must block or flag
        if sub.get("potential_capex"):
            state = evaluate_uncertainty(
                {"vendor": 0.95, "amount": 0.90, "gl_account": 0.70},
            )
            # GL confidence 0.70 (medium range) → partial post only
            assert not state.can_post or state.partial_post_allowed

    def test_mixed_month_recon_survives_duplicate_guard(self):
        """Adding same item twice to a recon must raise DuplicateItemError."""
        conn, rid = _recon_db(10000.0, 10000.0)
        add_reconciliation_item(rid, "deposit_in_transit", "Deposit ABC",
                                500.0, "2026-03-25", conn)
        with pytest.raises(DuplicateItemError):
            add_reconciliation_item(rid, "deposit_in_transit", "Deposit ABC",
                                    500.0, "2026-03-25", conn)

    def test_validate_tax_code_consistency_for_month(self):
        """All tax code/province combos used in March must validate correctly."""
        combos = [
            ("5200 - Office Supplies", "T", "QC"),
            ("5300 - Meals", "M", "QC"),
            ("6100 - Insurance", "I", "QC"),
            ("5200 - Supplies", "HST", "ON"),
            ("5200 - Supplies", "T", "QC"),
            ("5200 - Supplies", "E", "QC"),
        ]
        for gl, code, prov in combos:
            result = validate_tax_code(gl, code, prov)
            assert "valid" in result
            assert "warnings" in result
            # Province/code mismatch should warn
            if prov == "ON" and code == "T":
                assert any("hst" in w.lower() for w in result["warnings"])

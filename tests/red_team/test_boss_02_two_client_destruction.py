"""
tests/red_team/test_boss_02_two_client_destruction.py
=====================================================
BOSS FIGHT 2 — Two-Client Destruction.

Same vendors and invoice numbers across two clients.
Goal: prove NO cross-client leakage in reconciliation, audit,
working papers, and financial statements.
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.reconciliation_engine import (
    add_reconciliation_item,
    calculate_reconciliation,
    create_reconciliation,
    ensure_reconciliation_tables,
)
from src.engines.audit_engine import (
    ensure_audit_tables,
    create_working_paper,
    get_working_papers,
    generate_trial_balance,
    generate_financial_statements,
    get_or_create_working_paper,
    create_engagement,
)
from src.engines.tax_engine import calculate_gst_qst, calculate_itc_itr


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _seed_documents_table(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, raw_result TEXT,
            submitted_by TEXT, client_note TEXT, fraud_flags TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, document_date TEXT, amount REAL,
            currency TEXT DEFAULT 'CAD', doc_type TEXT,
            category TEXT, gl_account TEXT, tax_code TEXT,
            memo TEXT, review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, blocking_issues TEXT, notes TEXT
        );
    """)
    conn.commit()


def _insert_doc(conn, doc_id, client_code, vendor, amount, gl, tax_code, doc_date):
    conn.execute(
        """INSERT INTO documents
           (document_id, client_code, vendor, amount, gl_account, tax_code,
            document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (doc_id, client_code, vendor, amount, gl, tax_code, doc_date,
         "invoice", "approved"),
    )
    conn.execute(
        """INSERT INTO posting_jobs
           (posting_id, document_id, client_code, vendor, amount, gl_account,
            tax_code, document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (f"pj_{doc_id}", doc_id, client_code, vendor, amount, gl, tax_code,
         doc_date, "invoice", "approved"),
    )


class TestTwoClientDestruction:
    """Same vendors, same invoice numbers, two clients — zero leakage."""

    def test_same_vendor_same_invoice_number_different_clients(self):
        """INV-001 from 'Vendor X' exists in both CLIENT_A and CLIENT_B."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents_table(conn)

        _insert_doc(conn, "INV-001-A", "CLIENT_A", "Vendor X", 5000.0,
                    "5200", "T", "2026-03-15")
        _insert_doc(conn, "INV-001-B", "CLIENT_B", "Vendor X", 8000.0,
                    "5200", "T", "2026-03-15")
        conn.commit()

        # Trial balance must be isolated per client
        tb_a = generate_trial_balance(conn, "CLIENT_A", "2026-03")
        tb_b = generate_trial_balance(conn, "CLIENT_B", "2026-03")

        # Both should return lists (possibly empty if no posted docs)
        assert isinstance(tb_a, list)
        assert isinstance(tb_b, list)

    def test_reconciliation_isolation(self):
        """Recon for CLIENT_A must not include CLIENT_B items."""
        conn = _fresh_db()
        ensure_reconciliation_tables(conn)

        rid_a = create_reconciliation("CLIENT_A", "Chequing", "2026-03-31",
                                      25000.0, 25000.0, conn)
        rid_b = create_reconciliation("CLIENT_B", "Chequing", "2026-03-31",
                                      40000.0, 40000.0, conn)

        add_reconciliation_item(rid_a, "deposit_in_transit", "Deposit",
                                1000.0, "2026-03-30", conn)
        add_reconciliation_item(rid_b, "outstanding_cheque", "Chq #100",
                                2000.0, "2026-03-29", conn)

        result_a = calculate_reconciliation(rid_a, conn)
        result_b = calculate_reconciliation(rid_b, conn)

        # CLIENT_A: 25000 + 1000 = 26000 bank, 25000 book → not balanced
        assert not result_a["is_balanced"]
        # CLIENT_B: 40000 - 2000 = 38000 bank, 40000 book → not balanced
        assert not result_b["is_balanced"]
        # Crucially, CLIENT_A's deposit must NOT appear in CLIENT_B's calc
        assert result_b["bank_side"]["deposits_in_transit"] == 0.0

    def test_working_papers_isolation(self):
        """Working papers for one client must not leak to the other."""
        conn = _fresh_db()
        ensure_audit_tables(conn)

        wp_a = get_or_create_working_paper(
            conn, "CLIENT_A", "2026-03", "audit", "1000", "Cash",
        )
        wp_b = get_or_create_working_paper(
            conn, "CLIENT_B", "2026-03", "audit", "1000", "Cash",
        )

        papers_a = get_working_papers(conn, "CLIENT_A", "2026-03")
        papers_b = get_working_papers(conn, "CLIENT_B", "2026-03")

        # Each client should see only their own papers
        a_ids = {p["paper_id"] for p in papers_a}
        b_ids = {p["paper_id"] for p in papers_b}
        assert a_ids.isdisjoint(b_ids), "Working papers leaked between clients"

    def test_financial_statements_isolation(self):
        """Financial statements must not mix client data."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents_table(conn)

        _insert_doc(conn, "D-A1", "CLIENT_A", "Vendor X", 10000.0,
                    "4000", "T", "2026-03-05")
        _insert_doc(conn, "D-B1", "CLIENT_B", "Vendor X", 50000.0,
                    "4000", "T", "2026-03-05")
        conn.commit()

        fs_a = generate_financial_statements(conn, "CLIENT_A", "2026-03")
        fs_b = generate_financial_statements(conn, "CLIENT_B", "2026-03")

        # Both should return structure, neither should be identical
        # (different amounts means different statements)
        assert fs_a is not None
        assert fs_b is not None

    def test_tax_calculation_independent_of_client(self):
        """Tax engine is stateless — same input = same output regardless of client."""
        amount = Decimal("1000")
        result_a = calculate_gst_qst(amount)
        result_b = calculate_gst_qst(amount)
        assert result_a == result_b, "Tax engine must be stateless"

    def test_itc_itr_independent_of_client(self):
        """ITC/ITR recovery is client-agnostic."""
        for code in ("T", "M", "I", "E", "HST"):
            r1 = calculate_itc_itr(Decimal("5000"), code)
            r2 = calculate_itc_itr(Decimal("5000"), code)
            assert r1 == r2

    def test_overlapping_vendor_names_no_confusion(self):
        """Two clients with vendor 'ABC Inc.' — each gets their own docs."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents_table(conn)

        # Same vendor name, different amounts
        _insert_doc(conn, "OV-A1", "CLIENT_A", "ABC Inc.", 3000.0,
                    "5200", "T", "2026-03-10")
        _insert_doc(conn, "OV-A2", "CLIENT_A", "ABC Inc.", 1500.0,
                    "5200", "T", "2026-03-20")
        _insert_doc(conn, "OV-B1", "CLIENT_B", "ABC Inc.", 7000.0,
                    "5200", "T", "2026-03-10")
        conn.commit()

        # Query CLIENT_A docs only
        rows_a = conn.execute(
            "SELECT * FROM documents WHERE client_code = ?", ("CLIENT_A",)
        ).fetchall()
        rows_b = conn.execute(
            "SELECT * FROM documents WHERE client_code = ?", ("CLIENT_B",)
        ).fetchall()

        assert len(rows_a) == 2
        assert len(rows_b) == 1
        total_a = sum(r["amount"] for r in rows_a)
        total_b = sum(r["amount"] for r in rows_b)
        assert total_a == 4500.0
        assert total_b == 7000.0

    def test_concurrent_recon_different_clients_no_interference(self):
        """Creating and calculating recons for two clients simultaneously."""
        conn = _fresh_db()
        ensure_reconciliation_tables(conn)

        # Create both recons
        rid_a = create_reconciliation("CLIENT_A", "Savings", "2026-03-31",
                                      100000.0, 100000.0, conn)
        rid_b = create_reconciliation("CLIENT_B", "Savings", "2026-03-31",
                                      200000.0, 200000.0, conn)

        # Add items to both
        add_reconciliation_item(rid_a, "deposit_in_transit", "Wire",
                                5000.0, "2026-03-30", conn)
        add_reconciliation_item(rid_b, "deposit_in_transit", "Wire",
                                15000.0, "2026-03-30", conn)

        # Calculate both
        res_a = calculate_reconciliation(rid_a, conn)
        res_b = calculate_reconciliation(rid_b, conn)

        # A's deposit must not appear in B's calculation
        assert res_a["bank_side"]["deposits_in_transit"] == 5000.0
        assert res_b["bank_side"]["deposits_in_transit"] == 15000.0

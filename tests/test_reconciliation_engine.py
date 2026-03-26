"""
tests/test_reconciliation_engine.py
====================================
Tests for the bank reconciliation engine and dashboard routes.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.engines.reconciliation_engine import (
    BALANCE_TOLERANCE,
    add_reconciliation_item,
    auto_populate_outstanding_items,
    calculate_reconciliation,
    create_reconciliation,
    ensure_reconciliation_tables,
    finalize_reconciliation,
    generate_reconciliation_pdf,
    get_reconciliation,
    get_reconciliation_items,
    get_reconciliation_summary,
    list_reconciliations,
    mark_item_cleared,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """In-memory SQLite DB with all required tables."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    # Create minimal documents and bank tables needed by tests
    db.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            review_status TEXT,
            confidence REAL,
            raw_result TEXT,
            created_at TEXT,
            updated_at TEXT,
            ingest_source TEXT
        );

        CREATE TABLE IF NOT EXISTS bank_statements (
            statement_id TEXT PRIMARY KEY,
            bank_name TEXT,
            file_name TEXT,
            client_code TEXT,
            imported_by TEXT,
            imported_at TEXT,
            period_start TEXT,
            period_end TEXT,
            transaction_count INTEGER DEFAULT 0,
            matched_count INTEGER DEFAULT 0,
            unmatched_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS bank_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            txn_date TEXT,
            description TEXT,
            debit REAL,
            credit REAL,
            balance REAL,
            matched_document_id TEXT,
            match_confidence REAL,
            match_reason TEXT
        );
    """)
    ensure_reconciliation_tables(db)
    db.commit()
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def test_ensure_tables_creates_both(conn):
    tables = [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    ]
    assert "bank_reconciliations" in tables
    assert "reconciliation_items" in tables


def test_ensure_tables_idempotent(conn):
    # Calling twice should not raise
    ensure_reconciliation_tables(conn)
    ensure_reconciliation_tables(conn)


# ---------------------------------------------------------------------------
# create_reconciliation
# ---------------------------------------------------------------------------

def test_create_reconciliation_returns_id(conn):
    rid = create_reconciliation(
        "ACME", "Chequing", "2026-02-28", 10000.0, 9800.0, conn,
        account_number="12345", prepared_by="sam",
    )
    assert rid.startswith("recon_")

    row = conn.execute(
        "SELECT * FROM bank_reconciliations WHERE reconciliation_id = ?", (rid,)
    ).fetchone()
    assert row is not None
    assert row["client_code"] == "ACME"
    assert row["account_name"] == "Chequing"
    assert float(row["statement_ending_balance"]) == 10000.0
    assert float(row["gl_ending_balance"]) == 9800.0
    assert row["status"] == "open"
    assert row["prepared_by"] == "sam"


# ---------------------------------------------------------------------------
# add_reconciliation_item
# ---------------------------------------------------------------------------

def test_add_item_and_retrieve(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    iid = add_reconciliation_item(
        rid, "deposit_in_transit", "Customer payment", 500.0, "2026-02-27", conn,
    )
    assert iid.startswith("ri_")

    items = get_reconciliation_items(rid, conn)
    assert len(items) == 1
    assert items[0]["item_type"] == "deposit_in_transit"
    assert items[0]["amount"] == 500.0
    assert items[0]["status"] == "outstanding"


# ---------------------------------------------------------------------------
# calculate_reconciliation
# ---------------------------------------------------------------------------

def test_calculate_balanced(conn):
    """When statement + deposits - cheques == GL, difference should be 0."""
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10500.0, conn)
    add_reconciliation_item(rid, "deposit_in_transit", "Deposit A", 1000.0, "2026-02-27", conn)
    add_reconciliation_item(rid, "outstanding_cheque", "Cheque 101", 500.0, "2026-02-25", conn)

    result = calculate_reconciliation(rid, conn)
    # Bank: 10000 + 1000 - 500 = 10500
    # Book: 10500
    assert result["bank_side"]["adjusted_bank_balance"] == 10500.0
    assert result["book_side"]["adjusted_book_balance"] == 10500.0
    assert result["difference"] == 0.0
    assert result["is_balanced"] is True


def test_calculate_unbalanced(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 9000.0, conn)
    result = calculate_reconciliation(rid, conn)
    assert result["difference"] == 1000.0
    assert result["is_balanced"] is False


def test_calculate_with_bank_charges_and_interest(conn):
    """Book side: GL - bank_charges + interest_earned."""
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10050.0, conn)
    add_reconciliation_item(rid, "bank_charge", "Monthly fee", 25.0, "2026-02-28", conn)
    add_reconciliation_item(rid, "interest_earned", "Interest", 75.0, "2026-02-28", conn)

    result = calculate_reconciliation(rid, conn)
    # Book: 10050 - 25 + 75 = 10100
    # Bank: 10000
    assert result["book_side"]["adjusted_book_balance"] == 10100.0
    assert result["bank_side"]["adjusted_bank_balance"] == 10000.0


def test_calculate_tolerance(conn):
    """$0.01 tolerance should be considered balanced."""
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.01, 10000.0, conn)
    result = calculate_reconciliation(rid, conn)
    assert result["is_balanced"] is True


def test_calculate_bank_errors(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10100.0, conn)
    add_reconciliation_item(rid, "bank_error", "Bank deposited wrong amount", 100.0, "2026-02-26", conn)
    result = calculate_reconciliation(rid, conn)
    # Bank: 10000 + 100 = 10100
    # Book: 10100
    assert result["bank_side"]["adjusted_bank_balance"] == 10100.0
    assert result["is_balanced"] is True


def test_calculate_book_errors(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 9950.0, conn)
    add_reconciliation_item(rid, "book_error", "Recording error", 50.0, "2026-02-26", conn)
    result = calculate_reconciliation(rid, conn)
    # Book: 9950 + 50 = 10000
    assert result["book_side"]["adjusted_book_balance"] == 10000.0
    assert result["is_balanced"] is True


# ---------------------------------------------------------------------------
# mark_item_cleared
# ---------------------------------------------------------------------------

def test_mark_item_cleared(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    iid = add_reconciliation_item(rid, "outstanding_cheque", "Cheque 101", 500.0, "2026-02-25", conn)

    assert mark_item_cleared(iid, "2026-03-01", conn) is True

    items = get_reconciliation_items(rid, conn)
    cleared_item = [i for i in items if i["item_id"] == iid][0]
    assert cleared_item["status"] == "cleared"
    assert cleared_item["cleared_date"] == "2026-03-01"


def test_mark_item_cleared_nonexistent(conn):
    assert mark_item_cleared("nonexistent", "2026-03-01", conn) is False


def test_cleared_items_excluded_from_calculation(conn):
    """Cleared items should not affect the reconciliation balance."""
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    iid = add_reconciliation_item(rid, "outstanding_cheque", "Cheque 101", 500.0, "2026-02-25", conn)

    # Before clearing: bank = 10000 - 500 = 9500
    result1 = calculate_reconciliation(rid, conn)
    assert result1["bank_side"]["adjusted_bank_balance"] == 9500.0

    mark_item_cleared(iid, "2026-03-01", conn)

    # After clearing: bank = 10000
    result2 = calculate_reconciliation(rid, conn)
    assert result2["bank_side"]["adjusted_bank_balance"] == 10000.0
    assert result2["is_balanced"] is True


# ---------------------------------------------------------------------------
# finalize_reconciliation
# ---------------------------------------------------------------------------

def test_finalize_balanced(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    assert finalize_reconciliation(rid, "manager1", conn) is True

    recon = get_reconciliation(rid, conn)
    assert recon["status"] == "balanced"
    assert recon["reviewed_by"] == "manager1"
    assert recon["reviewed_at"] is not None


def test_finalize_unbalanced_rejected(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 9000.0, conn)
    assert finalize_reconciliation(rid, "manager1", conn) is False

    recon = get_reconciliation(rid, conn)
    assert recon["status"] == "open"  # unchanged
    assert recon["reviewed_by"] is None


# ---------------------------------------------------------------------------
# auto_populate_outstanding_items
# ---------------------------------------------------------------------------

def test_auto_populate(conn):
    # Setup: create a bank statement with unmatched transactions
    conn.execute(
        "INSERT INTO bank_statements (statement_id, bank_name, client_code, period_start, period_end) VALUES (?, ?, ?, ?, ?)",
        ("stmt_001", "Desjardins", "ACME", "2026-02-01", "2026-02-28"),
    )
    conn.execute(
        "INSERT INTO bank_transactions (statement_id, document_id, txn_date, description, debit, credit, matched_document_id, match_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("stmt_001", "doc_001", "2026-02-15", "Supplier payment", 1500.0, None, None, "no_matching_invoice"),
    )
    conn.execute(
        "INSERT INTO bank_transactions (statement_id, document_id, txn_date, description, debit, credit, matched_document_id, match_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("stmt_001", "doc_002", "2026-02-20", "Client deposit", None, 3000.0, None, "no_matching_invoice"),
    )
    conn.execute(
        "INSERT INTO bank_transactions (statement_id, document_id, txn_date, description, debit, credit, matched_document_id, match_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("stmt_001", "doc_003", "2026-02-28", "Frais bancaires", 15.0, None, None, "no_matching_invoice"),
    )
    conn.commit()

    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    added = auto_populate_outstanding_items(rid, conn)
    assert added == 3

    items = get_reconciliation_items(rid, conn)
    types = {i["item_type"] for i in items}
    assert "outstanding_cheque" in types
    assert "deposit_in_transit" in types
    assert "bank_charge" in types


def test_auto_populate_bank_charge_detection(conn):
    """Bank charges should be detected by French/English keywords."""
    conn.execute(
        "INSERT INTO bank_statements (statement_id, bank_name, client_code, period_start, period_end) VALUES (?, ?, ?, ?, ?)",
        ("stmt_002", "BMO", "ACME", "2026-03-01", "2026-03-31"),
    )
    for doc_id, desc in [
        ("doc_100", "Service charge"),
        ("doc_101", "Frais de service mensuel"),
        ("doc_102", "Bank fee"),
    ]:
        conn.execute(
            "INSERT INTO bank_transactions (statement_id, document_id, txn_date, description, debit, credit, matched_document_id, match_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("stmt_002", doc_id, "2026-03-15", desc, 10.0, None, None, "no_matching_invoice"),
        )
    conn.commit()

    rid = create_reconciliation("ACME", "Chequing", "2026-03-31", 10000.0, 10000.0, conn)
    added = auto_populate_outstanding_items(rid, conn)
    assert added == 3
    items = get_reconciliation_items(rid, conn)
    assert all(i["item_type"] == "bank_charge" for i in items)


# ---------------------------------------------------------------------------
# list_reconciliations
# ---------------------------------------------------------------------------

def test_list_reconciliations(conn):
    create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    create_reconciliation("BETA", "Savings", "2026-02-28", 5000.0, 5000.0, conn)
    create_reconciliation("ACME", "Chequing", "2026-03-31", 12000.0, 12000.0, conn)

    all_recons = list_reconciliations(conn)
    assert len(all_recons) == 3

    acme_only = list_reconciliations(conn, client_code="ACME")
    assert len(acme_only) == 2

    feb_only = list_reconciliations(conn, period="2026-02")
    assert len(feb_only) == 2


# ---------------------------------------------------------------------------
# get_reconciliation_summary
# ---------------------------------------------------------------------------

def test_get_summary(conn):
    # Insert a document so there's a known client
    conn.execute(
        "INSERT INTO documents (document_id, client_code, doc_type) VALUES (?, ?, ?)",
        ("doc_x", "GAMMA", "invoice"),
    )
    conn.commit()

    create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    summary = get_reconciliation_summary(conn)
    assert "open_clients" in summary
    assert "balanced_clients" in summary
    assert "at_risk_clients" in summary
    assert "avg_days_to_complete" in summary


# ---------------------------------------------------------------------------
# generate_reconciliation_pdf
# ---------------------------------------------------------------------------

def test_generate_pdf_nonexistent(conn):
    pdf = generate_reconciliation_pdf("nonexistent", "en", conn)
    assert pdf == b""


def test_generate_pdf_returns_bytes(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    add_reconciliation_item(rid, "deposit_in_transit", "Deposit A", 500.0, "2026-02-27", conn)
    pdf = generate_reconciliation_pdf(rid, "en", conn)
    assert isinstance(pdf, bytes)
    assert len(pdf) > 0


def test_generate_pdf_french(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    pdf = generate_reconciliation_pdf(rid, "fr", conn)
    assert isinstance(pdf, bytes)
    assert len(pdf) > 0


# ---------------------------------------------------------------------------
# get_reconciliation / get_reconciliation_items
# ---------------------------------------------------------------------------

def test_get_reconciliation_not_found(conn):
    assert get_reconciliation("nonexistent", conn) is None


def test_get_reconciliation_items_empty(conn):
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 10000.0, conn)
    items = get_reconciliation_items(rid, conn)
    assert items == []


# ---------------------------------------------------------------------------
# Integration: full reconciliation workflow
# ---------------------------------------------------------------------------

def test_full_workflow(conn):
    """Create → add items → calculate → finalize → PDF."""
    rid = create_reconciliation(
        "ACME", "Chequing", "2026-02-28", 15000.0, 14500.0, conn,
        prepared_by="sam",
    )

    # Bank side: 15000 + 500 (DIT) - 1000 (OC) = 14500
    add_reconciliation_item(rid, "deposit_in_transit", "Customer A", 500.0, "2026-02-27", conn)
    add_reconciliation_item(rid, "outstanding_cheque", "Cheque 200", 1000.0, "2026-02-20", conn)

    result = calculate_reconciliation(rid, conn)
    assert result["bank_side"]["adjusted_bank_balance"] == 14500.0
    assert result["book_side"]["adjusted_book_balance"] == 14500.0
    assert result["is_balanced"] is True

    # Finalize
    assert finalize_reconciliation(rid, "manager1", conn) is True
    recon = get_reconciliation(rid, conn)
    assert recon["status"] == "balanced"
    assert recon["reviewed_by"] == "manager1"

    # PDF
    pdf = generate_reconciliation_pdf(rid, "en", conn)
    assert len(pdf) > 0


def test_complex_workflow_with_all_item_types(conn):
    """Test with every item type present."""
    rid = create_reconciliation("ACME", "Chequing", "2026-02-28", 10000.0, 9600.0, conn)

    add_reconciliation_item(rid, "deposit_in_transit", "DIT 1", 200.0, "2026-02-27", conn)
    add_reconciliation_item(rid, "outstanding_cheque", "OC 1", 300.0, "2026-02-25", conn)
    add_reconciliation_item(rid, "bank_error", "Bank err +50", 50.0, "2026-02-26", conn)
    add_reconciliation_item(rid, "bank_charge", "Monthly fee", 25.0, "2026-02-28", conn)
    add_reconciliation_item(rid, "interest_earned", "Interest", 75.0, "2026-02-28", conn)
    add_reconciliation_item(rid, "book_error", "Book err +", 0.0, "2026-02-28", conn)

    result = calculate_reconciliation(rid, conn)
    # Bank: 10000 + 200 - 300 + 50 = 9950
    assert result["bank_side"]["adjusted_bank_balance"] == 9950.0
    # Book: 9600 - 25 + 75 + 0 = 9650
    assert result["book_side"]["adjusted_book_balance"] == 9650.0
    assert result["difference"] == 300.0
    assert result["is_balanced"] is False

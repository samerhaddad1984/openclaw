"""
Tests for CRA T2, Quebec chart of accounts (200), and CO-17 mappings seeding.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.engines.audit_engine import (
    ensure_audit_tables,
    get_chart_of_accounts,
    get_co17_mappings,
    seed_chart_of_accounts,
    seed_chart_of_accounts_quebec,
    seed_co17_mappings,
    generate_financial_statements,
    _QUEBEC_CHART_200,
)
from scripts.seed_vendor_knowledge import (
    CRA_T2_CATEGORIES,
    seed_cra_categories,
    seed_chart_and_co17,
)


@pytest.fixture
def mem_db():
    """In-memory SQLite database with row_factory set."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_audit_tables(conn)
    yield conn
    conn.close()


@pytest.fixture
def tmp_db(tmp_path):
    """Temporary on-disk database for seed functions that open their own connection."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    ensure_audit_tables(conn)
    conn.close()
    return db_path


# ── Test 1: CRA T2 categories seed into learning_memory_patterns ────────────

def test_seed_cra_categories(tmp_db):
    count = seed_cra_categories(db_path=tmp_db)
    assert count == len(CRA_T2_CATEGORIES)
    # Re-run should be idempotent
    count2 = seed_cra_categories(db_path=tmp_db)
    assert count2 == 0


# ── Test 2: CRA categories have correct tax codes ───────────────────────────

def test_cra_category_tax_codes(tmp_db):
    seed_cra_categories(db_path=tmp_db)
    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT vendor, tax_code FROM learning_memory_patterns "
        "WHERE category = 'cra_t2_schedule1'"
    ).fetchall()
    conn.close()
    tax_map = {r["vendor"]: r["tax_code"] for r in rows}
    assert tax_map["CRA-T2-8690"] == "I"   # Insurance -> I
    assert tax_map["CRA-T2-8590"] == "E"   # Bad debts -> E
    assert tax_map["CRA-T2-9200"] == "M"   # Meals -> M
    assert tax_map["CRA-T2-8520"] == "T"   # Advertising -> T


# ── Test 3: Quebec chart has at least 190 accounts ──────────────────────────

def test_chart_of_accounts_quebec_count(mem_db):
    count = seed_chart_of_accounts_quebec(mem_db)
    assert count >= 190
    rows = get_chart_of_accounts(mem_db)
    assert len(rows) >= 190


# ── Test 4: Chart includes all account types ────────────────────────────────

def test_chart_account_types(mem_db):
    seed_chart_of_accounts_quebec(mem_db)
    rows = get_chart_of_accounts(mem_db)
    types_found = {r["account_type"] for r in rows}
    assert types_found == {"asset", "liability", "equity", "revenue", "expense"}


# ── Test 5: Expense accounts have CRA T2 lines ─────────────────────────────

def test_expense_accounts_have_cra_lines(mem_db):
    seed_chart_of_accounts_quebec(mem_db)
    expense_rows = mem_db.execute(
        "SELECT * FROM chart_of_accounts WHERE account_type = 'expense'"
    ).fetchall()
    # Most expense accounts should have a cra_t2_line (a few like tax expense may not)
    with_cra = [r for r in expense_rows if r["cra_t2_line"]]
    assert len(with_cra) >= 50


# ── Test 6: CO-17 mappings seed correctly ───────────────────────────────────

def test_co17_mappings_seed(mem_db):
    count = seed_co17_mappings(mem_db)
    assert count == 15
    mappings = get_co17_mappings(mem_db)
    assert len(mappings) == 15
    lines = {m["co17_line"] for m in mappings}
    assert "20" in lines   # Salaires
    assert "42" in lines   # Loyer
    assert "99" in lines   # Autres


# ── Test 7: CO-17 gl_account_codes are valid JSON arrays ────────────────────

def test_co17_gl_codes_format(mem_db):
    seed_co17_mappings(mem_db)
    mappings = get_co17_mappings(mem_db)
    for m in mappings:
        codes = json.loads(m["gl_account_codes"])
        assert isinstance(codes, list)
        assert len(codes) > 0
        for code in codes:
            assert code.startswith("5")  # All expense codes start with 5


# ── Test 8: CO-17 GL codes reference existing chart accounts ────────────────

def test_co17_codes_exist_in_chart(mem_db):
    seed_chart_of_accounts_quebec(mem_db)
    seed_co17_mappings(mem_db)
    mappings = get_co17_mappings(mem_db)
    all_codes = set()
    for m in mappings:
        all_codes.update(json.loads(m["gl_account_codes"]))
    chart_codes = {r["account_code"] for r in get_chart_of_accounts(mem_db)}
    missing = all_codes - chart_codes
    assert not missing, f"CO-17 references non-existent accounts: {missing}"


# ── Test 9: financial_statement_section populated for all accounts ──────────

def test_financial_statement_section(mem_db):
    seed_chart_of_accounts_quebec(mem_db)
    rows = get_chart_of_accounts(mem_db)
    for r in rows:
        assert r["financial_statement_section"], (
            f"Account {r['account_code']} missing financial_statement_section"
        )


# ── Test 10: generate_financial_statements includes new fields ──────────────

def test_financial_statements_include_cra_co17(mem_db):
    seed_chart_of_accounts_quebec(mem_db)
    # Create a documents table and a posted document for testing
    mem_db.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            document_date TEXT,
            vendor TEXT,
            amount REAL,
            gl_account TEXT,
            review_status TEXT DEFAULT 'approved'
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            document_id TEXT,
            posting_status TEXT DEFAULT 'posted',
            created_at TEXT,
            updated_at TEXT
        );
        INSERT INTO documents VALUES
            ('doc1', 'TEST', '2024-01', 'Acme', 1000.0, '5430 Publicite', 'approved');
        INSERT INTO posting_jobs VALUES
            ('doc1', 'posted', '2024-01-15', '2024-01-15');
    """)
    mem_db.commit()
    stmts = generate_financial_statements(mem_db, "TEST", "2024-01")
    expenses = stmts["income_statement"]["expenses"]
    assert len(expenses) >= 1
    expense = expenses[0]
    assert "financial_statement_section" in expense
    assert "cra_t2_line" in expense
    assert "co17_line" in expense
    # Account 5430 maps to CRA line 8520 and CO-17 line 48
    assert expense["cra_t2_line"] == "8520"
    assert expense["co17_line"] == "48"

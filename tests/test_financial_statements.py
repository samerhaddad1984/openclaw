"""Sprint C Batch 2 — financial statement correctness.

Covers:
- BUG #3: balance sheet renderer expected flat keys that engine wasn't
          providing. Engine now populates both nested (for PDF) and flat
          (for HTML renderer) keys.
- BUG #5: trial balance is now validated to balance; output carries a
          `balanced` flag, and financial statements output also carries
          `trial_balance_balanced` plus a balance-sheet `balance_ok` flag
          (accounting identity: assets == liabilities + equity).
"""
from __future__ import annotations

import sqlite3

import pytest

from src.engines import audit_engine as ae


def _setup_minimal_client(conn, client_code="ACME"):
    """Seed chart_of_accounts + a handful of documents/postings so the
    TB and financial statements have something real to build on.
    """
    ae.ensure_audit_tables(conn)
    ae.seed_chart_of_accounts(conn)
    # Minimum schema we need to exercise the engine.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, client_code TEXT,
            document_date TEXT, gl_account TEXT,
            amount REAL, review_status TEXT,
            vendor TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT, posting_status TEXT,
            created_at TEXT, updated_at TEXT
        );
    """)
    # Three posted docs: one asset-increase, one revenue, one expense.
    docs = [
        ("d1", "1100", 500.0),   # Receivable (asset, debit normal)
        ("d2", "4000", 500.0),   # Revenue (credit normal)
        ("d3", "5400", 100.0),   # Office expense (debit normal)
    ]
    for did, gl, amt in docs:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, document_date, gl_account, "
            "amount, review_status, vendor) VALUES (?,?,?,?,?,?,?)",
            (did, client_code, "2026-04-15", gl, amt, "Ready", "Test"),
        )
        conn.execute(
            "INSERT INTO posting_jobs (posting_id, document_id, posting_status, "
            "created_at, updated_at) VALUES (?,?,?,datetime('now'),datetime('now'))",
            (f"pj_{did}", did, "posted"),
        )
    conn.commit()


def test_trial_balance_with_totals_has_balanced_flag(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    tb = ae.trial_balance_with_totals(conn, "ACME", "2026-04")
    assert "debit_total" in tb
    assert "credit_total" in tb
    assert "balanced" in tb
    assert tb["debit_total"] == pytest.approx(600.0)    # 500 + 100
    assert tb["credit_total"] == pytest.approx(500.0)
    # Without a double-entry GL, the TB is NOT expected to balance from
    # document-only postings — the flag correctly surfaces that.
    assert tb["balanced"] is False


def test_trial_balance_balanced_flag_is_true_when_sides_match(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    # Manually force a balanced set by adding offsetting rows.
    conn.execute(
        "INSERT INTO documents (document_id, client_code, document_date, gl_account, "
        "amount, review_status, vendor) VALUES ('d4','ACME','2026-04-15','2100',100.0,'Ready','Test')"
    )
    conn.execute(
        "INSERT INTO posting_jobs (posting_id, document_id, posting_status, created_at, updated_at) "
        "VALUES ('pj_d4','d4','posted',datetime('now'),datetime('now'))"
    )
    conn.commit()
    tb = ae.trial_balance_with_totals(conn, "ACME", "2026-04")
    assert tb["debit_total"] == pytest.approx(tb["credit_total"])
    assert tb["balanced"] is True


def test_balance_sheet_renders_with_flat_keys(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    stmts = ae.generate_financial_statements(conn, "ACME", "2026-04")
    bs = stmts["balance_sheet"]
    # Flat keys the HTML renderer iterates exist and are dicts.
    assert isinstance(bs.get("current_assets"), dict)
    assert isinstance(bs.get("non_current_assets"), dict)
    assert isinstance(bs.get("current_liabilities"), dict)
    assert isinstance(bs.get("long_term_liabilities"), dict)
    assert isinstance(bs.get("equity"), dict)
    # Scalar totals present.
    for k in ("total_current_assets", "total_non_current_assets", "total_assets",
               "total_current_liabilities", "total_long_term_liabilities",
               "total_liabilities", "total_equity", "balance_ok",
               "balance_difference"):
        assert k in bs, f"{k} missing from balance_sheet"


def test_balance_sheet_has_the_1100_receivable_as_current_asset(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    stmts = ae.generate_financial_statements(conn, "ACME", "2026-04")
    ca = stmts["balance_sheet"]["current_assets"]
    # Keyed "code — name" (em dash). At least one key starts with 1100.
    assert any(k.startswith("1100") for k in ca.keys()), ca


def test_balance_sheet_balance_ok_flag(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    stmts = ae.generate_financial_statements(conn, "ACME", "2026-04")
    bs = stmts["balance_sheet"]
    # From document-only postings the accounting identity doesn't hold.
    # The engine must still report the numbers; balance_ok is the flag
    # the renderer uses to show a warning banner.
    assert isinstance(bs["balance_ok"], bool)
    diff = bs["balance_difference"]
    # The engine keeps totals as Decimals for precision; ensure the flag
    # value is numeric-coercible for downstream renderers.
    assert float(diff) == float(diff)


def test_income_statement_flat_keys_present(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    stmts = ae.generate_financial_statements(conn, "ACME", "2026-04")
    inc = stmts["income_statement"]
    assert isinstance(inc["revenue"], dict)
    assert isinstance(inc["expenses"], dict)
    assert "total_revenue" in inc
    assert "total_expenses" in inc
    assert "net_income" in inc


def test_financial_statements_carry_trial_balance_flag(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    _setup_minimal_client(conn)
    stmts = ae.generate_financial_statements(conn, "ACME", "2026-04")
    assert "trial_balance_balanced" in stmts
    assert "trial_balance_debit_total" in stmts
    assert "trial_balance_credit_total" in stmts

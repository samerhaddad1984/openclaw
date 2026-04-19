"""Statement of Changes in Equity tests (Sprint F Fix 5)."""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines import audit_engine  # noqa: E402


def _conn(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c


def _seed_company_with_equity(conn: sqlite3.Connection, client: str = "ACME") -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            amount REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            review_status TEXT,
            subtotal REAL,
            tax_total REAL,
            vendor TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            posting_status TEXT,
            external_id TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )
    audit_engine.ensure_audit_tables(conn)
    audit_engine.seed_chart_of_accounts(conn)
    audit_engine.seed_chart_of_accounts_quebec(conn)
    # Seed prior-year equity (opening balance of retained earnings = 50,000)
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, document_date, gl_account, review_status) "
        "VALUES ('OPN1', ?, 50000, '2024-06-30', '3200', 'approved')",
        (client,),
    )
    # Current period share capital increase of 10,000
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, document_date, gl_account, review_status) "
        "VALUES ('SH1', ?, 10000, '2025-03-15', '3100', 'approved')",
        (client,),
    )
    # Current period dividends paid 5,000 (debit-normal account 3300)
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, document_date, gl_account, review_status) "
        "VALUES ('DIV1', ?, 5000, '2025-06-15', '3300', 'approved')",
        (client,),
    )
    # Some current-period revenue + expense to produce a net income.
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, document_date, gl_account, review_status) "
        "VALUES ('R1', ?, 30000, '2025-02-01', '4100', 'approved')",
        (client,),
    )
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, document_date, gl_account, review_status) "
        "VALUES ('E1', ?, 12000, '2025-02-02', '5000', 'approved')",
        (client,),
    )
    # Mark revenue + expense docs as posted so generate_trial_balance picks them up.
    for did in ("R1", "E1"):
        conn.execute(
            "INSERT INTO posting_jobs (posting_id, document_id, posting_status, external_id, created_at) VALUES (?, ?, 'posted', ?, datetime('now'))",
            (f"PJ-{did}", did, f"EXT-{did}"),
        )
    conn.commit()


def test_soce_opening_balance_from_gl(tmp_path):
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    soce = audit_engine.generate_soce(conn, "ACME", "2025")
    conn.close()
    # Retained earnings opening = 50,000 (seeded before 2025-01-01).
    # Share cap seeded in-period, so opening share capital is 0.
    assert soce["total_opening_equity"] == Decimal("50000.00")


def test_soce_closing_equals_opening_plus_movements(tmp_path):
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    soce = audit_engine.generate_soce(conn, "ACME", "2025")
    conn.close()
    # closing = 50k RE + 10k shares - 5k dividends = 55k.
    assert soce["total_closing_equity"] == Decimal("55000.00")
    assert soce["total_change_in_equity"] == Decimal("5000.00")


def test_soce_includes_net_income(tmp_path):
    # SOCE must carry the net-income figure from the income statement.
    # (We don't recompute NI here; we just verify the bundle field is
    # populated with the same value as the IS produces.)
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025")
    soce = audit_engine.generate_soce(conn, "ACME", "2025")
    conn.close()
    expected = stmts["income_statement"]["net_income"]
    assert soce["net_income"] == expected
    assert soce["net_income"] != Decimal("0")


def test_soce_handles_dividends(tmp_path):
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    soce = audit_engine.generate_soce(conn, "ACME", "2025")
    conn.close()
    assert soce["dividends_paid"] == Decimal("5000.00")


def test_soce_share_issuance_recorded(tmp_path):
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    soce = audit_engine.generate_soce(conn, "ACME", "2025")
    conn.close()
    assert soce["share_issuance"] == Decimal("10000.00")


def test_soce_appears_in_fs_bundle(tmp_path):
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025")
    conn.close()
    assert "statement_of_changes_in_equity" in stmts
    soce = stmts["statement_of_changes_in_equity"]
    assert "total_opening_equity" in soce
    assert "total_closing_equity" in soce


def test_soce_pdf_export(tmp_path):
    db = tmp_path / "s.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    pdf = audit_engine.generate_soce_pdf(conn, "ACME", "2025")
    conn.close()
    assert pdf[:4] == b"%PDF"
    assert len(pdf) > 500


def test_soce_period_boundaries_month(tmp_path):
    # YYYY-MM period string must resolve correctly.
    db = tmp_path / "sm.db"
    conn = _conn(db)
    _seed_company_with_equity(conn)
    soce = audit_engine.generate_soce(conn, "ACME", "2025-06")
    conn.close()
    assert soce["period_start"] == "2025-06-01"
    assert soce["period_end"] == "2025-06-30"

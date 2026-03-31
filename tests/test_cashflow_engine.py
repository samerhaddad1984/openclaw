"""Tests for src/engines/cashflow_engine.py — Cash Flow Statement (Indirect Method)."""
from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest

from src.engines.cashflow_engine import (
    generate_cash_flow_statement,
    get_depreciation,
    get_financing_activities,
    get_investing_activities,
    get_net_income,
    get_working_capital_changes,
    validate_closing_cash,
)


@pytest.fixture()
def conn():
    """In-memory DB with documents and fixed_assets tables."""
    c = sqlite3.connect(":memory:")
    c.row_factory = lambda cur, row: {col[0]: row[i] for i, col in enumerate(cur.description)}
    c.execute("""
        CREATE TABLE documents (
            document_id TEXT, client_code TEXT, document_date TEXT,
            amount REAL, gl_account TEXT, tax_code TEXT,
            review_status TEXT, vendor TEXT
        )
    """)
    c.execute("""
        CREATE TABLE fixed_assets (
            asset_id TEXT PRIMARY KEY, client_code TEXT, asset_name TEXT,
            cca_class INTEGER, acquisition_date TEXT, cost REAL,
            opening_ucc REAL, current_ucc REAL, accumulated_cca REAL,
            status TEXT, disposal_date TEXT, disposal_proceeds REAL, created_at TEXT
        )
    """)
    c.commit()
    return c


def _insert_doc(conn, client, date, amount, gl):
    conn.execute(
        "INSERT INTO documents (document_id, client_code, document_date, amount, gl_account) VALUES (?,?,?,?,?)",
        (f"D-{date}-{gl}", client, date, amount, gl),
    )
    conn.commit()


class TestNetIncome:
    def test_revenue_minus_expenses(self, conn):
        _insert_doc(conn, "ACME", "2026-03-15", 10000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-03-20", 3000.0, "5200 - Rent")
        result = get_net_income("ACME", "2026-01-01", "2026-12-31", conn)
        assert result == Decimal("7000.00")

    def test_zero_when_no_data(self, conn):
        result = get_net_income("EMPTY", "2026-01-01", "2026-12-31", conn)
        assert result == Decimal("0.00")


class TestWorkingCapitalChanges:
    def test_ar_increase_negative_cash(self, conn):
        # AR at start = 0, AR at end = 5000 -> negative 5000
        _insert_doc(conn, "ACME", "2026-06-15", 5000.0, "1100 - AR")
        result = get_working_capital_changes("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["accounts_receivable_change"] == -5000.0

    def test_ap_increase_positive_cash(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 3000.0, "2000 - AP")
        result = get_working_capital_changes("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["accounts_payable_change"] == 3000.0

    def test_working_capital_net(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 5000.0, "1100 - AR")
        _insert_doc(conn, "ACME", "2026-06-15", 3000.0, "2000 - AP")
        result = get_working_capital_changes("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["net_working_capital_change"] == -2000.0


class TestCashReconciliation:
    def test_reconciled_when_match(self, conn):
        # Cash balance at period end = 10000
        _insert_doc(conn, "ACME", "2026-06-15", 10000.0, "1000 - Cash")
        result = validate_closing_cash("ACME", "2026-12-31", Decimal("10000.00"), conn)
        assert result["reconciled"] is True
        assert abs(result["difference"]) < 0.02

    def test_gap_flagged(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 10000.0, "1000 - Cash")
        result = validate_closing_cash("ACME", "2026-12-31", Decimal("9500.00"), conn)
        assert result["reconciled"] is False
        assert result["flag"] == "cash_flow_reconciliation_gap"
        assert result["difference"] == -500.0


class TestInvestingActivities:
    def test_asset_purchase(self, conn):
        conn.execute(
            """INSERT INTO fixed_assets
               (asset_id, client_code, asset_name, cca_class, acquisition_date,
                cost, opening_ucc, current_ucc, accumulated_cca, status)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            ("FA-001", "ACME", "Truck", 10, "2026-05-01", 50000.0, 50000.0, 42500.0, 7500.0, "active"),
        )
        conn.commit()
        result = get_investing_activities("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["purchase_of_capital_assets"] == 50000.0

    def test_asset_disposal_proceeds(self, conn):
        conn.execute(
            """INSERT INTO fixed_assets
               (asset_id, client_code, asset_name, cca_class, acquisition_date,
                cost, opening_ucc, current_ucc, accumulated_cca, status,
                disposal_date, disposal_proceeds)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("FA-002", "ACME", "Old Van", 10, "2024-01-01", 30000.0, 30000.0, 0.0, 30000.0,
             "disposed", "2026-06-15", 5000.0),
        )
        conn.commit()
        result = get_investing_activities("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["proceeds_from_disposal"] == 5000.0


class TestFinancingActivities:
    def test_debt_proceeds(self, conn):
        _insert_doc(conn, "ACME", "2026-04-01", 100000.0, "2500 - LT Debt")
        result = get_financing_activities("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["proceeds_from_long_term_debt"] == 100000.0

    def test_dividends(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 5000.0, "3200 - Dividends")
        result = get_financing_activities("ACME", "2026-01-01", "2026-12-31", conn)
        assert result["payment_of_dividends"] == 5000.0


class TestFullCashFlowStatement:
    def test_complete_statement_structure(self, conn):
        # Revenue
        _insert_doc(conn, "ACME", "2026-03-15", 50000.0, "4100 - Revenue")
        # Expenses
        _insert_doc(conn, "ACME", "2026-03-20", 20000.0, "5200 - Rent")
        # Cash at start
        _insert_doc(conn, "ACME", "2025-12-31", 15000.0, "1000 - Cash")

        result = generate_cash_flow_statement("ACME", "2026-01-01", "2026-12-31", conn)

        assert result["client_code"] == "ACME"
        assert "operating_activities" in result
        assert "investing_activities" in result
        assert "financing_activities" in result
        assert "net_change_in_cash" in result
        assert "opening_cash_balance" in result
        assert "closing_cash_balance" in result
        assert "bank_reconciliation" in result
        assert "labels" in result
        assert result["labels"]["fr"]["operating"] == "Activités d'exploitation"
        assert result["labels"]["en"]["operating"] == "Operating Activities"
        assert result["operating_activities"]["net_income"] == 30000.0

    def test_empty_client(self, conn):
        result = generate_cash_flow_statement("NONE", "2026-01-01", "2026-12-31", conn)
        assert result["net_change_in_cash"] == 0.0
        assert result["closing_cash_balance"] == 0.0

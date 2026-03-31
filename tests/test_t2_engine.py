"""Tests for src/engines/t2_engine.py — T2 Corporate Tax Pre-fill."""
from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest

from src.engines.t2_engine import (
    generate_co17_mapping,
    generate_schedule_1,
    generate_schedule_100,
    generate_schedule_125,
    generate_schedule_50,
    generate_t2_prefill,
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
    c.execute("""
        CREATE TABLE related_parties (
            id INTEGER PRIMARY KEY, client_code TEXT, party_name TEXT,
            relationship_type TEXT, ownership_pct REAL,
            dividends_paid REAL, salary_paid REAL, loans_amount REAL
        )
    """)
    c.commit()
    return c


def _insert_doc(conn, client, date, amount, gl, tax_code="T"):
    conn.execute(
        "INSERT INTO documents (document_id, client_code, document_date, amount, gl_account, tax_code) VALUES (?,?,?,?,?,?)",
        (f"D-{date}-{gl}", client, date, amount, gl, tax_code),
    )
    conn.commit()


class TestSchedule1AddBacks:
    def test_net_income_line(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 100000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-06-20", 40000.0, "5200 - Rent")
        result = generate_schedule_1("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["001"]["amount"] == 60000.0

    def test_meals_addback_50pct(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 100000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-06-20", 2000.0, "5400 - Meals", tax_code="M")
        result = generate_schedule_1("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["101"]["amount"] == 1000.0  # 50% of 2000

    def test_taxable_income_calculation(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 100000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-06-20", 40000.0, "5200 - Rent")
        result = generate_schedule_1("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        # Net income 60000 + addbacks should result in line 300
        assert "300" in lines_by_num
        assert lines_by_num["300"]["description"] == "Net income for tax purposes"


class TestSchedule100BalanceSheet:
    def test_asset_lines(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 50000.0, "1000 - Cash")
        _insert_doc(conn, "ACME", "2026-06-15", 25000.0, "1100 - AR")
        result = generate_schedule_100("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["101"]["amount"] == 50000.0  # Cash
        assert lines_by_num["105"]["amount"] == 25000.0  # AR

    def test_liability_lines(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 15000.0, "2000 - AP")
        _insert_doc(conn, "ACME", "2026-06-15", 5000.0, "2200 - GST Payable")
        result = generate_schedule_100("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["301"]["amount"] == 15000.0  # AP
        assert lines_by_num["310"]["amount"] == 5000.0   # GST/QST

    def test_total_assets(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 50000.0, "1000 - Cash")
        _insert_doc(conn, "ACME", "2026-06-15", 25000.0, "1100 - AR")
        result = generate_schedule_100("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["199"]["amount"] == 75000.0

    def test_capital_assets_from_fixed_assets(self, conn):
        conn.execute(
            """INSERT INTO fixed_assets
               (asset_id, client_code, asset_name, cca_class, acquisition_date,
                cost, opening_ucc, current_ucc, accumulated_cca, status)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            ("FA-001", "ACME", "Truck", 10, "2026-01-15", 50000.0, 50000.0, 42500.0, 7500.0, "active"),
        )
        conn.commit()
        result = generate_schedule_100("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["171"]["amount"] == 42500.0


class TestSchedule125IncomeStatement:
    def test_revenue_and_expenses(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 200000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-06-20", 80000.0, "5100 - COGS")
        _insert_doc(conn, "ACME", "2026-06-25", 50000.0, "5600 - OpEx")
        result = generate_schedule_125("ACME", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["8000"]["amount"] == 200000.0
        assert lines_by_num["8300"]["amount"] == 80000.0
        assert lines_by_num["GP"]["amount"] == 120000.0
        assert lines_by_num["NI"]["amount"] == 70000.0

    def test_empty_client(self, conn):
        result = generate_schedule_125("NOBODY", "2026-12-31", conn)
        lines_by_num = {l["line"]: l for l in result["lines"]}
        assert lines_by_num["NI"]["amount"] == 0.0


class TestCO17Mapping:
    def test_co17_lines_mapped(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 50000.0, "1000 - Cash")
        _insert_doc(conn, "ACME", "2026-06-15", 100000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-06-20", 40000.0, "5200 - Rent")
        result = generate_co17_mapping("ACME", "2026-12-31", conn)
        assert len(result["lines"]) > 0
        # Check that CO-17 line numbers are present
        co17_line_nums = {l["co17_line"] for l in result["lines"]}
        assert "10" in co17_line_nums  # Cash -> CO-17 line 10

    def test_co17_title_bilingual(self, conn):
        result = generate_co17_mapping("ACME", "2026-12-31", conn)
        assert "CO-17" in result["title"]
        assert "Québec" in result["title_fr"]


class TestSchedule50:
    def test_shareholders_from_related_parties(self, conn):
        conn.execute(
            """INSERT INTO related_parties
               (client_code, party_name, relationship_type, ownership_pct,
                dividends_paid, salary_paid, loans_amount)
               VALUES (?,?,?,?,?,?,?)""",
            ("ACME", "Jane Doe", "shareholder", 75.0, 10000.0, 50000.0, 0.0),
        )
        conn.commit()
        result = generate_schedule_50("ACME", "2026-12-31", conn)
        assert len(result["shareholders"]) == 1
        assert result["shareholders"][0]["name"] == "Jane Doe"
        assert result["shareholders"][0]["ownership_pct"] == 75.0
        assert result["shareholders"][0]["dividends_paid"] == 10000.0

    def test_empty_shareholders(self, conn):
        result = generate_schedule_50("EMPTY", "2026-12-31", conn)
        assert result["shareholders"] == []


class TestT2Prefill:
    def test_full_prefill_structure(self, conn):
        _insert_doc(conn, "ACME", "2026-06-15", 100000.0, "4100 - Revenue")
        _insert_doc(conn, "ACME", "2026-06-20", 40000.0, "5200 - Rent")
        result = generate_t2_prefill("ACME", "2026-12-31", conn)
        assert result["client_code"] == "ACME"
        assert "schedule_1" in result
        assert "schedule_8" in result
        assert "schedule_50" in result
        assert "schedule_100" in result
        assert "schedule_125" in result
        assert "co17" in result
        assert "disclaimer" in result
        assert "fr" in result["disclaimer"]
        assert "en" in result["disclaimer"]
        assert "pré-remplis" in result["disclaimer"]["fr"]
        assert "pre-filled" in result["disclaimer"]["en"]

    def test_empty_prefill(self, conn):
        result = generate_t2_prefill("NOBODY", "2026-12-31", conn)
        assert result["schedule_1"]["schedule"] == "1"
        assert result["schedule_100"]["schedule"] == "100"

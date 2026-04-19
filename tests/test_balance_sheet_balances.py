"""BS identity A = L + E must hold on every financial statement run.

Companion to test_ni_sign_convention.py — verifies the balance sheet
closes even when the underlying equity accounts are empty (fresh ledger)
or only partially posted.
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines import audit_engine  # noqa: E402


def _mk_db(tmp_path):
    db = tmp_path / "bs.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, client_code TEXT, vendor TEXT,
            amount REAL, document_date TEXT, gl_account TEXT, tax_code TEXT,
            review_status TEXT, subtotal REAL, tax_total REAL,
            gst_amount REAL, qst_amount REAL, raw_result TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            posting_status TEXT, external_id TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE ar_invoices (
            invoice_id TEXT PRIMARY KEY, client_code TEXT,
            customer_name TEXT, invoice_number TEXT,
            invoice_date TEXT, due_date TEXT,
            amount_ht REAL, gst_amount REAL, qst_amount REAL,
            total_amount REAL, status TEXT, description TEXT
        );
    """)
    audit_engine.ensure_audit_tables(conn)
    audit_engine.seed_chart_of_accounts(conn)
    audit_engine.seed_chart_of_accounts_quebec(conn)
    return db, conn


def _seed(conn, client, period_date, revenue, expenses):
    if revenue > 0:
        gst = round(revenue * 0.05, 2)
        qst = round(revenue * 0.09975, 2)
        conn.execute(
            "INSERT INTO ar_invoices (invoice_id, client_code, invoice_date, "
            "amount_ht, gst_amount, qst_amount, total_amount, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'sent')",
            (f"AR-{client}-{period_date}", client, period_date, revenue, gst, qst,
             revenue + gst + qst),
        )
    if expenses > 0:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, vendor, amount, "
            "document_date, gl_account, tax_code, review_status) "
            "VALUES (?, ?, 'Supplier', ?, ?, '5000', 'T', 'approved')",
            (f"AP-{client}-{period_date}", client, expenses, period_date),
        )
        conn.execute(
            "INSERT INTO posting_jobs (posting_id, document_id, posting_status, "
            "external_id, created_at) VALUES (?, ?, 'posted', 'EXT', datetime('now'))",
            (f"P-{client}-{period_date}", f"AP-{client}-{period_date}"),
        )
    conn.commit()


def _bs_diff(fs) -> Decimal:
    bs = fs["balance_sheet"]
    return Decimal(str(bs["total_assets"])) - (
        Decimal(str(bs["total_liabilities"])) + Decimal(str(bs["total_equity"]))
    )


# ---------------------------------------------------------------------------
# Core identity tests
# ---------------------------------------------------------------------------

def test_bs_balances_for_profitable_client(tmp_path):
    """Revenue 100k, expense 60k → NI +40k. BS must close."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "PROFIT", "2025-07-15", revenue=100_000, expenses=60_000)
    fs = audit_engine.generate_financial_statements(conn, "PROFIT", "2025-07")
    assert fs["income_statement"]["net_income"] == Decimal("40000.00")
    assert fs["balance_sheet"]["balance_ok"] is True, \
        f"diff={fs['balance_sheet']['balance_difference']}"
    assert abs(_bs_diff(fs)) <= Decimal("0.01")
    conn.close()


def test_bs_balances_for_loss_client(tmp_path):
    """Revenue 50k, expense 80k → NI -30k. Identity still holds."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "LOSS", "2025-07-15", revenue=50_000, expenses=80_000)
    fs = audit_engine.generate_financial_statements(conn, "LOSS", "2025-07")
    assert fs["income_statement"]["net_income"] == Decimal("-30000.00")
    assert fs["balance_sheet"]["balance_ok"] is True, \
        f"diff={fs['balance_sheet']['balance_difference']}"
    conn.close()


def test_bs_balances_for_breakeven_client(tmp_path):
    """Revenue 75k, expense 75k → NI 0. Identity must still close."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "FLAT", "2025-07-15", revenue=75_000, expenses=75_000)
    fs = audit_engine.generate_financial_statements(conn, "FLAT", "2025-07")
    assert fs["income_statement"]["net_income"] == Decimal("0.00")
    assert fs["balance_sheet"]["balance_ok"] is True
    conn.close()


def test_bs_balances_for_revenue_only(tmp_path):
    """Revenue 50k, no expenses. NI = +50k. BS must close."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "REV-ONLY", "2025-07-15", revenue=50_000, expenses=0)
    fs = audit_engine.generate_financial_statements(conn, "REV-ONLY", "2025-07")
    assert fs["balance_sheet"]["balance_ok"] is True, \
        f"diff={fs['balance_sheet']['balance_difference']}"
    conn.close()


def test_bs_balances_for_expense_only(tmp_path):
    """Expense 30k, no revenue. NI = -30k. BS must close."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "EXP-ONLY", "2025-07-15", revenue=0, expenses=30_000)
    fs = audit_engine.generate_financial_statements(conn, "EXP-ONLY", "2025-07")
    assert fs["balance_sheet"]["balance_ok"] is True
    conn.close()


# ---------------------------------------------------------------------------
# Cross-statement consistency
# ---------------------------------------------------------------------------

def test_bs_total_equity_matches_soce_closing(tmp_path):
    """BS total_equity must equal SOCE total_closing_equity (same number,
    computed once, used twice)."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "MATCH", "2025-07-15", revenue=200_000, expenses=120_000)
    fs = audit_engine.generate_financial_statements(conn, "MATCH", "2025-07")
    bs_eq = Decimal(str(fs["balance_sheet"]["total_equity"]))
    soce_close = Decimal(str(fs["statement_of_changes_in_equity"]["total_closing_equity"]))
    assert bs_eq == soce_close
    conn.close()


def test_bs_equity_exposes_current_period_ni_line_item(tmp_path):
    """The equity section must include a 'Current Period Net Income' line
    for CPA presentation."""
    _, conn = _mk_db(tmp_path)
    _seed(conn, "VIEW", "2025-07-15", revenue=100_000, expenses=60_000)
    fs = audit_engine.generate_financial_statements(conn, "VIEW", "2025-07")
    items = fs["balance_sheet"]["equity_detail"]["items"]
    ni_lines = [x for x in items if "Net Income" in (x.get("account_name") or "")]
    assert ni_lines, f"no NI equity line; items={[x.get('account_name') for x in items]}"
    assert Decimal(str(ni_lines[0]["amount"])) == Decimal("40000.00")
    assert ni_lines[0].get("_synthetic") is True
    conn.close()


# ---------------------------------------------------------------------------
# Opening equity
# ---------------------------------------------------------------------------

def test_bs_balances_with_manual_opening_equity_seed(tmp_path):
    """Manual opening-equity seed + current activity → BS still closes."""
    _, conn = _mk_db(tmp_path)
    # Seed $25k opening retained earnings via the public helper.
    audit_engine.set_opening_equity_balance(
        conn, client_code="SEEDED", period="2025-07",
        account_code="3500", amount=Decimal("25000.00"),
    )
    _seed(conn, "SEEDED", "2025-07-15", revenue=90_000, expenses=30_000)
    fs = audit_engine.generate_financial_statements(conn, "SEEDED", "2025-07")
    assert fs["income_statement"]["net_income"] == Decimal("60000.00")
    # closing equity should equal opening + NI = 25k + 60k = 85k
    soce_close = Decimal(str(fs["statement_of_changes_in_equity"]["total_closing_equity"]))
    assert soce_close == Decimal("85000.00"), soce_close
    conn.close()


# ---------------------------------------------------------------------------
# Simulation regression: the 3 CPA clients must balance.
# ---------------------------------------------------------------------------

def test_bs_balances_on_three_simulation_clients():
    sim_db = Path("/opt/otocpa/tests/simulation/sim.db")
    if not sim_db.exists():
        pytest.skip("sim.db not present")
    conn = sqlite3.connect(str(sim_db))
    conn.row_factory = sqlite3.Row
    periods = [r["period"] for r in conn.execute(
        "SELECT DISTINCT period FROM trial_balance ORDER BY period LIMIT 1"
    ).fetchall()]
    if not periods:
        pytest.skip("no trial_balance rows")
    period = periods[0]
    bad = []
    for cc in ("ACME-CAFE", "ACME-CONST", "ACME-SOLM"):
        fs = audit_engine.generate_financial_statements(conn, cc, period)
        if not fs["balance_sheet"]["balance_ok"]:
            bad.append((cc, fs["balance_sheet"]["balance_difference"]))
    assert not bad, f"unbalanced clients: {bad}"
    conn.close()

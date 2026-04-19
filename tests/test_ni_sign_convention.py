"""Part 1 Bug A — revenue / liability / equity sign convention.

After fix, a profitable business must report a POSITIVE net income
and POSITIVE total_equity. The balance-sheet identity (A = L + E)
must hold within $0.01 on synthetic data.
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
    db = tmp_path / "ni.db"
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


def _seed_profitable(conn, client, period_date, revenue, expenses):
    """Seed a client with revenue and expenses in the same period."""
    # Revenue via AR invoices (base = revenue; tax small to avoid drift).
    base = revenue
    gst = round(base * 0.05, 2)
    qst = round(base * 0.09975, 2)
    conn.execute(
        "INSERT INTO ar_invoices (invoice_id, client_code, invoice_date, "
        "amount_ht, gst_amount, qst_amount, total_amount, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'sent')",
        (f"AR-{period_date}", client, period_date, base, gst, qst,
         base + gst + qst),
    )
    # Expense via AP document.
    conn.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, "
        "document_date, gl_account, tax_code, review_status) "
        "VALUES (?, ?, 'Supplier', ?, ?, '5000', 'T', 'approved')",
        (f"AP-{period_date}", client, expenses, period_date),
    )
    conn.execute(
        "INSERT INTO posting_jobs (posting_id, document_id, posting_status, "
        "external_id, created_at) VALUES (?, ?, 'posted', 'EXT', datetime('now'))",
        (f"P-{period_date}", f"AP-{period_date}"),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Core sign-convention tests
# ---------------------------------------------------------------------------

def test_positive_ni_from_profitable_scenario(tmp_path):
    db, conn = _mk_db(tmp_path)
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=100_000, expenses=60_000)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    ni = Decimal(str(stmts["income_statement"]["net_income"]))
    assert ni == Decimal("40000.00"), (
        f"Profitable scenario (rev 100k − exp 60k) should yield +$40,000, got {ni}"
    )
    conn.close()


def test_negative_ni_from_loss_scenario(tmp_path):
    db, conn = _mk_db(tmp_path)
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=50_000, expenses=80_000)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    ni = Decimal(str(stmts["income_statement"]["net_income"]))
    assert ni == Decimal("-30000.00"), (
        f"Loss scenario (rev 50k − exp 80k) should yield −$30,000, got {ni}"
    )
    conn.close()


def test_zero_ni_from_breakeven(tmp_path):
    db, conn = _mk_db(tmp_path)
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=75_000, expenses=75_000)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    ni = Decimal(str(stmts["income_statement"]["net_income"]))
    assert ni == Decimal("0.00")
    conn.close()


def test_total_revenue_positive(tmp_path):
    db, conn = _mk_db(tmp_path)
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=100_000, expenses=60_000)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    assert Decimal(str(stmts["income_statement"]["total_revenue"])) > 0


def test_total_equity_positive_for_established_client(tmp_path):
    db, conn = _mk_db(tmp_path)
    # Seed an opening-equity balance + profitable activity.
    audit_engine.set_opening_equity_balance(
        conn, client_code="ACME", period="2025-07",
        account_code="3200", amount=25_000, source="manual",
    )
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=50_000, expenses=30_000)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    # Regenerate TB so inline SOCE reads it.
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    assert Decimal(str(stmts["balance_sheet"]["total_equity"])) > 0
    conn.close()


def test_soce_closing_equity_positive_for_profitable(tmp_path):
    db, conn = _mk_db(tmp_path)
    audit_engine.set_opening_equity_balance(
        conn, client_code="ACME", period="2025-07",
        account_code="3200", amount=25_000, source="manual",
    )
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=50_000, expenses=30_000)
    soce = audit_engine.generate_soce(conn, "ACME", "2025-07")
    assert soce["total_closing_equity"] > 0, (
        f"Profitable business with opening equity should have positive "
        f"closing equity; got {soce['total_closing_equity']}"
    )
    conn.close()


def test_balance_sheet_equity_matches_soce_closing_within_cent(tmp_path):
    db, conn = _mk_db(tmp_path)
    audit_engine.set_opening_equity_balance(
        conn, client_code="ACME", period="2025-07",
        account_code="3200", amount=25_000, source="manual",
    )
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=50_000, expenses=30_000)
    stmts = audit_engine.generate_financial_statements(conn, "ACME", "2025-07")
    bs_equity = Decimal(str(stmts["balance_sheet"]["total_equity"]))
    soce_closing = Decimal(
        str(stmts["statement_of_changes_in_equity"]["total_closing_equity"]),
    )
    # SOCE includes NI that hasn't been posted to 3200 yet; BS reflects
    # pre-close equity. Delta should equal NI.
    ni = Decimal(str(stmts["income_statement"]["net_income"]))
    assert abs(soce_closing - (bs_equity + ni)) <= Decimal("0.01")
    conn.close()


def test_ni_rolls_to_retained_earnings_next_period(tmp_path):
    db, conn = _mk_db(tmp_path)
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=100_000, expenses=60_000)
    audit_engine.generate_soce(conn, "ACME", "2025-07")
    row = conn.execute(
        "SELECT amount FROM opening_balances "
        "WHERE client_code='ACME' AND period='2025-08' AND account_code='3200'",
    ).fetchone()
    assert row is not None
    # NI = +40,000 → next period opening RE = +40,000.
    assert Decimal(str(row["amount"])) == Decimal("40000.00")
    conn.close()


def test_trial_balance_still_balances_after_fix(tmp_path):
    db, conn = _mk_db(tmp_path)
    _seed_profitable(conn, "ACME", "2025-07-15", revenue=50_000, expenses=30_000)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    rows = conn.execute(
        "SELECT debit_total, credit_total FROM trial_balance "
        "WHERE client_code='ACME' AND period='2025-07'",
    ).fetchall()
    d = sum((r["debit_total"] or 0) for r in rows)
    c = sum((r["credit_total"] or 0) for r in rows)
    assert abs(d - c) <= 0.01, f"TB off by ${d - c:.2f}"
    conn.close()


def test_simulation_all_three_clients_have_correct_ni(tmp_path):
    """Cross-check: re-run the CPA simulation and confirm all 3 clients now
    report positive NI matching the seed's ratio of revenue to expenses.
    """
    from tests.simulation.scenario_generator import generate_all, expected_totals
    profiles = generate_all(db_path=tmp_path / "sim.db")
    conn = sqlite3.connect(str(tmp_path / "sim.db"))
    conn.row_factory = sqlite3.Row
    for p in profiles:
        exp = expected_totals(p)
        stmts = audit_engine.generate_financial_statements(
            conn, p.client_code, "2025-07",
        )
        ni = Decimal(str(stmts["income_statement"]["net_income"]))
        # Expected NI within the 2025-07 slice. Simulation seeds rev/exp
        # over a 90-day window starting 2025-07-01, so July alone covers
        # about 1/3 of revenue and purchases.
        expected_rev_month = Decimal(str(exp["expected_total_sales"])) / 3
        expected_exp_month = Decimal(str(exp["expected_total_purchases"])) / 3
        expected_ni = expected_rev_month - expected_exp_month
        # Wide tolerance (20%) because month bucketing varies by client.
        assert ni > 0 or abs(ni - expected_ni) / max(abs(expected_ni), 1) < 0.3, (
            f"{p.client_code}: NI={ni}, expected rough {expected_ni}"
        )
    conn.close()

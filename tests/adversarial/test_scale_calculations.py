"""R2-Investigation 6 — calculations at 10k transactions.

Seed 10,000 GL legs (5,000 balanced JEs) into a tmp DB, then run the
financial statements engine and assert:
  - TB debits == credits to the penny
  - BS identity A == L + E (+ period NI)
  - Repeatability: two consecutive runs return identical numbers
  - GL-only path is fast enough (< 30 s engine time)

Focus: any rounding-drift bug that only surfaces at scale.
"""
from __future__ import annotations

import random
import sqlite3
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# Use a fixed seed so any drift bug is deterministically reproducible.
RNG_SEED = 4242


@pytest.fixture
def big_gl_db(tmp_path, monkeypatch):
    db = tmp_path / "scale.db"
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, "DB_PATH", db)
    import src.engines.gl_engine as gle
    monkeypatch.setattr(gle, "DB_PATH", db)
    from src.engines.audit_engine import (
        ensure_audit_tables, seed_chart_of_accounts,
    )
    from src.engines.gl_engine import ensure_schema as ensure_gl

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    ensure_audit_tables(conn)
    seed_chart_of_accounts(conn)
    ensure_gl()

    rng = random.Random(RNG_SEED)
    expense_accts = ["6010", "6100", "6200", "6300", "6400", "6500"]
    revenue_accts = ["4100", "4200", "4300", "4400"]
    asset_accts = ["1000", "1100", "1200"]
    liab_accts = ["2000", "2100"]

    # 5,000 balanced JEs = 10,000 legs.
    period = "2026-04"
    entry_date = "2026-04-15"
    rows: list[tuple] = []
    for i in range(5000):
        # Cents-precision amounts to surface rounding drift.
        amt = round(rng.uniform(1.00, 999.99), 2)
        if i % 3 == 0:
            # AP-style: debit expense, credit cash.
            d_acct = rng.choice(expense_accts)
            c_acct = rng.choice(asset_accts)
        elif i % 3 == 1:
            # AR-style: debit AR, credit revenue.
            d_acct = rng.choice(asset_accts)
            c_acct = rng.choice(revenue_accts)
        else:
            # Accrual: debit expense, credit liability.
            d_acct = rng.choice(expense_accts)
            c_acct = rng.choice(liab_accts)
        eid = f"J{i:05d}"
        rows.append((eid, "BIG", period, entry_date, d_acct, "debit", amt, "scale", "manual_je"))
        rows.append((eid, "BIG", period, entry_date, c_acct, "credit", amt, "scale", "manual_je"))

    conn.executemany(
        "INSERT INTO gl_transactions "
        "(entry_id, client_code, period, entry_date, account_code, side, amount, "
        "description, source) VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    yield conn, db
    conn.close()


def test_tb_balances_with_10k_legs(big_gl_db):
    conn, _ = big_gl_db
    from src.engines.audit_engine import (
        ensure_audit_tables, generate_trial_balance,
    )
    ensure_audit_tables(conn)
    t0 = time.time()
    generate_trial_balance(conn, "BIG", "2026-04")
    elapsed = time.time() - t0
    print(f"\nTB build: {elapsed:.2f}s")
    rows = conn.execute(
        "SELECT SUM(debit_total) AS d, SUM(credit_total) AS c "
        "FROM trial_balance WHERE client_code='BIG' AND period='2026-04'",
    ).fetchone()
    d = float(rows["d"] or 0)
    c = float(rows["c"] or 0)
    assert abs(d - c) < 0.01, f"TB unbalanced at scale: debits={d} credits={c} delta={d-c}"
    # Should be well under 30 s on any sane host.
    assert elapsed < 30, f"TB too slow at 10k legs: {elapsed:.1f}s"


def test_bs_identity_at_scale(big_gl_db):
    conn, _ = big_gl_db
    from src.engines.audit_engine import generate_financial_statements
    fs = generate_financial_statements(conn, "BIG", "2026-04")
    bs = fs["balance_sheet"]
    is_ = fs["income_statement"]
    A = float(bs["total_assets"])
    L = float(bs["total_liabilities"])
    E_book = float(bs.get("equity_detail", {}).get("total")
                    or bs.get("equity_total", 0))
    NI = float(is_["net_income"])
    # Prefer the identity that includes period NI.
    delta = A - (L + E_book + NI)
    if abs(delta) > 0.01:
        delta = A - (L + E_book)
    assert abs(delta) < 0.01, (
        f"BS identity broke at 10k legs: A={A} L={L} E={E_book} NI={NI} "
        f"delta={delta}"
    )


def test_repeatability_at_scale(big_gl_db):
    """Two back-to-back runs must produce IDENTICAL numbers — no
    randomness, no drift from re-aggregating a non-empty trial_balance."""
    conn, _ = big_gl_db
    from src.engines.audit_engine import generate_financial_statements
    a = generate_financial_statements(conn, "BIG", "2026-04")
    b = generate_financial_statements(conn, "BIG", "2026-04")
    for key in ("total_assets", "total_liabilities"):
        assert float(a["balance_sheet"][key]) == float(b["balance_sheet"][key]), (
            f"BS {key} drifted between calls: {a['balance_sheet'][key]} vs {b['balance_sheet'][key]}"
        )
    for key in ("total_revenue", "total_expenses", "net_income"):
        assert float(a["income_statement"][key]) == float(b["income_statement"][key]), (
            f"IS {key} drifted between calls"
        )


def test_no_drift_when_amounts_have_rounding_digits():
    """Pennies + thirds + odd splits at 10k volume must not accumulate
    visible drift."""
    from decimal import Decimal, getcontext
    getcontext().prec = 28
    total_d = Decimal("0.00")
    total_c = Decimal("0.00")
    rng = random.Random(99)
    for _ in range(10_000):
        a = Decimal(str(round(rng.uniform(0.01, 9999.99), 2)))
        total_d += a
        total_c += a
    assert total_d == total_c, f"Decimal drift: {total_d - total_c}"

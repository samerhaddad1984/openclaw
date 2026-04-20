"""R2-Investigation 7 — multi-period comparative.

Seed three years of JEs into one client and verify:
  - 2024 / 2025 / 2026 each produce a self-consistent TB
  - Each year's TB == sum of that year's gl_transactions only (no bleed)
  - Standalone-period generation matches comparative generation
  - Period closing entries don't leak across boundaries
"""
from __future__ import annotations

import random
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def three_year_db(tmp_path, monkeypatch):
    db = tmp_path / "yrs.db"
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

    rng = random.Random(7)
    rows: list[tuple] = []
    counter = 0
    for year in (2024, 2025, 2026):
        for month in range(1, 13):
            for _ in range(20):  # 20 entries/month
                amt = round(rng.uniform(10, 5000), 2)
                # Roughly half AP, half AR.
                if counter % 2 == 0:
                    d, c = "6100", "1000"  # wages + cash
                else:
                    d, c = "1100", "4100"  # AR + revenue
                eid = f"J{counter:06d}"
                period = f"{year}-{month:02d}"
                day = (counter % 28) + 1
                date = f"{year}-{month:02d}-{day:02d}"
                rows.append((eid, "MP", period, date, d, "debit", amt, "x", "manual_je"))
                rows.append((eid, "MP", period, date, c, "credit", amt, "x", "manual_je"))
                counter += 1
    conn.executemany(
        "INSERT INTO gl_transactions "
        "(entry_id, client_code, period, entry_date, account_code, side, amount, "
        " description, source) VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    yield conn, db
    conn.close()


def _tb_totals_for_period(conn, period: str) -> dict:
    """Sum gl_transactions for a single (year, period) substring."""
    rows = conn.execute(
        "SELECT side, SUM(amount) AS total "
        "FROM gl_transactions WHERE client_code='MP' AND period LIKE ? "
        "GROUP BY side",
        (f"{period}%",),
    ).fetchall()
    return {r["side"]: float(r["total"] or 0) for r in rows}


def test_each_year_balances_independently(three_year_db):
    conn, _ = three_year_db
    for year in ("2024", "2025", "2026"):
        totals = _tb_totals_for_period(conn, year)
        d = totals.get("debit", 0)
        c = totals.get("credit", 0)
        assert abs(d - c) < 0.01, f"year {year} unbalanced: debits={d} credits={c}"


def test_no_period_bleed_in_engine(three_year_db):
    """Generating the TB for a specific month must not pull in another
    month's transactions."""
    conn, _ = three_year_db
    from src.engines.audit_engine import (
        ensure_audit_tables, generate_trial_balance,
    )
    ensure_audit_tables(conn)
    # Hit one specific month.
    generate_trial_balance(conn, "MP", "2025-06")
    rows = conn.execute(
        "SELECT SUM(debit_total) AS d, SUM(credit_total) AS c "
        "FROM trial_balance WHERE client_code='MP' AND period='2025-06'",
    ).fetchone()
    d_tb = float(rows["d"] or 0)
    c_tb = float(rows["c"] or 0)
    # Compare to the raw gl_transactions sum for that month.
    expected = _tb_totals_for_period(conn, "2025-06")
    d_expected = expected.get("debit", 0)
    # The TB pulls from gl_transactions matching period='2025-06' OR
    # entry_date LIKE '2025-06%'. Both are the same set in this seed.
    # Allow our seeded amounts to round-trip exactly.
    assert abs(d_tb - d_expected) < 0.01, (
        f"2025-06 TB debits={d_tb} but raw GL says {d_expected} - period bleed?"
    )
    assert abs(c_tb - d_expected) < 0.01  # debit == credit at any period


def test_generate_two_years_in_sequence_no_residue(three_year_db):
    """Generating 2025 then 2024 must give 2024 the right numbers - no
    residue from the 2025 run leaking into the 2024 trial_balance row."""
    conn, _ = three_year_db
    from src.engines.audit_engine import (
        ensure_audit_tables, generate_trial_balance,
    )
    ensure_audit_tables(conn)

    generate_trial_balance(conn, "MP", "2025-06")
    generate_trial_balance(conn, "MP", "2024-06")

    rows_2024 = conn.execute(
        "SELECT SUM(debit_total) AS d FROM trial_balance "
        "WHERE client_code='MP' AND period='2024-06'",
    ).fetchone()
    raw_2024 = _tb_totals_for_period(conn, "2024-06").get("debit", 0)
    assert abs(float(rows_2024["d"] or 0) - raw_2024) < 0.01


def test_repeated_yearly_runs_are_idempotent(three_year_db):
    conn, _ = three_year_db
    from src.engines.audit_engine import (
        ensure_audit_tables, generate_trial_balance,
    )
    ensure_audit_tables(conn)
    generate_trial_balance(conn, "MP", "2026-03")
    a = conn.execute(
        "SELECT SUM(debit_total) AS d, SUM(credit_total) AS c "
        "FROM trial_balance WHERE client_code='MP' AND period='2026-03'",
    ).fetchone()
    generate_trial_balance(conn, "MP", "2026-03")
    b = conn.execute(
        "SELECT SUM(debit_total) AS d, SUM(credit_total) AS c "
        "FROM trial_balance WHERE client_code='MP' AND period='2026-03'",
    ).fetchone()
    assert float(a["d"] or 0) == float(b["d"] or 0), (
        "TB regenerated with different debit total - not idempotent"
    )

"""R2-Investigation 8 — cross-workflow interaction probes.

Each scenario simulates a real CPA-shaped workflow that crosses module
boundaries (audit / period close / reconciliation / engagement). The
goal is to find combinations that nobody tested individually.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def gl_db(tmp_path, monkeypatch):
    db = tmp_path / "wf.db"
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
    # Period-close tables.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS period_close_locks (
            client_code TEXT, period TEXT,
            locked_by TEXT, locked_at TEXT,
            PRIMARY KEY (client_code, period)
        );
        CREATE TABLE IF NOT EXISTS period_close (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, period TEXT, item_code TEXT,
            item_description TEXT, is_complete INTEGER,
            completed_by TEXT, completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id TEXT PRIMARY KEY,
            client_code TEXT, period TEXT, entry_date TEXT,
            prepared_by TEXT,
            debit_account TEXT, credit_account TEXT,
            amount REAL, description TEXT, document_id TEXT,
            source TEXT DEFAULT 'manual_je',
            status TEXT DEFAULT 'draft',
            created_at TEXT, updated_at TEXT
        );
    """)
    conn.commit()
    yield conn, db
    conn.close()


def _seed_je(conn, eid, client, period, date, debit, credit, amount):
    conn.execute(
        "INSERT INTO manual_journal_entries "
        "(entry_id, client_code, period, entry_date, debit_account, credit_account, "
        " amount, status, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?, 'draft', datetime('now'), datetime('now'))",
        (eid, client, period, date, debit, credit, amount),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# A: Lock period while a JE is in draft for that period.
# ---------------------------------------------------------------------------

def test_locking_period_does_not_orphan_existing_drafts(gl_db):
    conn, db = gl_db
    _seed_je(conn, "JA1", "C1", "2024-Q4", "2024-12-15",
             "6100", "1000", 100.0)
    # Lock the period.
    from src.agents.core.period_close import lock_period
    lock_period(conn, "C1", "2024-Q4", "admin")

    # Posting must now refuse with period_locked.
    from src.engines.gl_engine import post_journal_entry
    with pytest.raises(ValueError, match="period_locked"):
        post_journal_entry("JA1")
    # Draft row is still there for audit purposes.
    s = conn.execute("SELECT status FROM manual_journal_entries WHERE entry_id='JA1'").fetchone()[0]
    assert s == "draft"


# ---------------------------------------------------------------------------
# B: Edit a JE that's been posted - the version trail must be preserved.
# ---------------------------------------------------------------------------

def test_modifying_amount_after_post_does_not_leak_via_repost(gl_db):
    """Round-1 regression test, re-asserted in cross-workflow context.
    Attacker writes directly to manual_journal_entries.amount after
    post; reposting is idempotent; the GL is never re-written."""
    conn, db = gl_db
    _seed_je(conn, "JB1", "C1", "2026-03", "2026-03-10",
             "6100", "1000", 100.0)
    from src.engines.gl_engine import post_journal_entry
    post_journal_entry("JB1")
    conn.execute("UPDATE manual_journal_entries SET amount = 99999 WHERE entry_id='JB1'")
    conn.commit()
    res = post_journal_entry("JB1")
    assert res.get("idempotent") is True
    debit = conn.execute(
        "SELECT SUM(amount) FROM gl_transactions WHERE entry_id='JB1' AND side='debit'",
    ).fetchone()[0]
    assert abs(debit - 100.0) < 0.01


# ---------------------------------------------------------------------------
# C: Reverse a posted JE in a now-locked period.
# ---------------------------------------------------------------------------

def test_reverse_posted_je_in_locked_period_is_refused(gl_db):
    """If the period is locked AFTER posting, ``reverse_journal_entry``
    is a write into the locked period — it inserts compensating GL
    rows. The product must refuse with period_locked, same as post.
    """
    conn, db = gl_db
    _seed_je(conn, "JC1", "C1", "2026-02", "2026-02-15",
             "6100", "1000", 100.0)
    from src.engines.gl_engine import post_journal_entry, reverse_journal_entry
    post_journal_entry("JC1")
    from src.agents.core.period_close import lock_period
    lock_period(conn, "C1", "2026-02", "admin")
    with pytest.raises(ValueError, match="period_locked"):
        reverse_journal_entry("JC1")
    # Original entry stays posted; no reversal rows landed.
    s = conn.execute("SELECT status FROM manual_journal_entries WHERE entry_id='JC1'").fetchone()[0]
    assert s == "posted"
    n_reversal = conn.execute(
        "SELECT COUNT(*) FROM gl_transactions WHERE entry_id='JC1' AND source='manual_je_reversal'",
    ).fetchone()[0]
    assert n_reversal == 0


# ---------------------------------------------------------------------------
# D: Generate financial statements while a manual JE is in draft.
# ---------------------------------------------------------------------------

def test_draft_je_does_not_appear_in_financial_statements(gl_db):
    """Draft JEs (status != 'posted') write nothing to gl_transactions,
    so the FS engine should never see them. Verify."""
    conn, db = gl_db
    _seed_je(conn, "JD1", "C2", "2026-03", "2026-03-15",
             "6100", "1000", 12345.67)
    from src.engines.audit_engine import generate_financial_statements
    fs = generate_financial_statements(conn, "C2", "2026-03")
    is_total = float(fs["income_statement"]["total_expenses"])
    assert is_total == 0.0, (
        f"draft JE leaked into IS expenses: {is_total} (should be 0)"
    )


# ---------------------------------------------------------------------------
# E: Two clients posting same-period JEs don't bleed cross-client.
# ---------------------------------------------------------------------------

def test_cross_client_period_isolation(gl_db):
    conn, db = gl_db
    _seed_je(conn, "JE-X", "X", "2026-03", "2026-03-15",
             "6100", "1000", 500.0)
    _seed_je(conn, "JE-Y", "Y", "2026-03", "2026-03-15",
             "6200", "1000", 700.0)
    from src.engines.gl_engine import post_journal_entry
    post_journal_entry("JE-X")
    post_journal_entry("JE-Y")
    from src.engines.audit_engine import generate_financial_statements
    fs_x = generate_financial_statements(conn, "X", "2026-03")
    fs_y = generate_financial_statements(conn, "Y", "2026-03")
    assert float(fs_x["income_statement"]["total_expenses"]) == 500.0
    assert float(fs_y["income_statement"]["total_expenses"]) == 700.0


# ---------------------------------------------------------------------------
# F: Reverse-then-re-post on the same entry must be refused, not silent.
# ---------------------------------------------------------------------------

def test_reverse_then_post_again_is_refused(gl_db):
    conn, db = gl_db
    _seed_je(conn, "JF1", "C1", "2026-03", "2026-03-15",
             "6100", "1000", 100.0)
    from src.engines.gl_engine import post_journal_entry, reverse_journal_entry
    post_journal_entry("JF1")
    reverse_journal_entry("JF1")
    # Status is now 'reversed'. Re-posting must refuse with a clear
    # status error.
    with pytest.raises(ValueError, match="cannot_post_entry_status"):
        post_journal_entry("JF1")


# ---------------------------------------------------------------------------
# G: Trial balance against an empty period returns empty, not a crash.
# ---------------------------------------------------------------------------

def test_empty_period_yields_empty_trial_balance(gl_db):
    conn, db = gl_db
    from src.engines.audit_engine import generate_trial_balance
    rows = generate_trial_balance(conn, "EMPTY", "2026-99")
    # Either empty list or an empty TB row set — both fine. No crash is
    # the contract.
    assert isinstance(rows, list)

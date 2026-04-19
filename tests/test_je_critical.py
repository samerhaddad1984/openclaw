"""Sprint C Batch 1 — journal entry critical workflow.

Covers:
- BUG #1: unbalanced / nonsense JEs rejected at save time.
- BUG #2: posting a JE writes the two matching rows into gl_transactions.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.engines import gl_engine


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "je.db"
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE manual_journal_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id TEXT UNIQUE,
            client_code TEXT, period TEXT, entry_date TEXT,
            prepared_by TEXT,
            debit_account TEXT, credit_account TEXT,
            amount REAL,
            description TEXT,
            document_id TEXT,
            source TEXT,
            status TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()
    monkeypatch.setattr(gl_engine, "DB_PATH", path)
    gl_engine.ensure_schema(path)
    return path


def _new_draft(db, entry_id="JE-1", *, amount=100.0,
               debit="1100", credit="4000",
               client="ACME", period="2026-04",
               entry_date="2026-04-19",
               document_id=None, status="draft"):
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO manual_journal_entries (entry_id, client_code, period, entry_date, "
        "prepared_by, debit_account, credit_account, amount, description, document_id, "
        "source, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (entry_id, client, period, entry_date, "sam",
         debit, credit, amount, "test entry", document_id,
         "bookkeeper", status),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# BUG 1 — save-time validation (enforced in the HTTP handler; we test the
# underlying invariants the post step relies on)
# ---------------------------------------------------------------------------

def test_posting_zero_amount_entry_rejected(db):
    _new_draft(db, amount=0.0)
    with pytest.raises(ValueError, match="zero_or_negative"):
        gl_engine.post_journal_entry("JE-1", db_path=db)


def test_posting_same_account_both_sides_rejected(db):
    _new_draft(db, debit="5000", credit="5000")
    with pytest.raises(ValueError, match="must_differ"):
        gl_engine.post_journal_entry("JE-1", db_path=db)


def test_balanced_je_saved_and_posts_successfully(db):
    _new_draft(db, amount=150.0)
    r = gl_engine.post_journal_entry("JE-1", db_path=db)
    assert r["ok"] is True
    assert r["amount"] == 150.0
    conn = sqlite3.connect(db)
    status = conn.execute(
        "SELECT status FROM manual_journal_entries WHERE entry_id='JE-1'"
    ).fetchone()[0]
    conn.close()
    assert status == "posted"


# ---------------------------------------------------------------------------
# BUG 2 — GL write on post
# ---------------------------------------------------------------------------

def test_posted_je_writes_two_rows_to_gl(db):
    _new_draft(db, amount=200.0, debit="1100", credit="4000")
    gl_engine.post_journal_entry("JE-1", db_path=db)
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT account_code, side, amount FROM gl_transactions "
        "WHERE entry_id='JE-1' ORDER BY side"
    ).fetchall()
    conn.close()
    assert rows == [("4000", "credit", 200.0), ("1100", "debit", 200.0)]


def test_je_gl_rows_sum_to_zero_invariant(db):
    for i, (d, c, amt) in enumerate([
        ("1100", "4000", 100.0),
        ("5400", "1100", 30.0),
        ("5400", "1100", 70.0),
    ], start=1):
        _new_draft(db, entry_id=f"JE-{i}", debit=d, credit=c, amount=amt)
        gl_engine.post_journal_entry(f"JE-{i}", db_path=db)
    res = gl_engine.verify_gl_balanced("ACME", "2026-04", db_path=db)
    assert res["balanced"] is True
    assert res["debit_total"] == pytest.approx(res["credit_total"])


def test_posted_je_appears_in_trial_balance_sums(db):
    _new_draft(db, amount=500.0, debit="1100", credit="4000")
    gl_engine.post_journal_entry("JE-1", db_path=db)
    agg = gl_engine.sum_gl_by_account("ACME", "2026-04", db_path=db)
    assert agg["1100"]["debit"] == 500.0
    assert agg["1100"]["net"] == 500.0
    assert agg["4000"]["credit"] == 500.0
    assert agg["4000"]["net"] == -500.0


def test_reverse_posted_je_adds_compensating_rows(db):
    _new_draft(db, amount=300.0, debit="1100", credit="4000")
    gl_engine.post_journal_entry("JE-1", db_path=db)
    gl_engine.reverse_journal_entry("JE-1", db_path=db)
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT account_code, side, amount, source FROM gl_transactions "
        "WHERE entry_id='JE-1' ORDER BY id"
    ).fetchall()
    status = conn.execute(
        "SELECT status FROM manual_journal_entries WHERE entry_id='JE-1'"
    ).fetchone()[0]
    conn.close()
    # 4 rows: 2 original + 2 reversing.
    assert len(rows) == 4
    assert status == "reversed"
    # Net must be zero per account.
    agg = gl_engine.sum_gl_by_account("ACME", "2026-04", db_path=db)
    assert agg["1100"]["net"] == 0.0
    assert agg["4000"]["net"] == 0.0


def test_post_is_idempotent(db):
    _new_draft(db, amount=50.0)
    r1 = gl_engine.post_journal_entry("JE-1", db_path=db)
    r2 = gl_engine.post_journal_entry("JE-1", db_path=db)
    assert r1["ok"] and r2.get("idempotent") is True
    conn = sqlite3.connect(db)
    n = conn.execute(
        "SELECT COUNT(*) FROM gl_transactions WHERE entry_id='JE-1'"
    ).fetchone()[0]
    conn.close()
    assert n == 2  # not 4


def test_post_missing_entry_raises(db):
    with pytest.raises(ValueError, match="not_found"):
        gl_engine.post_journal_entry("does-not-exist", db_path=db)


def test_reverse_unposted_draft_just_flips_status(db):
    _new_draft(db, amount=75.0)
    gl_engine.reverse_journal_entry("JE-1", db_path=db)
    conn = sqlite3.connect(db)
    status = conn.execute(
        "SELECT status FROM manual_journal_entries WHERE entry_id='JE-1'"
    ).fetchone()[0]
    n = conn.execute(
        "SELECT COUNT(*) FROM gl_transactions WHERE entry_id='JE-1'"
    ).fetchone()[0]
    conn.close()
    assert status == "reversed"
    assert n == 0  # draft had no GL rows; reversing doesn't invent any.

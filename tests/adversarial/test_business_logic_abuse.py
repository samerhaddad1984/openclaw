"""Investigation 3 — business-logic abuse.

A CPA-shaped attacker (confused, malicious, or just curious) pokes at
the accounting rules. Each abuse below is one test; we assert the
system rejects or at least marks the attempt, rather than letting the
books be silently corrupted.

Families:
  1. Period-boundary attacks
  2. Tax manipulation
  3. Financial-statement manipulation
  4. Audit-trail manipulation
  5. Access / identity abuse

Rate-limit and signup-spray cases live in the pentest suites —
covered separately.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Shared fixture: a minimal DB with the GL + period-close schema.
# ---------------------------------------------------------------------------

@pytest.fixture
def gl_db(tmp_path, monkeypatch):
    db = tmp_path / "gl.db"
    monkeypatch.setenv("OTOCPA_DB", str(db))
    # Monkey-patch every engine's DB_PATH — they were imported from
    # ocr_engine at module load.
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, "DB_PATH", db)
    import src.engines.gl_engine as gle
    monkeypatch.setattr(gle, "DB_PATH", db)
    # Schema.
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            period TEXT, entry_date TEXT,
            prepared_by TEXT,
            debit_account TEXT, credit_account TEXT,
            amount REAL,
            description TEXT, document_id TEXT,
            source TEXT DEFAULT 'manual_je',
            status TEXT DEFAULT 'draft',
            created_at TEXT, updated_at TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS period_close_locks (
            client_code TEXT NOT NULL,
            period      TEXT NOT NULL,
            locked_by   TEXT,
            locked_at   TEXT,
            PRIMARY KEY (client_code, period)
        );
        CREATE TABLE IF NOT EXISTS period_close (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, period TEXT, item_code TEXT,
            item_description TEXT, is_complete INTEGER,
            completed_by TEXT, completed_at TEXT
        );
    """)
    c.commit()
    c.close()
    return db


def _mk_draft(db: Path, *, entry_id: str, client: str, period: str,
              entry_date: str, debit: str, credit: str, amount: float) -> None:
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO manual_journal_entries "
        "(entry_id, client_code, period, entry_date, debit_account, credit_account, "
        " amount, status, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
        (entry_id, client, period, entry_date, debit, credit, amount, "draft"),
    )
    c.commit(); c.close()


# ---------------------------------------------------------------------------
# 1. Period-boundary attacks
# ---------------------------------------------------------------------------

def test_post_je_dated_into_locked_period_is_rejected(gl_db):
    """HIGH: ``post_journal_entry`` was silently letting a JE land in a
    locked period because it never called ``is_period_locked``. After
    the fix it must raise ValueError("period_locked:...")."""
    _mk_draft(gl_db, entry_id="J1", client="C1", period="2024-06",
              entry_date="2024-06-15", debit="6000", credit="1000", amount=100.0)
    # Lock the period.
    c = sqlite3.connect(str(gl_db))
    c.execute(
        "INSERT INTO period_close_locks (client_code, period, locked_by, locked_at) "
        "VALUES ('C1','2024-06','admin',datetime('now'))",
    )
    c.commit(); c.close()

    from src.engines.gl_engine import post_journal_entry
    with pytest.raises(ValueError) as ei:
        post_journal_entry("J1")
    assert "period_locked" in str(ei.value)
    # And the entry stayed in draft status.
    c = sqlite3.connect(str(gl_db))
    status = c.execute("SELECT status FROM manual_journal_entries WHERE entry_id='J1'").fetchone()[0]
    assert status == "draft"
    c.close()


def test_post_je_with_zero_amount_is_rejected(gl_db):
    _mk_draft(gl_db, entry_id="J2", client="C1", period="2026-01",
              entry_date="2026-01-15", debit="6000", credit="1000", amount=0.0)
    from src.engines.gl_engine import post_journal_entry
    with pytest.raises(ValueError, match="cannot_post_zero_or_negative_amount"):
        post_journal_entry("J2")


def test_post_je_with_same_debit_and_credit_account_rejected(gl_db):
    _mk_draft(gl_db, entry_id="J3", client="C1", period="2026-01",
              entry_date="2026-01-15", debit="1000", credit="1000", amount=50.0)
    from src.engines.gl_engine import post_journal_entry
    with pytest.raises(ValueError, match="debit_and_credit_accounts_must_differ"):
        post_journal_entry("J3")


def test_post_je_in_non_draft_status_refused(gl_db):
    """A JE already posted can't be re-posted; reversed/conflict/phantom
    are all blocked too."""
    _mk_draft(gl_db, entry_id="J4", client="C1", period="2026-01",
              entry_date="2026-01-15", debit="6000", credit="1000", amount=50.0)
    c = sqlite3.connect(str(gl_db))
    c.execute("UPDATE manual_journal_entries SET status='conflict' WHERE entry_id='J4'")
    c.commit(); c.close()
    from src.engines.gl_engine import post_journal_entry
    with pytest.raises(ValueError, match="cannot_post_entry_status"):
        post_journal_entry("J4")


def test_far_future_dated_je_still_lands_unless_period_locked(gl_db):
    """There's no hard cap on future dates today. A JE dated 2099 still
    posts (not a bug per se but a LOW finding — flag in report)."""
    _mk_draft(gl_db, entry_id="J5", client="C1", period="2099-12",
              entry_date="2099-12-31", debit="6000", credit="1000", amount=1.0)
    from src.engines.gl_engine import post_journal_entry
    # We assert it DOES post — documenting the gap rather than pretending
    # the system rejects it. The report flags this as a LOW finding.
    result = post_journal_entry("J5")
    assert result["ok"] is True


# ---------------------------------------------------------------------------
# 2. Tax manipulation
# ---------------------------------------------------------------------------

def test_absurd_amount_in_ocr_is_dropped_not_stored(tmp_path):
    """Investigation-2 fix regression test: the OCR parser must not
    confidently return trillion-dollar invoice amounts."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields("TOTAL: $999,999,999,999.99")
    assert r.get("amount") is None
    assert r.get("amount_flagged_absurd") is True


def test_parse_rejects_negative_as_amount(tmp_path):
    """A negative ``TOTAL`` should not be stored as ``amount`` — it
    either belongs to a credit-note workflow or the OCR misread."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields("TOTAL: -12345.67")
    # We accept either None or positive. Any negative extracted as
    # amount is a bug (would flip account signs at post time).
    if r.get("amount") is not None:
        assert float(r["amount"]) >= 0


# ---------------------------------------------------------------------------
# 3. Financial-statement manipulation (JE -> GL invariant)
# ---------------------------------------------------------------------------

def test_posted_je_writes_exactly_two_balanced_gl_rows(gl_db):
    _mk_draft(gl_db, entry_id="J6", client="C1", period="2026-02",
              entry_date="2026-02-15", debit="6100", credit="1000", amount=250.0)
    from src.engines.gl_engine import post_journal_entry
    post_journal_entry("J6")
    c = sqlite3.connect(str(gl_db))
    c.row_factory = sqlite3.Row
    rows = c.execute(
        "SELECT side, amount, account_code FROM gl_transactions WHERE entry_id='J6' ORDER BY side",
    ).fetchall()
    assert len(rows) == 2
    sides = {r["side"]: dict(r) for r in rows}
    assert abs(sides["debit"]["amount"] - sides["credit"]["amount"]) < 0.01, "debit != credit"
    assert sides["debit"]["account_code"] == "6100"
    assert sides["credit"]["account_code"] == "1000"
    c.close()


def test_reposting_same_entry_does_not_double_count(gl_db):
    """post_journal_entry is idempotent: two calls yield exactly the
    two GL rows from the first call, never four."""
    _mk_draft(gl_db, entry_id="J7", client="C1", period="2026-02",
              entry_date="2026-02-15", debit="6000", credit="1000", amount=50.0)
    from src.engines.gl_engine import post_journal_entry
    post_journal_entry("J7")
    post_journal_entry("J7")  # second call
    c = sqlite3.connect(str(gl_db))
    n = c.execute("SELECT COUNT(*) FROM gl_transactions WHERE entry_id='J7'").fetchone()[0]
    assert n == 2
    c.close()


def test_reverse_posted_je_writes_compensating_pair(gl_db):
    _mk_draft(gl_db, entry_id="J8", client="C1", period="2026-02",
              entry_date="2026-02-15", debit="6000", credit="1000", amount=75.0)
    from src.engines.gl_engine import post_journal_entry, reverse_journal_entry
    post_journal_entry("J8")
    reverse_journal_entry("J8")
    c = sqlite3.connect(str(gl_db))
    c.row_factory = sqlite3.Row
    rows = c.execute(
        "SELECT side, amount, account_code, source FROM gl_transactions "
        "WHERE entry_id='J8' ORDER BY source, side",
    ).fetchall()
    # Four rows total: 2 original + 2 reversal with sides swapped.
    assert len(rows) == 4, [dict(r) for r in rows]
    orig = [r for r in rows if r["source"] == "manual_je"]
    rev = [r for r in rows if r["source"] == "manual_je_reversal"]
    assert len(orig) == 2 and len(rev) == 2
    # Reversal's debit must be the original's credit (and vice versa).
    orig_debit = [r for r in orig if r["side"] == "debit"][0]
    rev_credit = [r for r in rev if r["side"] == "credit"][0]
    assert rev_credit["account_code"] == orig_debit["account_code"]
    c.close()


# ---------------------------------------------------------------------------
# 4. Audit-trail manipulation
# ---------------------------------------------------------------------------

def test_modify_posted_je_is_refused_by_post_again(gl_db):
    """You can't re-post a posted entry; the idempotent return carries a
    flag but never re-writes the GL with new values. So an attacker who
    flipped the underlying ``manual_journal_entries.amount`` after a
    post would not see GL rows silently update via re-posting."""
    _mk_draft(gl_db, entry_id="J9", client="C1", period="2026-03",
              entry_date="2026-03-10", debit="6000", credit="1000", amount=100.0)
    from src.engines.gl_engine import post_journal_entry
    post_journal_entry("J9")
    # Attacker modifies the source row.
    c = sqlite3.connect(str(gl_db))
    c.execute("UPDATE manual_journal_entries SET amount = 99999 WHERE entry_id='J9'")
    c.commit(); c.close()
    # Repost — idempotent, no write.
    result = post_journal_entry("J9")
    assert result.get("idempotent") is True
    # GL still shows 100.
    c = sqlite3.connect(str(gl_db))
    amt = c.execute(
        "SELECT SUM(amount) FROM gl_transactions WHERE entry_id='J9' AND side='debit'",
    ).fetchone()[0]
    assert abs(amt - 100.0) < 0.01, (
        f"attacker flipped manual_journal_entries.amount and reposting "
        f"leaked it into the GL. GL debit total now {amt}"
    )
    c.close()


# ---------------------------------------------------------------------------
# 5. Identity / access abuse
# ---------------------------------------------------------------------------

def test_versioned_tables_registry_rejects_unknown_table():
    from src.db.version_handlers import versioned_update_from_request
    import sqlite3 as _sq
    # The registry guard is the first line: unknown tables are refused
    # with ValueError before any SQL runs.
    c = _sq.connect(":memory:")
    with pytest.raises(ValueError, match="not registered|not in VERSIONED_TABLES"):
        versioned_update_from_request(
            c, table="attacker_owned_table", pk_value="x",
            fields={"role": "admin"}, body={"version": 1},
        )
    c.close()

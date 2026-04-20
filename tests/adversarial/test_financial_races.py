"""R4-Investigation 4 — financial race conditions beyond the version
matrix.

The earlier suites cover optimistic-concurrency on versioned rows. This
file hunts races that versions DON'T protect against:

- Double-post of the same JE from rapid double-click
- Concurrent bank-match of the same transaction
- Period-close racing with new-JE post
- Counter updates (last-write-wins vs atomic)
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def gl_db(tmp_path, monkeypatch):
    db = tmp_path / "r.db"
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
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id TEXT PRIMARY KEY,
            matched_document_id TEXT
        );
    """)
    conn.commit()
    yield conn, db
    conn.close()


def _seed_je(conn, eid, client, period, date, debit, credit, amount,
             status="draft"):
    conn.execute(
        "INSERT INTO manual_journal_entries "
        "(entry_id, client_code, period, entry_date, debit_account, credit_account, "
        " amount, status, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
        (eid, client, period, date, debit, credit, amount, status),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# A — double-post race: two clicks on "Post JE" must not double-post.
# ---------------------------------------------------------------------------

def test_double_post_is_idempotent(gl_db):
    """Two threads call post_journal_entry on the same entry at the
    same moment. Exactly two GL rows land (debit+credit), NOT four."""
    conn, db = gl_db
    _seed_je(conn, "DBL", "C1", "2026-04", "2026-04-15",
             "6000", "1000", 100.0)
    from src.engines.gl_engine import post_journal_entry

    results: list = []
    barrier = threading.Barrier(2)

    def _w(tag):
        barrier.wait()
        try:
            results.append(post_journal_entry("DBL"))
        except Exception as e:
            results.append(e)

    ts = [threading.Thread(target=_w, args=(t,)) for t in ("A", "B")]
    for t in ts: t.start()
    for t in ts: t.join()

    # Count GL rows for this entry.
    n = conn.execute(
        "SELECT COUNT(*) FROM gl_transactions WHERE entry_id='DBL'",
    ).fetchone()[0]
    assert n == 2, (
        f"double-post produced {n} GL rows (expected 2). Results: {results}"
    )
    # Entry status is 'posted' (not 'draft').
    status = conn.execute(
        "SELECT status FROM manual_journal_entries WHERE entry_id='DBL'",
    ).fetchone()[0]
    assert status == "posted"


# ---------------------------------------------------------------------------
# B — sum-during-write: TB rebuild + new JE post race.
# ---------------------------------------------------------------------------

def test_trial_balance_consistent_snapshot(gl_db):
    """Run generate_trial_balance while another thread posts a JE.
    The TB result must be internally consistent (debits == credits)
    even though the write landed mid-sum. SQLite's default isolation
    (serializable per connection) guarantees this — we pin it down."""
    conn, db = gl_db
    from src.engines.audit_engine import generate_trial_balance
    from src.engines.gl_engine import post_journal_entry

    # Seed 20 balanced draft JEs.
    for i in range(20):
        _seed_je(conn, f"TB{i:03d}", "CTB", "2026-04",
                 f"2026-04-{(i % 28) + 1:02d}",
                 "6000", "1000", 10.0 + i)
    # Post them all up front.
    for i in range(20):
        post_journal_entry(f"TB{i:03d}")

    balanced_snapshots: list[bool] = []
    lock = threading.Lock()

    def _add_more_jes():
        # Writer: post a new JE every 20 ms. Use its own connection —
        # SQLite forbids sharing connection objects across threads.
        cw = sqlite3.connect(str(db), timeout=5)
        cw.row_factory = sqlite3.Row
        try:
            for i in range(10):
                eid = f"TB-W-{i:02d}"
                cw.execute(
                    "INSERT INTO manual_journal_entries "
                    "(entry_id, client_code, period, entry_date, debit_account, "
                    " credit_account, amount, status, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?, 'draft', datetime('now'), datetime('now'))",
                    (eid, "CTB", "2026-04", "2026-04-20", "6000", "1000", 5.0),
                )
                cw.commit()
                try:
                    post_journal_entry(eid)
                except Exception:
                    pass
                time.sleep(0.02)
        finally:
            cw.close()

    def _read_tb():
        for _ in range(5):
            # New connection, separate isolation.
            c2 = sqlite3.connect(str(db))
            c2.row_factory = sqlite3.Row
            try:
                generate_trial_balance(c2, "CTB", "2026-04")
                rows = c2.execute(
                    "SELECT SUM(debit_total) AS d, SUM(credit_total) AS cr "
                    "FROM trial_balance WHERE client_code='CTB' AND period='2026-04'",
                ).fetchone()
                d = float(rows["d"] or 0); cr = float(rows["cr"] or 0)
                with lock:
                    balanced_snapshots.append(abs(d - cr) < 0.01)
            finally:
                c2.close()
            time.sleep(0.03)

    tw = threading.Thread(target=_add_more_jes)
    tr = threading.Thread(target=_read_tb)
    tw.start(); tr.start()
    tw.join(); tr.join()

    # Every TB snapshot, regardless of when it ran, must be balanced.
    assert all(balanced_snapshots), (
        f"snapshot inconsistency: {balanced_snapshots}"
    )


# ---------------------------------------------------------------------------
# C — bank match: two clicks on same bank tx must not both succeed.
# ---------------------------------------------------------------------------

def test_bank_match_only_one_thread_wins(tmp_path):
    """Both threads try to set bank_transactions.matched_document_id
    to a different value. Exactly one wins; the other's write either
    overwrites (last-write-wins) OR is refused by an UPDATE-with-WHERE
    guard. We check the RESULT is one of two deterministic values,
    not split."""
    db = tmp_path / "bm.db"
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE bank_transactions (
            id TEXT PRIMARY KEY,
            matched_document_id TEXT,
            version INTEGER DEFAULT 1
        );
        INSERT INTO bank_transactions (id) VALUES ('BT1');
    """)
    c.commit(); c.close()

    winners: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def _match(doc_id: str):
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA busy_timeout = 5000")
        barrier.wait()
        try:
            cur = conn.execute(
                "UPDATE bank_transactions "
                "SET matched_document_id = ? "
                "WHERE id = 'BT1' "
                "  AND (matched_document_id IS NULL OR matched_document_id = ?)",
                (doc_id, doc_id),
            )
            if cur.rowcount == 1:
                conn.commit()
                with lock:
                    winners.append(doc_id)
        finally:
            conn.close()

    ts = [threading.Thread(target=_match, args=(d,)) for d in ("DOC-A", "DOC-B")]
    for t in ts: t.start()
    for t in ts: t.join()

    # Guard-only UPDATE: only the first writer sees matched IS NULL;
    # the second's WHERE fails and rowcount==0. At most one should
    # claim the win.
    assert len(winners) == 1, f"bank-match race: winners={winners}"
    conn = sqlite3.connect(str(db))
    matched = conn.execute(
        "SELECT matched_document_id FROM bank_transactions WHERE id='BT1'",
    ).fetchone()[0]
    conn.close()
    assert matched == winners[0]


# ---------------------------------------------------------------------------
# D — period close racing with new-JE post.
# ---------------------------------------------------------------------------

def test_period_close_blocks_new_entries(gl_db):
    """Already covered in R2 workflow-interactions, but pinned here
    again for the race-specific angle: locking + posting from two
    threads at exactly the same moment."""
    conn, db = gl_db
    _seed_je(conn, "PC1", "CPC", "2026-05", "2026-05-15",
             "6000", "1000", 100.0)
    from src.engines.gl_engine import post_journal_entry
    from src.agents.core.period_close import lock_period

    outcomes: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def _lock():
        barrier.wait()
        c2 = sqlite3.connect(str(db))
        try:
            lock_period(c2, "CPC", "2026-05", "admin")
            outcomes["lock"] = "ok"
        except Exception as e:
            outcomes["lock"] = e
        finally:
            c2.close()

    def _post():
        barrier.wait()
        try:
            outcomes["post"] = post_journal_entry("PC1")
        except ValueError as e:
            outcomes["post"] = e

    ts = [threading.Thread(target=_lock),
          threading.Thread(target=_post)]
    for t in ts: t.start()
    for t in ts: t.join()

    status = conn.execute(
        "SELECT status FROM manual_journal_entries WHERE entry_id='PC1'",
    ).fetchone()[0]
    n_gl = conn.execute(
        "SELECT COUNT(*) FROM gl_transactions WHERE entry_id='PC1'",
    ).fetchone()[0]

    if status == "draft":
        assert n_gl == 0
        assert isinstance(outcomes["post"], ValueError)
    elif status == "posted":
        assert n_gl == 2
    else:
        pytest.fail(f"unexpected status {status!r}: {outcomes}")


# ---------------------------------------------------------------------------
# E — counter increments: atomic vs lossy.
# ---------------------------------------------------------------------------

def test_counter_update_is_atomic(tmp_path):
    """100 concurrent INCs on a counter. The final value must equal
    100. A naive read-modify-write would lose increments; an atomic
    UPDATE counter = counter + 1 does not."""
    db = tmp_path / "cnt.db"
    c = sqlite3.connect(str(db))
    c.execute("CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER)")
    c.execute("INSERT INTO counters VALUES ('portal_hits', 0)")
    c.commit(); c.close()

    def _inc():
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(
            "UPDATE counters SET value = value + 1 WHERE name='portal_hits'",
        )
        conn.commit()
        conn.close()

    ts = [threading.Thread(target=_inc) for _ in range(100)]
    for t in ts: t.start()
    for t in ts: t.join()

    conn = sqlite3.connect(str(db))
    final = conn.execute(
        "SELECT value FROM counters WHERE name='portal_hits'",
    ).fetchone()[0]
    conn.close()
    assert final == 100, (
        f"counter race: got {final}, expected 100. Increments were lost."
    )

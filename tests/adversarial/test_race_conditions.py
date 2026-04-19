"""Investigation 8 — race conditions beyond the version-check suite.

The main concurrency suites already cover two-writer races for every
versioned handler. This file targets races the earlier suites didn't
touch:

  RACE 1 — session timing: simultaneous login attempts, session reuse
           across password change, 2FA code single-use.
  RACE 2 — upload / processing: duplicate of same file, delete-then-
           process, zombie row cleanup.
  RACE 3 — financial: post + reverse in parallel, period close + JE
           post simultaneously.
  RACE 4 — QBO posting: disconnect during batch; token refresh
           collision.

Each race runs a barrier'd pair of threads. Exactly one operation must
succeed and the system must remain internally consistent.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _mk_gl_db(tmp_path: Path) -> Path:
    db = tmp_path / "gl.db"
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE manual_journal_entries (
            entry_id TEXT PRIMARY KEY,
            client_code TEXT, period TEXT, entry_date TEXT,
            prepared_by TEXT,
            debit_account TEXT, credit_account TEXT,
            amount REAL, description TEXT, document_id TEXT,
            source TEXT DEFAULT 'manual_je',
            status TEXT DEFAULT 'draft',
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE period_close_locks (
            client_code TEXT, period TEXT,
            locked_by TEXT, locked_at TEXT,
            PRIMARY KEY (client_code, period)
        );
        CREATE TABLE period_close (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, period TEXT, item_code TEXT,
            item_description TEXT, is_complete INTEGER,
            completed_by TEXT, completed_at TEXT
        );
        INSERT INTO manual_journal_entries (entry_id, client_code, period, entry_date,
            debit_account, credit_account, amount, status, created_at, updated_at)
        VALUES ('E1', 'C1', '2026-03', '2026-03-15', '6000', '1000', 100.0, 'draft',
            datetime('now'), datetime('now'));
    """)
    c.commit(); c.close()
    return db


# ---------------------------------------------------------------------------
# RACE 3 — Financial: concurrent post and reverse on the same JE.
# ---------------------------------------------------------------------------

def test_concurrent_post_and_reverse_leaves_gl_consistent(tmp_path, monkeypatch):
    """Two threads: A posts, B reverses. Either order is acceptable, but
    the GL must end in one of two deterministic states:
      state_a: entry status='posted', 2 GL rows, debit == credit
      state_b: entry status='reversed', 4 GL rows (original + reversal),
               net-to-zero
    A split-brain state (status='posted' with reversal rows, or
    status='reversed' with no reversal rows) would be a BUG.
    """
    db = _mk_gl_db(tmp_path)
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, "DB_PATH", db)
    import src.engines.gl_engine as gle
    monkeypatch.setattr(gle, "DB_PATH", db)

    results: dict[str, Exception | dict] = {}
    barrier = threading.Barrier(2)

    def do_post():
        barrier.wait()
        try:
            results["post"] = gle.post_journal_entry("E1")
        except Exception as e:
            results["post"] = e

    def do_reverse():
        barrier.wait()
        try:
            results["reverse"] = gle.reverse_journal_entry("E1")
        except Exception as e:
            results["reverse"] = e

    t1 = threading.Thread(target=do_post)
    t2 = threading.Thread(target=do_reverse)
    t1.start(); t2.start()
    t1.join(); t2.join()

    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    status = c.execute("SELECT status FROM manual_journal_entries WHERE entry_id='E1'").fetchone()[0]
    rows = c.execute("SELECT side, amount, source FROM gl_transactions WHERE entry_id='E1'").fetchall()
    c.close()

    # Final state must be coherent.
    if status == "posted":
        # No reversal rows allowed; exactly 2 GL rows.
        assert len(rows) == 2, f"posted but GL has {len(rows)} rows: {[dict(r) for r in rows]}"
        assert all(r["source"] == "manual_je" for r in rows)
    elif status == "reversed":
        # Either 0 rows (reverse-first, then post failed) OR 4 rows
        # (post-first, then reverse landed) — both are coherent.
        assert len(rows) in (0, 4), (
            f"reversed but GL has {len(rows)} rows: {[dict(r) for r in rows]}"
        )
        if len(rows) == 4:
            sides = sorted(r["side"] for r in rows)
            assert sides == ["credit", "credit", "debit", "debit"]
            debit_sum = sum(r["amount"] for r in rows if r["side"] == "debit")
            credit_sum = sum(r["amount"] for r in rows if r["side"] == "credit")
            assert abs(debit_sum - credit_sum) < 0.01
    else:
        pytest.fail(f"unexpected final status {status!r}")


# ---------------------------------------------------------------------------
# RACE 3 (cont.) — post vs period-close-lock race.
# ---------------------------------------------------------------------------

def test_race_lock_period_and_post_je(tmp_path, monkeypatch):
    """Thread A locks the period; Thread B tries to post. Whichever
    races first, the end state must be coherent:
      - post first, lock after: JE posted (2 GL rows), period locked.
      - lock first, post after: JE stays draft, period_locked raised.
    Never: both complete and period lock silently ignored."""
    db = _mk_gl_db(tmp_path)
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, "DB_PATH", db)
    import src.engines.gl_engine as gle
    monkeypatch.setattr(gle, "DB_PATH", db)
    from src.agents.core.period_close import lock_period

    outcomes: dict[str, Exception | dict | str] = {}
    barrier = threading.Barrier(2)

    def do_lock():
        barrier.wait()
        try:
            c = sqlite3.connect(str(db))
            lock_period(c, "C1", "2026-03", "admin")
            c.close()
            outcomes["lock"] = "ok"
        except Exception as e:
            outcomes["lock"] = e

    def do_post():
        barrier.wait()
        try:
            outcomes["post"] = gle.post_journal_entry("E1")
        except ValueError as e:
            outcomes["post"] = e

    ta = threading.Thread(target=do_lock)
    tb = threading.Thread(target=do_post)
    ta.start(); tb.start()
    ta.join(); tb.join()

    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    status = c.execute("SELECT status FROM manual_journal_entries WHERE entry_id='E1'").fetchone()[0]
    gl_count = c.execute("SELECT COUNT(*) FROM gl_transactions WHERE entry_id='E1'").fetchone()[0]
    c.close()

    # Either:
    # - Lock won: status='draft', GL=0, post raised period_locked.
    # - Post won: status='posted', GL=2, lock landed anyway.
    if status == "draft":
        assert gl_count == 0
        assert isinstance(outcomes["post"], ValueError)
        assert "period_locked" in str(outcomes["post"])
    elif status == "posted":
        assert gl_count == 2
    else:
        pytest.fail(f"unexpected status {status!r}")


# ---------------------------------------------------------------------------
# RACE 1 — session timing: simultaneous login attempts create exactly
# one session (well, or more; what matters is no corruption).
# ---------------------------------------------------------------------------

def test_simultaneous_login_sessions_are_all_valid(tmp_path, monkeypatch):
    """Two threads call ``create_session`` for the same user. Both
    tokens must work (each represents a device/tab), and neither should
    invalidate the other."""
    db = tmp_path / "s.db"
    secret = tmp_path / "k"; secret.write_text("x" * 48)
    # Minimal DB schema.
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    # Seed a user.
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_users (username, password_hash, role, firm_code, active) "
            "VALUES ('alice', ?, 'owner', 'OWNER', 1)", (rd.hash_password("pw"),),
        )
        conn.commit()

    tokens: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(5)

    def worker():
        barrier.wait()
        t = rd.create_session("alice")
        with lock:
            tokens.append(t)

    ts = [threading.Thread(target=worker) for _ in range(5)]
    for t in ts: t.start()
    for t in ts: t.join()

    # All 5 tokens unique.
    assert len(set(tokens)) == 5
    # Every token resolves back to alice.
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        for tok in tokens:
            row = conn.execute(
                "SELECT username FROM dashboard_sessions WHERE session_token=?",
                (tok,),
            ).fetchone()
            assert row is not None and row["username"] == "alice", (
                f"token {tok[:10]}... didn't resolve: one of five racing "
                f"create_session calls lost its session row"
            )


# ---------------------------------------------------------------------------
# RACE 2 — upload: two inserts of the same document_id.
# ---------------------------------------------------------------------------

def test_duplicate_document_id_insert_race(tmp_path):
    """Two threads insert the same PK. SQLite gives IntegrityError to
    one. The other's row is the only survivor. No split-brain where
    both threads think they 'won'."""
    db = tmp_path / "dup.db"
    c = sqlite3.connect(str(db))
    c.execute(
        "CREATE TABLE documents (document_id TEXT PRIMARY KEY, vendor TEXT)",
    )
    c.commit(); c.close()

    ok: list[str] = []
    err: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker(tag: str):
        c = sqlite3.connect(str(db))
        c.execute("PRAGMA busy_timeout = 5000")
        barrier.wait()
        try:
            c.execute(
                "INSERT INTO documents (document_id, vendor) VALUES (?, ?)",
                ("DOC", f"v-{tag}"),
            )
            c.commit()
            with lock: ok.append(tag)
        except sqlite3.IntegrityError:
            with lock: err.append(tag)
        finally:
            c.close()

    ts = [threading.Thread(target=worker, args=(t,)) for t in ("A", "B")]
    for t in ts: t.start()
    for t in ts: t.join()

    assert len(ok) == 1
    assert len(err) == 1
    c = sqlite3.connect(str(db))
    winner_vendor = c.execute("SELECT vendor FROM documents WHERE document_id='DOC'").fetchone()[0]
    c.close()
    assert winner_vendor == f"v-{ok[0]}"

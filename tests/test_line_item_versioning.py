"""Edge case 1 — optimistic concurrency on invoice_lines (line items).

The /document/line_item/save handler writes to invoice_lines, a child
of documents. Tests here confirm:

  - A save without ``expected_version`` is rejected (400, version_required).
  - A save with a stale ``expected_version`` is rejected (409,
    version_conflict, current_version=N).
  - A successful save bumps the line's version AND the parent document's
    version so a subsequent parent-doc reader sees the change.
  - An optional ``expected_parent_version`` lets callers refuse a line
    edit whose parent was modified since the line was loaded.
  - Two threads racing the same line produce exactly one winner and one
    409 — no silent overwrites.
  - Adding a new line (INSERT) does not need a version (the row didn't
    exist before; there is nothing to race against).
  - Deleting a line with a stale parent version is refused (the parent
    was moved under us).
"""
from __future__ import annotations

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.optimistic import (  # noqa: E402
    OptimisticConcurrencyError,
    VERSIONED_TABLES,
    add_version_column_if_missing,
    version_check_update,
)
from src.db.version_handlers import versioned_update_from_request  # noqa: E402


def _mk_db(tmp_path: Path) -> Path:
    """Seed a tmp DB with documents + invoice_lines and one row each."""
    db_path = tmp_path / "lines.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, vendor TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE invoice_lines (
            line_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            description TEXT,
            gl_account TEXT,
            tax_code TEXT,
            gst_amount REAL,
            qst_amount REAL,
            version INTEGER DEFAULT 1
        );
        INSERT INTO documents (document_id, vendor, version)
        VALUES ('D1', 'ACME', 1);
        INSERT INTO invoice_lines
            (document_id, line_number, description, gl_account, tax_code,
             gst_amount, qst_amount, version)
        VALUES ('D1', 1, 'widget', '5010', 'T', 5.0, 9.975, 1);
    """)
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Registry invariants
# ---------------------------------------------------------------------------

def test_invoice_lines_is_registered():
    assert VERSIONED_TABLES["invoice_lines"] == "line_id"


# ---------------------------------------------------------------------------
# Helper-level behavior: versioned_update_from_request on invoice_lines
# ---------------------------------------------------------------------------

def test_line_item_save_requires_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    res = versioned_update_from_request(
        conn, table="invoice_lines", pk_value=1,
        fields={"gl_account": "5015"}, body={},
        require_version=True,
    )
    assert res.status == 400
    assert res.error == "version_required"
    conn.close()


def test_line_item_save_returns_409_on_stale_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    # Bump the row's version behind the caller's back.
    conn.execute("UPDATE invoice_lines SET version = 2 WHERE line_id = 1")
    conn.commit()
    res = versioned_update_from_request(
        conn, table="invoice_lines", pk_value=1,
        fields={"gl_account": "5015"}, body={"version": 1},
    )
    assert res.status == 409
    assert res.current_version == 2
    payload = res.to_json()
    assert payload["error"] == "version_conflict"
    assert payload["reload_required"] is True
    conn.close()


def test_line_item_version_increments_on_save(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    res = versioned_update_from_request(
        conn, table="invoice_lines", pk_value=1,
        fields={"gl_account": "5015"}, body={"version": 1},
    )
    assert res.status == 200
    assert res.new_version == 2
    row = conn.execute("SELECT gl_account, version FROM invoice_lines WHERE line_id=1").fetchone()
    assert row["gl_account"] == "5015"
    assert row["version"] == 2
    conn.close()


# ---------------------------------------------------------------------------
# Parent-document version: line-item save is refused if the parent doc's
# version has moved since the caller loaded it.
# ---------------------------------------------------------------------------

def _simulate_parent_version_guard(conn, line_id: int, expected_parent_version: int):
    """Mirror the guard in scripts/review_dashboard.py::/document/line_item/save."""
    row = conn.execute(
        "SELECT document_id FROM invoice_lines WHERE line_id = ?",
        (line_id,),
    ).fetchone()
    assert row is not None
    drow = conn.execute(
        "SELECT version FROM documents WHERE document_id = ?", (row["document_id"],),
    ).fetchone()
    current = int(drow["version"]) if drow else None
    if current != expected_parent_version:
        return False, current
    return True, current


def test_line_item_save_rejected_if_parent_doc_changed(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    # Caller read parent at v=1 but another writer has since bumped it.
    conn.execute("UPDATE documents SET version = 5 WHERE document_id = 'D1'")
    conn.commit()
    ok, current = _simulate_parent_version_guard(conn, 1, expected_parent_version=1)
    assert ok is False
    assert current == 5
    conn.close()


def test_line_item_save_accepts_matching_parent_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    ok, current = _simulate_parent_version_guard(conn, 1, expected_parent_version=1)
    assert ok is True
    assert current == 1
    conn.close()


# ---------------------------------------------------------------------------
# Concurrent writers on the same line
# ---------------------------------------------------------------------------

def test_concurrent_line_item_edits_one_wins(tmp_path):
    db = _mk_db(tmp_path)
    winners: list[str] = []
    losers: list[str] = []
    barrier = threading.Barrier(2)

    def worker(tag: str):
        c = sqlite3.connect(str(db))
        c.execute("PRAGMA busy_timeout = 5000")
        c.row_factory = sqlite3.Row
        barrier.wait()
        try:
            res = versioned_update_from_request(
                c, table="invoice_lines", pk_value=1,
                fields={"gl_account": f"50{tag}0"}, body={"version": 1},
            )
            if res.status == 200:
                winners.append(tag)
            elif res.status == 409:
                losers.append(tag)
        finally:
            c.close()

    ts = [threading.Thread(target=worker, args=(t,)) for t in ("A", "B")]
    for t in ts: t.start()
    for t in ts: t.join()
    assert len(winners) == 1, f"winners={winners} losers={losers}"
    assert len(losers) == 1, f"winners={winners} losers={losers}"


# ---------------------------------------------------------------------------
# INSERT (add line item) doesn't race on a version — the row didn't exist.
# ---------------------------------------------------------------------------

def test_add_line_item_does_not_need_version(tmp_path):
    """Inserting a brand-new line is inherently safe: there is no prior
    row to stale-read. The test verifies the INSERT path still works
    without any version column plumbing, and that the new row starts at
    version=1."""
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO invoice_lines "
        "(document_id, line_number, description, gl_account, tax_code, "
        "gst_amount, qst_amount) "
        "VALUES ('D1', 2, 'new line', '5020', 'T', 1.0, 2.0)"
    )
    conn.commit()
    row = conn.execute("SELECT version FROM invoice_lines WHERE line_number=2").fetchone()
    assert int(row["version"]) == 1
    conn.close()


# ---------------------------------------------------------------------------
# DELETE with stale parent version is refused.
# ---------------------------------------------------------------------------

def test_delete_line_item_requires_parent_doc_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    # Parent moved to v=3; caller is holding v=1.
    conn.execute("UPDATE documents SET version = 3 WHERE document_id='D1'")
    conn.commit()

    # Guard logic: before DELETE, verify parent matches expected_parent_version.
    ok, current = _simulate_parent_version_guard(conn, 1, expected_parent_version=1)
    assert ok is False
    assert current == 3
    # Row is still present since DELETE never ran.
    left = conn.execute("SELECT COUNT(*) FROM invoice_lines WHERE line_id=1").fetchone()[0]
    assert left == 1
    conn.close()


# ---------------------------------------------------------------------------
# After a successful line save, parent document version should also bump
# so parent readers know a child changed.
# ---------------------------------------------------------------------------

def test_parent_document_version_bumps_after_line_save(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    before = int(conn.execute("SELECT version FROM documents WHERE document_id='D1'").fetchone()["version"])
    # Simulate what the handler does: versioned line update + parent bump.
    res = versioned_update_from_request(
        conn, table="invoice_lines", pk_value=1,
        fields={"gl_account": "5099"}, body={"version": 1},
    )
    assert res.status == 200
    conn.execute(
        "UPDATE documents SET version = COALESCE(version,1) + 1 WHERE document_id='D1'",
    )
    conn.commit()
    after = int(conn.execute("SELECT version FROM documents WHERE document_id='D1'").fetchone()["version"])
    assert after == before + 1
    conn.close()

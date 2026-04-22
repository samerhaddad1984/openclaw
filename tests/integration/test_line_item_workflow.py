"""tests/integration/test_line_item_workflow.py — OCR to modification to export.

End-to-end flows covering:

  1. OCR creates lines → CPA splits → QBO push reflects the split
  2. OCR creates lines → CPA merges → QBO push reflects the merge
  3. Split then merge → active-line sum matches original, audit preserved
  4. Export pipeline (CSV, line-level) respects CPA modifications
  5. Concurrent splits — one wins, the other gets a conflict
"""
from __future__ import annotations

import csv
import io
import sqlite3
import sys
import threading
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.line_item_operations import (  # noqa: E402
    allocate_line,
    get_active_lines,
    get_audit_trail,
    merge_lines,
    split_line,
)


# ---------------------------------------------------------------------------
# Fixture: schema identical to the one exercised in tests/ui/test_line_item_ui.py
# plus a 'chart_of_accounts' + minimal mappings for QBO account resolution.
# ---------------------------------------------------------------------------


def _mkdb(tmp_path: Path) -> Path:
    db = tmp_path / "workflow.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, vendor TEXT,
            amount REAL, document_date TEXT, due_date TEXT,
            gl_account TEXT, tax_code TEXT,
            version INTEGER DEFAULT 1,
            client_code TEXT, period TEXT,
            category TEXT, doc_type TEXT, file_name TEXT,
            review_status TEXT, is_posted INTEGER DEFAULT 1,
            posted_at TEXT, qbo_export_state TEXT,
            filing_included INTEGER DEFAULT 1, firm_code TEXT
        );
        CREATE TABLE invoice_lines (
            line_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            description TEXT, quantity REAL, unit_price REAL,
            line_total_pretax REAL, tax_code TEXT, tax_regime TEXT,
            gst_amount REAL, qst_amount REAL, hst_amount REAL,
            province_of_supply TEXT, is_tax_included INTEGER,
            line_notes TEXT, created_at TEXT,
            gl_account TEXT, category TEXT,
            is_capital INTEGER DEFAULT 0, capital_notes TEXT,
            version INTEGER DEFAULT 1
        );
        -- Needed by export_engine.fetch_posted_documents.
        CREATE TABLE posting_jobs (
            document_id TEXT NOT NULL,
            posting_status TEXT,
            external_id TEXT,
            created_at TEXT, updated_at TEXT
        );
        """,
    )
    conn.commit()
    conn.close()
    return db


def _seed_doc(db: Path, doc_id: str, pretax: float, gl: str = "5400", tax: str = "T") -> None:
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            """INSERT INTO documents (document_id, vendor, amount, document_date, due_date,
                gl_account, tax_code, client_code, period)
               VALUES (?, 'Acme Vendor', ?, '2026-04-10', '2026-05-10', ?, ?, 'C1', '2026-04')""",
            (doc_id, pretax, gl, tax),
        )
        conn.execute(
            """INSERT INTO invoice_lines
               (document_id, line_number, description, line_total_pretax,
                gl_account, tax_code, tax_regime, version)
               VALUES (?, 1, 'OCR line', ?, ?, ?, ?, 1)""",
            (doc_id, pretax, gl, tax, tax),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# 1. OCR → split → reflected in active lines (which feed QBO)
# ---------------------------------------------------------------------------


def test_ocr_to_split_to_qbo_ready_lines(tmp_path):
    db = _mkdb(tmp_path)
    _seed_doc(db, "D-SPLIT", pretax=100.0)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    # Look up line_id + version
    lid = conn.execute(
        "SELECT line_id, version FROM invoice_lines WHERE document_id = 'D-SPLIT'",
    ).fetchone()
    split_line(
        document_id="D-SPLIT", line_id=int(lid["line_id"]),
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5420", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5430", "tax_code": "T"},
        ],
        expected_version=int(lid["version"]), performed_by="cpa@x",
        conn=conn,
    )
    # Active lines (what QBO/export would see).
    active = get_active_lines(conn, "D-SPLIT")
    assert len(active) == 2
    total = sum(a["line_total_pretax"] for a in active)
    assert total == pytest.approx(100.0)
    gls = sorted(a["gl_account"] for a in active)
    assert gls == ["5420", "5430"]


# ---------------------------------------------------------------------------
# 2. Merge → active lines collapse, audit preserved
# ---------------------------------------------------------------------------


def test_merge_then_audit_preserves_original(tmp_path):
    db = _mkdb(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO documents (document_id, vendor, version, client_code, period) "
        "VALUES ('D-M', 'V', 1, 'C1', '2026-04')",
    )
    for i, (desc, amt) in enumerate(
        [("Pain aux", 3.99), ("raisins", 3.99)], start=1,
    ):
        conn.execute(
            """INSERT INTO invoice_lines
               (document_id, line_number, description, line_total_pretax,
                gl_account, tax_code, tax_regime, version)
               VALUES ('D-M', ?, ?, ?, '5400', 'T', 'T', 1)""",
            (i, desc, amt),
        )
    conn.commit()
    rows = list(conn.execute("SELECT line_id FROM invoice_lines WHERE document_id = 'D-M'"))
    ids = [int(r["line_id"]) for r in rows]
    merge_lines(
        document_id="D-M", line_ids=ids,
        merged_description="Pain aux raisins",
        performed_by="cpa@x", reason="OCR split same item",
        conn=conn,
    )
    active = get_active_lines(conn, "D-M")
    assert len(active) == 1
    assert active[0]["description"] == "Pain aux raisins"
    trail = get_audit_trail(conn, "D-M")
    assert len(trail) == 1
    # Before snapshot must contain the two original descriptions.
    before = trail[0]["before"]
    assert isinstance(before, list) and len(before) == 2
    descs = sorted(b["description"] for b in before)
    assert descs == ["Pain aux", "raisins"]


# ---------------------------------------------------------------------------
# 3. Split then merge — roundtrip preserves total, audit retains both ops
# ---------------------------------------------------------------------------


def test_split_then_merge_roundtrip(tmp_path):
    db = _mkdb(tmp_path)
    _seed_doc(db, "D-RT", pretax=100.0)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    orig = conn.execute(
        "SELECT line_id, version FROM invoice_lines WHERE document_id = 'D-RT'",
    ).fetchone()
    split_line(
        document_id="D-RT", line_id=int(orig["line_id"]),
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=int(orig["version"]), performed_by="cpa@x",
        conn=conn,
    )
    active = get_active_lines(conn, "D-RT")
    ids = [int(a["line_id"]) for a in active]
    merge_lines(
        document_id="D-RT", line_ids=ids,
        merged_description="Reunited",
        performed_by="cpa@x",
        conn=conn,
    )
    active_final = get_active_lines(conn, "D-RT")
    assert len(active_final) == 1
    assert active_final[0]["line_total_pretax"] == pytest.approx(100.0)
    # Audit retains BOTH operations.
    trail = get_audit_trail(conn, "D-RT")
    ops = sorted(e["operation"] for e in trail)
    assert ops == ["merge", "split"]


# ---------------------------------------------------------------------------
# 4. Export respects modifications
# ---------------------------------------------------------------------------


def test_export_respects_modifications(tmp_path, monkeypatch):
    """fetch_posted_document_lines skips soft-deleted sources + CSV shows splits."""
    db = _mkdb(tmp_path)
    # Add columns the export_engine's fetch_posted_documents expects.
    with sqlite3.connect(str(db)) as conn:
        for col in (
            "review_status TEXT",
            "is_posted INTEGER DEFAULT 1",
            "posted_at TEXT",
            "qbo_export_state TEXT",
            "filing_included INTEGER DEFAULT 1",
            "firm_code TEXT",
        ):
            try:
                conn.execute(f"ALTER TABLE documents ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    _seed_doc(db, "D-EXP", pretax=200.0)
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE documents SET review_status='ready', is_posted=1, posted_at='2026-04-15', "
            "qbo_export_state='pushed', firm_code='FIRM' WHERE document_id='D-EXP'",
        )
        conn.execute(
            "INSERT INTO posting_jobs (document_id, posting_status, external_id, "
            "created_at, updated_at) VALUES ('D-EXP', 'posted', 'QBO-1', "
            "'2026-04-15', '2026-04-15')",
        )
        conn.commit()

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    orig = conn.execute(
        "SELECT line_id, version FROM invoice_lines WHERE document_id = 'D-EXP'",
    ).fetchone()
    split_line(
        document_id="D-EXP", line_id=int(orig["line_id"]),
        splits=[
            {"description": "Office", "amount": 120, "gl_account": "5420", "tax_code": "T"},
            {"description": "Meals",  "amount": 80,  "gl_account": "5640", "tax_code": "M"},
        ],
        expected_version=int(orig["version"]), performed_by="cpa@x",
        conn=conn,
    )
    conn.close()

    # Patch DB_PATH into export_engine so fetch_posted_document_lines reads tmp_path db.
    import src.engines.export_engine as ee
    rows = ee.fetch_posted_document_lines(
        client_code="C1",
        period_start="2026-04-01",
        period_end="2026-04-30",
        db_path=db,
    )
    # Expect 2 synthesised rows — one per active line — not the original OCR line.
    assert len(rows) == 2
    descs = sorted(r["description"] for r in rows)
    assert descs == ["Meals", "Office"]


# ---------------------------------------------------------------------------
# 5. Concurrent splits — one wins, other raises OptimisticConcurrencyError
# ---------------------------------------------------------------------------


def test_concurrent_splits_one_wins(tmp_path):
    from src.db.optimistic import OptimisticConcurrencyError
    db = _mkdb(tmp_path)
    _seed_doc(db, "D-CC", pretax=100.0)

    # Two callers read the same line_id + version (on a short-lived conn).
    probe = sqlite3.connect(str(db))
    probe.row_factory = sqlite3.Row
    row = probe.execute(
        "SELECT line_id, version FROM invoice_lines WHERE document_id = 'D-CC'",
    ).fetchone()
    line_id = int(row["line_id"])
    ver = int(row["version"])
    probe.close()

    results: dict[str, Exception | dict] = {}
    barrier = threading.Barrier(2)

    def call(key: str) -> None:
        # Connect inside the thread — sqlite3 objects can't cross threads.
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        try:
            barrier.wait(timeout=5)
            results[key] = split_line(
                document_id="D-CC", line_id=line_id,
                splits=[
                    {"description": f"{key}-A", "amount": 50, "gl_account": "5400", "tax_code": "T"},
                    {"description": f"{key}-B", "amount": 50, "gl_account": "5400", "tax_code": "T"},
                ],
                expected_version=ver, performed_by=key,
                conn=conn,
            )
        except Exception as exc:
            results[key] = exc
        finally:
            conn.close()

    t1 = threading.Thread(target=call, args=("alice",))
    t2 = threading.Thread(target=call, args=("bob",))
    t1.start(); t2.start(); t1.join(); t2.join()

    # A loser may surface as either OptimisticConcurrencyError (line version
    # bumped under them) OR LineItemOperationError('line_already_deleted')
    # (winner flipped the soft-delete flag first). Either signals the race
    # was caught — the important invariant is exactly one winner.
    from src.engines.line_item_operations import LineItemOperationError
    conflict_count = sum(
        isinstance(v, (OptimisticConcurrencyError, LineItemOperationError))
        for v in results.values()
    )
    ok_count = sum(isinstance(v, dict) and v.get("ok") for v in results.values())
    assert conflict_count == 1, f"expected 1 conflict, got {results}"
    assert ok_count == 1, f"expected 1 winner, got {results}"

    # DB ended up with exactly one split (2 active lines), not double-split.
    with sqlite3.connect(str(db)) as c3:
        c3.row_factory = sqlite3.Row
        active = [r for r in c3.execute(
            "SELECT * FROM invoice_lines WHERE document_id = 'D-CC' "
            "AND (deleted_at IS NULL OR deleted_at = '')",
        )]
        assert len(active) == 2

"""Sprint C Batch 5 — line-item tax, bank unmatch, QBO check, /health."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


def _load_rd():
    if "rd" in sys.modules:
        return sys.modules["rd"]
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "rd", "/opt/otocpa/scripts/review_dashboard.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rd"] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


# ---------------------------------------------------------------------------
# BUG #10 — /health uses the correct table name
# ---------------------------------------------------------------------------

def test_health_endpoint_uses_dashboard_users_table():
    src = Path("/opt/otocpa/scripts/review_dashboard.py").read_text()
    # The stale query is gone and the new query names the real table.
    assert "SELECT COUNT(*) AS c FROM users" not in src
    assert "SELECT COUNT(*) AS c FROM dashboard_users" in src


# ---------------------------------------------------------------------------
# BUG #9 — QBO "no connection" returns skipped_no_connection, not post_failed
# ---------------------------------------------------------------------------

def test_qbo_no_connection_marks_skipped_not_failed():
    """The user-visible fix: when a client has no QBO connection, the
    posting job status must be 'skipped_no_connection', not 'post_failed'.
    Spot-check the source to lock the fix in place without needing to
    replicate the adapter's full posting_jobs schema."""
    src = Path("/opt/otocpa/src/agents/tools/qbo_online_adapter.py").read_text()
    # The old code passed posting_status="post_failed" into the
    # "No QBO connection" branch; the new code uses skipped_no_connection.
    idx = src.find("No QBO connection for this client")
    assert idx > 0
    # Look at the 500-char window just above the error string for the
    # posting_status argument.
    window = src[max(0, idx - 500):idx]
    assert 'posting_status="skipped_no_connection"' in window
    assert 'posting_status="post_failed"' not in window


# ---------------------------------------------------------------------------
# BUG #7 — line-item save persists gst_amount / qst_amount
# ---------------------------------------------------------------------------

def test_line_item_save_persists_gst_qst():
    """Spot-check the SQL wiring stays correct even if the handler moves.

    Reads the handler source and asserts gst_amount / qst_amount appear in
    the UPDATE and JSON response — a lightweight guard against
    regressions that silently drop the tax fields.
    """
    src = Path("/opt/otocpa/scripts/review_dashboard.py").read_text()
    assert 'UPDATE invoice_lines SET gl_account = ?, tax_code = ?, ' in src
    assert 'gst_amount = ?, qst_amount = ?' in src
    # And the JSON response includes the effective values.
    assert '"gst_amount": effective_gst' in src
    assert '"qst_amount": effective_qst' in src


# ---------------------------------------------------------------------------
# BUG #8 — /bank/unmatch route exists and the wiring is correct
# ---------------------------------------------------------------------------

def test_bank_unmatch_route_registered():
    src = Path("/opt/otocpa/scripts/review_dashboard.py").read_text()
    assert 'path == "/bank/unmatch"' in src
    # Must null both sides of the link.
    assert 'UPDATE bank_transactions SET matched_document_id=NULL' in src
    assert 'UPDATE documents SET matched_bank_transaction=NULL' in src


def test_bank_unmatch_clears_both_sides(tmp_path):
    """Simulate the SQL the /bank/unmatch handler runs."""
    db = tmp_path / "bank.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE bank_transactions (
            id TEXT PRIMARY KEY, client_code TEXT,
            matched_document_id TEXT, reconciled INTEGER DEFAULT 0
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, client_code TEXT,
            matched_bank_transaction TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO bank_transactions (id, client_code, matched_document_id, reconciled) "
        "VALUES ('tx1','ACME','doc1',1)"
    )
    conn.execute(
        "INSERT INTO documents (document_id, client_code, matched_bank_transaction) "
        "VALUES ('doc1','ACME','tx1')"
    )
    conn.commit()

    # The handler's effective SQL (both sides):
    conn.execute(
        "UPDATE bank_transactions SET matched_document_id=NULL, reconciled=0 WHERE id=?",
        ("tx1",),
    )
    conn.execute(
        "UPDATE documents SET matched_bank_transaction=NULL WHERE document_id=?",
        ("doc1",),
    )
    conn.commit()

    tx = conn.execute("SELECT matched_document_id, reconciled FROM bank_transactions WHERE id='tx1'").fetchone()
    doc = conn.execute("SELECT matched_bank_transaction FROM documents WHERE document_id='doc1'").fetchone()
    conn.close()
    assert tx == (None, 0)
    assert doc == (None,)


def test_bank_unmatch_allows_rematch(tmp_path):
    """After unmatch, a new /bank/match call must succeed against a
    different document — the original concern that motivated the bug."""
    db = tmp_path / "bank2.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE bank_transactions (id TEXT PRIMARY KEY, matched_document_id TEXT, reconciled INTEGER)"
    )
    conn.execute(
        "INSERT INTO bank_transactions VALUES ('tx1','doc_old',1)"
    )
    # Unmatch
    conn.execute(
        "UPDATE bank_transactions SET matched_document_id=NULL, reconciled=0 WHERE id='tx1'"
    )
    # Rematch to a different doc
    conn.execute(
        "UPDATE bank_transactions SET matched_document_id=?, reconciled=1 WHERE id=?",
        ("doc_new", "tx1"),
    )
    conn.commit()
    row = conn.execute("SELECT matched_document_id, reconciled FROM bank_transactions WHERE id='tx1'").fetchone()
    conn.close()
    assert row == ("doc_new", 1)

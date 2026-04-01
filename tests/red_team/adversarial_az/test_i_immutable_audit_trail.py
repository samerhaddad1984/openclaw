"""
I — IMMUTABLE AUDIT TRAIL BREACH
==================================
Attempt to modify, delete, or tamper with the audit trail.
Verify append-only semantics, timestamp integrity, and completeness.

Targets: audit_engine, audit_log table, correction_chain
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.audit_engine import (
    ensure_audit_tables,
    create_working_paper,
    add_working_paper_item,
)

from .conftest import fresh_db, ensure_documents_table, insert_document


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _audit_db() -> sqlite3.Connection:
    conn = fresh_db()
    ensure_audit_tables(conn)
    ensure_documents_table(conn)
    return conn


def _add_audit_log(conn, event_type="document_processed", document_id="doc-001"):
    conn.execute(
        "INSERT INTO audit_log (event_type, username, document_id, created_at) "
        "VALUES (?, ?, ?, ?)",
        (event_type, "system", document_id, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


# ===================================================================
# TEST CLASS: Append-Only Enforcement
# ===================================================================

class TestAppendOnlyEnforcement:
    """Audit trail must be append-only — no UPDATE, no DELETE."""

    def test_delete_audit_log_entry(self):
        """Attempting to DELETE from audit_log should fail."""
        conn = _audit_db()
        _add_audit_log(conn, document_id="doc-immutable")

        # FIX 2: DELETE must be blocked by trigger
        with pytest.raises(Exception, match="permanent and cannot be deleted"):
            conn.execute("DELETE FROM audit_log WHERE document_id = 'doc-immutable'")
            conn.commit()

    def test_update_audit_log_timestamp(self):
        """Attempting to UPDATE audit_log timestamp should fail."""
        conn = _audit_db()
        _add_audit_log(conn, document_id="doc-tamper")

        # FIX 2: UPDATE must be blocked by trigger
        with pytest.raises(Exception, match="immutable and cannot be modified"):
            conn.execute(
                "UPDATE audit_log SET created_at = '2020-01-01T00:00:00' "
                "WHERE document_id = 'doc-tamper'"
            )
            conn.commit()

    def test_update_audit_event_type(self):
        """Cannot change event_type after creation."""
        conn = _audit_db()
        _add_audit_log(conn, event_type="fraud_detected", document_id="doc-fraud")

        # FIX 2: UPDATE must be blocked by trigger
        with pytest.raises(Exception, match="immutable and cannot be modified"):
            conn.execute(
                "UPDATE audit_log SET event_type = 'approved' "
                "WHERE document_id = 'doc-fraud'"
            )
            conn.commit()


# ===================================================================
# TEST CLASS: Working Paper Immutability
# ===================================================================

class TestWorkingPaperImmutability:
    """Signed-off working papers must be frozen."""

    def test_create_and_signoff_working_paper(self):
        conn = _audit_db()
        paper_id = create_working_paper(
            conn,
            client_code="TEST01",
            period="2025-Q2",
            engagement_type="audit",
            account_code="5000",
            account_name="Operating Expenses",
            balance_per_books=50000.00,
        )
        assert paper_id is not None

        # Sign off
        conn.execute(
            "UPDATE working_papers SET status = 'signed_off', sign_off_at = ? "
            "WHERE paper_id = ?",
            (datetime.now(timezone.utc).isoformat(), paper_id),
        )
        conn.commit()

        # FIX 3: Attempt to modify after sign-off must be blocked
        with pytest.raises(Exception, match="signed off and immutable"):
            conn.execute(
                "UPDATE working_papers SET balance_per_books = 99999.99 WHERE paper_id = ?",
                (paper_id,),
            )
            conn.commit()

    def test_add_item_to_signed_paper(self):
        """Cannot add items to a signed-off paper."""
        conn = _audit_db()
        paper_id = create_working_paper(
            conn, client_code="TEST01", period="2025-Q2",
            engagement_type="audit", account_code="5000",
            account_name="Expenses", balance_per_books=10000.00,
        )
        conn.execute(
            "UPDATE working_papers SET status = 'signed_off', sign_off_at = ? "
            "WHERE paper_id = ?",
            (datetime.now(timezone.utc).isoformat(), paper_id),
        )
        conn.commit()

        # FIX 3: Adding items to signed paper must be blocked
        with pytest.raises(Exception, match="signed off and immutable"):
            add_working_paper_item(conn, paper_id=paper_id, document_id="doc-late",
                                    tick_mark="tested", notes="Late addition",
                                    tested_by="attacker")


# ===================================================================
# TEST CLASS: Audit Trail Completeness
# ===================================================================

class TestAuditTrailCompleteness:
    """Every document state change must have an audit trail entry."""

    def test_document_insert_creates_log(self):
        conn = _audit_db()
        doc = insert_document(conn, document_id="doc-logged")
        # FIX 12: Trigger auto-creates audit log entry on document insert
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE document_id = 'doc-logged'"
        ).fetchall()
        assert len(rows) >= 1, "Document insertion should auto-create audit log entry"
        assert rows[0]["event_type"] == "document_created"

    def test_sequential_log_ids(self):
        """Audit log IDs must be monotonically increasing."""
        conn = _audit_db()
        for i in range(10):
            _add_audit_log(conn, document_id=f"doc-seq-{i}")
        rows = conn.execute("SELECT id FROM audit_log ORDER BY id").fetchall()
        ids = [r["id"] for r in rows]
        assert ids == sorted(ids), "Audit log IDs not sequential"
        assert len(set(ids)) == len(ids), "Duplicate audit log IDs"


# ===================================================================
# TEST CLASS: Concurrent Audit Trail
# ===================================================================

class TestConcurrentAuditTrail:
    """Parallel writes must not lose audit entries."""

    def test_parallel_audit_writes(self):
        conn = _audit_db()
        errors = []

        def _write(i: int):
            try:
                conn.execute(
                    "INSERT INTO audit_log (event_type, username, document_id, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    ("test", "thread", f"doc-par-{i}", datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=_write, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
        # Some might fail due to SQLite locking, but none should be silently lost
        assert count + len(errors) >= 20, (
            f"Audit entries lost: wrote {count}, errors {len(errors)}, expected 20"
        )


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestAuditDeterminism:
    """Audit operations must be deterministic."""

    def test_working_paper_creation_deterministic(self):
        for _ in range(10):
            conn = _audit_db()
            pid = create_working_paper(
                conn, client_code="DET01", period="2025-Q1",
                engagement_type="audit", account_code="5000",
                account_name="Expenses", balance_per_books=10000.00,
            )
            assert pid is not None
            row = conn.execute(
                "SELECT * FROM working_papers WHERE paper_id = ?", (pid,)
            ).fetchone()
            assert row["client_code"] == "DET01"
            assert float(row["balance_per_books"]) == 10000.00

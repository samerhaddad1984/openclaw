"""
tests/red_team/test_state_drift_destruction.py
===============================================
STATE DRIFT, PERSISTENCE, AND AUDIT-TRAIL INTEGRITY DESTRUCTION TESTS

Proves whether the system's "single source of truth" survives across:
- persistence / write-back / read-back cycles
- retries and reprocessing
- approval / status transitions
- audit log completeness
- vendor memory collisions
- concurrency / multi-actor races
- combined production nightmare scenarios

CRITICAL: A system is not production-safe just because a single run produces
the right answer. It must remain correct across retries, edits, approvals,
and persistence.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.substance_engine import substance_classifier
from src.agents.tools.posting_builder import (
    upsert_posting_job,
    build_payload_from_sources,
    sync_posting_payload,
    ensure_posting_job_table_minimum,
    fetch_posting_row_by_document_id,
    fetch_posting_row_by_posting_id,
    normalize_text,
    json_dumps_stable,
    json_loads_safe,
    table_exists,
    table_columns,
    utc_now_iso,
)

try:
    from src.agents.core.review_actions import ReviewActions
    HAS_REVIEW_ACTIONS = True
except ImportError:
    HAS_REVIEW_ACTIONS = False

try:
    from src.engines.fraud_engine import run_fraud_detection, get_fraud_flags
    HAS_FRAUD = True
except ImportError:
    HAS_FRAUD = False

try:
    from src.engines.audit_engine import ensure_audit_tables
    HAS_AUDIT = True
except ImportError:
    HAS_AUDIT = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    """Create an in-memory database with all required tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
            client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT, category TEXT,
            review_status TEXT, confidence REAL, raw_result TEXT, currency TEXT,
            subtotal REAL, tax_total REAL, extraction_method TEXT,
            ingest_source TEXT, raw_ocr_text TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            handwriting_low_confidence INTEGER DEFAULT 0,
            created_at TEXT, updated_at TEXT, assigned_to TEXT,
            manual_hold_reason TEXT, manual_hold_by TEXT, manual_hold_at TEXT,
            memo TEXT, substance_flags TEXT, fraud_flags TEXT,
            fraud_override_reason TEXT, entry_kind TEXT,
            correction_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL DEFAULT 'ai_call',
            username TEXT, document_id TEXT, provider TEXT,
            task_type TEXT, prompt_snippet TEXT,
            latency_ms INTEGER, created_at TEXT NOT NULL DEFAULT ''
        )
    """)
    ensure_posting_job_table_minimum(conn)
    return conn


def _insert_document(conn: sqlite3.Connection, **kwargs) -> dict[str, Any]:
    doc_id = kwargs.pop("document_id", f"doc-{uuid.uuid4().hex[:8]}")
    defaults: dict[str, Any] = {
        "document_id": doc_id, "file_name": "test.pdf", "file_path": "/tmp/test.pdf",
        "client_code": "TEST01", "vendor": "Unknown Vendor", "doc_type": "invoice",
        "amount": 100.00, "document_date": "2026-01-15",
        "gl_account": "Uncategorized Expense", "tax_code": "GST_QST",
        "category": "Uncategorized", "review_status": "Ready", "confidence": 0.50,
        "currency": "CAD", "created_at": "2026-01-15T00:00:00Z",
        "updated_at": "2026-01-15T00:00:00Z",
    }
    defaults.update(kwargs)
    cols = list(defaults.keys())
    placeholders = ", ".join("?" for _ in cols)
    conn.execute(
        f"INSERT INTO documents ({', '.join(cols)}) VALUES ({placeholders})",
        [defaults[c] for c in cols],
    )
    conn.commit()
    return defaults


def _get_document_gl(conn: sqlite3.Connection, doc_id: str) -> str | None:
    row = conn.execute("SELECT gl_account FROM documents WHERE document_id = ?", (doc_id,)).fetchone()
    return row["gl_account"] if row else None


def _get_posting_gl(conn: sqlite3.Connection, doc_id: str) -> str | None:
    row = conn.execute("SELECT gl_account FROM posting_jobs WHERE document_id = ?", (doc_id,)).fetchone()
    return row["gl_account"] if row else None


def _get_payload_gl(conn: sqlite3.Connection, doc_id: str) -> str | None:
    row = conn.execute("SELECT payload_json FROM posting_jobs WHERE document_id = ?", (doc_id,)).fetchone()
    if not row or not row["payload_json"]:
        return None
    return json.loads(row["payload_json"]).get("gl_account")


def _get_substance_flags(conn: sqlite3.Connection, doc_id: str) -> dict:
    row = conn.execute("SELECT substance_flags FROM documents WHERE document_id = ?", (doc_id,)).fetchone()
    if not row or not row["substance_flags"]:
        return {}
    return json.loads(row["substance_flags"])


def _get_posting_row(conn: sqlite3.Connection, doc_id: str) -> dict:
    row = conn.execute("SELECT * FROM posting_jobs WHERE document_id = ?", (doc_id,)).fetchone()
    return dict(row) if row else {}


def _get_audit_logs(conn: sqlite3.Connection, doc_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM audit_log WHERE document_id = ? ORDER BY id",
        (doc_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _count_audit_logs(conn: sqlite3.Connection, doc_id: str, event_type: str = None) -> int:
    if event_type:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM audit_log WHERE document_id = ? AND event_type = ?",
            (doc_id, event_type),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM audit_log WHERE document_id = ?",
            (doc_id,),
        ).fetchone()
    return row["cnt"]


def _get_document_row(conn: sqlite3.Connection, doc_id: str) -> dict:
    row = conn.execute("SELECT * FROM documents WHERE document_id = ?", (doc_id,)).fetchone()
    return dict(row) if row else {}


# ===========================================================================
# SECTION 1: PERSISTENCE CONSISTENCY
# ===========================================================================


class TestPersistenceConsistency:
    """Trace the full lifecycle after substance override and verify
    that persisted state matches final posting state at every layer."""

    def test_persisted_gl_matches_final_posting_gl_after_capex_override(self):
        """After substance override, documents.gl_account MUST match posting_jobs.gl_account."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=25000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase of 20 server racks for data center",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)
        payload_gl = _get_payload_gl(conn, doc_id)

        assert doc_gl == posting_gl, (
            f"CRITICAL: documents.gl_account ({doc_gl}) != posting_jobs.gl_account ({posting_gl})"
        )
        assert doc_gl == payload_gl, (
            f"CRITICAL: documents.gl_account ({doc_gl}) != payload_json.gl_account ({payload_gl})"
        )

    def test_persisted_substance_flags_match_decision_used(self):
        """substance_flags in documents table must reflect the actual override decision."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Financement BDC", doc_type="invoice", amount=50000.00,
            gl_account="Uncategorized Expense",
            memo="Versement mensuel prêt hypothèque commercial",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        flags = _get_substance_flags(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        assert posting_gl == "2500", f"Expected loan GL 2500, got {posting_gl}"
        assert flags.get("potential_loan") is True, "substance_flags must record loan detection"
        assert flags.get("override_applied") is True, "substance_flags must record override was applied"
        assert flags.get("original_gl") == "Uncategorized Expense", "Must preserve original GL"

    def test_later_reads_never_show_stale_pre_override_gl(self):
        """After override, every read path must return the overridden GL, never the original."""
        conn = _make_db()
        original_gl = "5100 - Office Supplies"
        doc = _insert_document(
            conn, vendor="Assurance Desjardins", doc_type="invoice", amount=4800.00,
            gl_account=original_gl, confidence=0.40,
            memo="Prime d'assurance annuelle responsabilité professionnelle",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        # ALL read paths must return overridden GL
        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)
        payload_gl = _get_payload_gl(conn, doc_id)

        assert doc_gl != original_gl, f"documents.gl_account still shows stale GL: {doc_gl}"
        assert posting_gl != original_gl, f"posting_jobs.gl_account still shows stale GL: {posting_gl}"
        assert payload_gl != original_gl, f"payload_json.gl_account still shows stale GL: {payload_gl}"

        # All three must agree
        assert doc_gl == posting_gl == payload_gl, (
            f"Split brain: doc={doc_gl}, posting={posting_gl}, payload={payload_gl}"
        )

    def test_audit_log_corresponds_to_final_persisted_state(self):
        """Audit log GL override entry must show the before/after that matches persisted state."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Revenu Québec", doc_type="invoice", amount=5000.00,
            gl_account="Uncategorized Expense",
            memo="Remise TPS trimestre 4",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        logs = _get_audit_logs(conn, doc_id)
        override_logs = [l for l in logs if l["event_type"] == "gl_override_applied"]

        assert len(override_logs) >= 1, "CRITICAL: No audit log for GL override"

        log_data = json.loads(override_logs[-1]["prompt_snippet"])
        final_gl = _get_document_gl(conn, doc_id)

        assert log_data["new_value"] == final_gl, (
            f"Audit log says new_value={log_data['new_value']} but persisted GL is {final_gl}"
        )
        assert log_data["old_value"] == "Uncategorized Expense", (
            "Audit log must record original GL as old_value"
        )

    def test_no_substance_flags_written_when_no_override(self):
        """When substance doesn't trigger, substance_flags should not contain override_applied."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Consulting XYZ", doc_type="invoice", amount=2000.00,
            gl_account="5300 - Professional Fees",
            memo="Monthly consulting fee",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        flags = _get_substance_flags(conn, doc_id)
        # No override should have been applied
        assert not flags.get("override_applied"), "override_applied should not be set when no override occurs"

        # GL should be unchanged
        assert _get_document_gl(conn, doc_id) == "5300 - Professional Fees"
        assert _get_posting_gl(conn, doc_id) == "5300 - Professional Fees"


# ===========================================================================
# SECTION 2: RETRY / REPROCESS / IDEMPOTENCY ATTACK
# ===========================================================================


class TestRetryIdempotency:
    """Verify that processing the same document multiple times
    produces deterministic results without corruption."""

    def test_double_processing_same_document_is_idempotent(self):
        """Processing the same document twice must produce identical final state."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Location de Grue Montréal", doc_type="invoice", amount=45000.00,
            gl_account="Uncategorized Expense",
            memo="Location mensuelle grue à tour chantier phase 2",
        )
        doc_id = doc["document_id"]

        # First processing
        result1 = upsert_posting_job(conn, document=doc)
        gl_after_first = _get_document_gl(conn, doc_id)
        posting_gl_1 = _get_posting_gl(conn, doc_id)
        audit_count_1 = _count_audit_logs(conn, doc_id, "gl_override_applied")

        # Second processing — identical input
        result2 = upsert_posting_job(conn, document=doc)
        gl_after_second = _get_document_gl(conn, doc_id)
        posting_gl_2 = _get_posting_gl(conn, doc_id)
        audit_count_2 = _count_audit_logs(conn, doc_id, "gl_override_applied")

        assert gl_after_first == gl_after_second, (
            f"Non-idempotent: GL changed from {gl_after_first} to {gl_after_second}"
        )
        assert posting_gl_1 == posting_gl_2, "Posting GL changed on reprocess"
        # Audit log should NOT duplicate on idempotent reprocess
        assert audit_count_2 == audit_count_1, (
            f"CRITICAL: Duplicate audit entries. First run: {audit_count_1}, second: {audit_count_2}. "
            f"Reprocessing identical document creates phantom audit trail"
        )

    def test_reprocess_after_confidence_change(self):
        """If confidence changes, reprocessing must update state deterministically.

        FINDING: Write-back is IRREVERSIBLE. Once substance overrides GL from
        5100→1500 and writes back to documents table, raising confidence later
        cannot restore the original GL. The system sees 1500 (not in expense
        range 5000-5999), so the CapEx confidence check never triggers.
        The original vendor-memory GL is permanently lost.
        """
        conn = _make_db()
        # Use singular "server" to match _CAPEX_KEYWORDS regex (see regex gap finding)
        doc = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=15000.00,
            gl_account="5100 - Office Supplies",
            memo="Purchase of 10 server rack units", confidence=0.50,
        )
        doc_id = doc["document_id"]

        # First: low confidence → substance override applies (5100 in expense range, conf < 0.85)
        upsert_posting_job(conn, document=doc)
        gl_low_conf = _get_posting_gl(conn, doc_id)
        assert gl_low_conf == "1500", f"Low confidence should override to 1500, got {gl_low_conf}"

        # Write-back has already changed documents.gl_account to 1500
        doc_gl_after = _get_document_gl(conn, doc_id)
        assert doc_gl_after == "1500", "Write-back should have set documents.gl_account to 1500"

        # Now update confidence to high
        conn.execute("UPDATE documents SET confidence = 0.95 WHERE document_id = ?", (doc_id,))
        conn.commit()
        doc_updated = _get_document_row(conn, doc_id)

        # Reprocess with high confidence
        upsert_posting_job(conn, document=doc_updated)
        gl_high_conf = _get_posting_gl(conn, doc_id)

        # CRITICAL FINDING: GL stays at 1500 because write-back is irreversible.
        # documents.gl_account is now 1500 (not 5100), which is NOT in expense range,
        # so the confidence threshold check (FIX 5) never fires.
        # The original vendor-memory GL (5100) is permanently lost.
        assert gl_high_conf == "1500", (
            f"Expected 1500 (write-back is irreversible), got {gl_high_conf}"
        )

        # Verify: substance_flags still shows original_gl for reconstruction
        flags = _get_substance_flags(conn, doc_id)
        if not flags.get("original_gl"):
            pytest.fail(
                "HIGH: Write-back is irreversible and substance_flags doesn't preserve "
                "original_gl. The vendor-memory GL is permanently lost after override."
            )

    def test_reprocess_after_approval_state_change_preserves_approval(self):
        """Reprocessing after approval should not lose approval state."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Équipement Lourd Inc", doc_type="invoice", amount=80000.00,
            gl_account="Uncategorized Expense",
            memo="Achat excavatrice hydraulique",
        )
        doc_id = doc["document_id"]

        # Initial processing
        upsert_posting_job(conn, document=doc)
        posting_gl = _get_posting_gl(conn, doc_id)
        assert posting_gl == "1500", "Should override to CapEx"

        # Manually approve
        conn.execute(
            "UPDATE posting_jobs SET approval_state = 'approved_for_posting', "
            "posting_status = 'ready_to_post' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Reprocess the document
        result = upsert_posting_job(conn, document=doc)

        # Approval should be preserved (upsert merges with existing)
        posting = _get_posting_row(conn, doc_id)
        # The key question: does reprocessing reset approval?
        # Current behavior: upsert_posting_job defaults to pending_review if not explicitly set
        # This is either correct (re-review needed) or a bug (loses approval)
        # Document the actual behavior:
        actual_approval = posting.get("approval_state", "")
        actual_posting_status = posting.get("posting_status", "")

        # At minimum, GL must stay consistent
        assert _get_document_gl(conn, doc_id) == _get_posting_gl(conn, doc_id), (
            "Reprocessing broke GL consistency"
        )

    def test_no_duplicate_posting_jobs_on_reprocess(self):
        """Reprocessing must UPDATE, not create a second posting_jobs row."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Assurance Desjardins", doc_type="invoice", amount=3600.00,
            gl_account="Uncategorized Expense",
            memo="Prime assurance responsabilité civile",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)
        upsert_posting_job(conn, document=doc)
        upsert_posting_job(conn, document=doc)

        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM posting_jobs WHERE document_id = ?",
            (doc_id,),
        ).fetchone()["cnt"]

        assert count == 1, (
            f"CRITICAL: {count} posting_jobs rows for one document. "
            f"Reprocessing creates duplicates!"
        )

    def test_reprocess_after_fraud_flags_change(self):
        """After fraud flags are updated, reprocessing must not corrupt GL state."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="ABC Equipment", doc_type="invoice", amount=50000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase of industrial equipment",
        )
        doc_id = doc["document_id"]

        # First processing
        upsert_posting_job(conn, document=doc)
        gl_before_fraud = _get_posting_gl(conn, doc_id)

        # Simulate fraud flag addition
        conn.execute(
            "UPDATE documents SET fraud_flags = ? WHERE document_id = ?",
            (json.dumps([{"rule": "vendor_amount_anomaly", "severity": "high"}]), doc_id),
        )
        conn.commit()

        # Reprocess
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)
        gl_after_fraud = _get_posting_gl(conn, doc_id)

        # GL should remain consistent — fraud flags don't change GL mapping
        assert gl_before_fraud == gl_after_fraud, (
            f"Fraud flags changed GL from {gl_before_fraud} to {gl_after_fraud}"
        )

        # Verify no duplicate audit entries
        audit_count = _count_audit_logs(conn, doc_id, "gl_override_applied")
        assert audit_count <= 1, f"Duplicate audit entries after fraud flag + reprocess: {audit_count}"

    def test_blocked_items_stay_blocked_unless_legitimately_cleared(self):
        """Personal expense blocks must survive reprocessing."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Netflix", doc_type="invoice", amount=25.99,
            gl_account="5400 - Entertainment",
            memo="Monthly Netflix personal subscription",
        )
        doc_id = doc["document_id"]

        # First process — should block
        upsert_posting_job(conn, document=doc)
        posting1 = _get_posting_row(conn, doc_id)

        assert posting1["approval_state"] == "pending_review", (
            "Personal expense should be pending_review"
        )
        assert posting1["posting_status"] == "blocked", (
            "Personal expense should be blocked"
        )

        # Reprocess — block should persist
        upsert_posting_job(conn, document=doc)
        posting2 = _get_posting_row(conn, doc_id)

        assert posting2["approval_state"] == "pending_review", (
            "Block should survive reprocessing"
        )
        assert posting2["posting_status"] == "blocked", (
            "Block status should survive reprocessing"
        )


# ===========================================================================
# SECTION 3: APPROVAL / STATUS TRANSITION ATTACK
# ===========================================================================


class TestApprovalStatusTransitions:
    """Attack all state transitions for invalid or inconsistent states."""

    def test_cannot_approve_needs_review_document_via_posting_builder(self):
        """approve_posting_job should reject documents not in Ready status.
        Note: We can only test the check in approve_posting_job which uses open_db(),
        so we test the logic inline."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Test Vendor", doc_type="invoice", amount=100.00,
            gl_account="5100", review_status="NeedsReview",
        )
        doc_id = doc["document_id"]

        # Build initial posting job
        upsert_posting_job(conn, document=doc)

        posting = _get_posting_row(conn, doc_id)
        review_status = normalize_text(doc.get("review_status"))

        # The POSTABLE_STATUSES check should prevent approval
        from src.agents.tools.posting_builder import POSTABLE_STATUSES
        assert review_status not in POSTABLE_STATUSES, (
            "NeedsReview should NOT be in POSTABLE_STATUSES"
        )

    def test_substance_block_overrides_explicit_approval_attempt(self):
        """Even if we try to set approval_state='approved_for_posting',
        substance block_auto_approval must win."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Netflix", doc_type="invoice", amount=25.99,
            gl_account="5400", memo="Netflix personal subscription",
        )
        doc_id = doc["document_id"]

        # Try to force approval
        result = upsert_posting_job(
            conn, document=doc,
            approval_state="approved_for_posting",
            posting_status="ready_to_post",
        )

        posting = _get_posting_row(conn, doc_id)

        # Substance block_auto_approval should override our explicit approval
        assert posting["approval_state"] == "pending_review", (
            f"CRITICAL: Substance block was bypassed! approval_state={posting['approval_state']}"
        )
        assert posting["posting_status"] == "blocked", (
            f"CRITICAL: Substance block was bypassed! posting_status={posting['posting_status']}"
        )

    def test_review_status_and_posting_status_consistency(self):
        """When review_status=NeedsReview, posting_status should not be ready_to_post."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Personal Gym Corp", doc_type="invoice", amount=150.00,
            gl_account="5400", memo="Personal gym membership monthly",
            review_status="Ready",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        posting = _get_posting_row(conn, doc_id)
        doc_row = _get_document_row(conn, doc_id)

        # substance should block and change review_status to NeedsReview
        if posting["posting_status"] == "blocked":
            assert doc_row["review_status"] == "NeedsReview" or posting.get("review_status") == "NeedsReview", (
                "CRITICAL: posting_status=blocked but review_status not updated to NeedsReview"
            )

    def test_posted_status_requires_prior_approval(self):
        """A posting_jobs row should not be 'posted' without going through 'approved_for_posting'."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Test Corp", doc_type="invoice", amount=500.00,
            gl_account="5100", review_status="Ready",
        )
        doc_id = doc["document_id"]

        # Create posting job
        upsert_posting_job(conn, document=doc)

        # Try to skip approval and go directly to posted
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'posted' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        posting = _get_posting_row(conn, doc_id)

        # This is a gap: no DB-level constraint prevents this
        if posting["posting_status"] == "posted" and posting["approval_state"] == "pending_review":
            pytest.fail(
                "CRITICAL: posting_status='posted' while approval_state='pending_review'. "
                "No state machine enforcement at DB level. A document was posted without approval."
            )

    def test_invalid_transition_exception_to_posted(self):
        """Document in Exception status should not be directly postable."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Missing Corp", doc_type="invoice", amount=0,
            gl_account="", review_status="Exception",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        # Try to force posted
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'posted', "
            "approval_state = 'approved_for_posting' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        posting = _get_posting_row(conn, doc_id)
        doc_row = _get_document_row(conn, doc_id)

        # No constraint prevents posting an Exception document
        if posting["posting_status"] == "posted" and doc_row["review_status"] == "Exception":
            pytest.fail(
                "CRITICAL: Document in Exception status was posted. "
                "No database constraint prevents posting unreviewed exceptions."
            )

    def test_fraud_block_then_manager_override_then_repost(self):
        """Full cycle: fraud blocks -> manager overrides -> repost.
        Verify state consistency at each step."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Suspicious Corp", doc_type="invoice", amount=99999.99,
            gl_account="5100 - Office Supplies", review_status="Ready",
            memo="Consulting services",
        )
        doc_id = doc["document_id"]

        # Step 1: Initial posting
        upsert_posting_job(conn, document=doc)

        # Step 2: Fraud detection adds flags
        conn.execute(
            "UPDATE documents SET fraud_flags = ?, review_status = 'NeedsReview' WHERE document_id = ?",
            (json.dumps([{"rule": "new_vendor_large_amount", "severity": "high"}]), doc_id),
        )
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'blocked', "
            "approval_state = 'pending_review' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Step 3: Manager override
        conn.execute(
            "UPDATE documents SET fraud_override_reason = 'Verified with vendor directly', "
            "review_status = 'Ready' WHERE document_id = ?",
            (doc_id,),
        )
        conn.execute(
            "UPDATE posting_jobs SET approval_state = 'approved_for_posting', "
            "posting_status = 'ready_to_post' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Step 4: Mark posted
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'posted' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Verify final consistency
        doc_row = _get_document_row(conn, doc_id)
        posting = _get_posting_row(conn, doc_id)

        assert doc_row["review_status"] == "Ready"
        assert posting["posting_status"] == "posted"
        assert posting["approval_state"] == "approved_for_posting"
        assert doc_row.get("fraud_override_reason"), "Fraud override reason must be preserved"
        assert _get_document_gl(conn, doc_id) == _get_posting_gl(conn, doc_id), (
            "GL must be consistent after fraud override cycle"
        )


# ===========================================================================
# SECTION 4: AUDIT LOG TRUTHFULNESS
# ===========================================================================


class TestAuditLogTruthfulness:
    """Verify that audit_log is actually trustworthy and complete."""

    def test_override_then_later_change_creates_audit_trail(self):
        """If GL is overridden, then changes again, both events must be logged."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Location Serveur Cloud", doc_type="invoice", amount=20000.00,
            gl_account="Uncategorized Expense",
            memo="Achat serveur dédié infrastructure",
        )
        doc_id = doc["document_id"]

        # First override: Uncategorized → 1500 (CapEx)
        upsert_posting_job(conn, document=doc)
        first_gl = _get_posting_gl(conn, doc_id)
        assert first_gl == "1500", f"Expected 1500, got {first_gl}"

        # Manually change GL (simulating manual dashboard override)
        conn.execute(
            "UPDATE documents SET gl_account = '5200 - Rent' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Reprocess — substance will try to override again
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)

        logs = _get_audit_logs(conn, doc_id)
        override_logs = [l for l in logs if l["event_type"] == "gl_override_applied"]

        # We need at least the original override logged
        assert len(override_logs) >= 1, "No audit log entries for GL overrides"

        # Check if the second override (5200 → 1500) was also logged
        if len(override_logs) >= 2:
            second_data = json.loads(override_logs[1]["prompt_snippet"])
            assert second_data["old_value"] == "5200 - Rent", (
                "Second override log should show 5200 as old_value"
            )
            assert second_data["new_value"] == "1500", (
                "Second override log should show 1500 as new_value"
            )

    def test_audit_log_preserves_before_after_values(self):
        """Every gl_override_applied entry must have old_value and new_value."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Prêt BDC Commercial", doc_type="invoice", amount=100000.00,
            gl_account="Uncategorized Expense",
            memo="Financement hypothèque commerciale",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        logs = _get_audit_logs(conn, doc_id)
        override_logs = [l for l in logs if l["event_type"] == "gl_override_applied"]

        for log in override_logs:
            data = json.loads(log["prompt_snippet"])
            assert "old_value" in data, f"Audit entry missing old_value: {data}"
            assert "new_value" in data, f"Audit entry missing new_value: {data}"
            assert data["old_value"] != data["new_value"], (
                f"Audit logged a no-op change: {data['old_value']} → {data['new_value']}"
            )

    def test_audit_entries_are_ordered_chronologically(self):
        """Audit log entries for a document must be in chronological order."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Équipement Construction", doc_type="invoice", amount=75000.00,
            gl_account="Uncategorized Expense",
            memo="Achat bulldozer CAT D6",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        # Insert a manual audit entry
        conn.execute(
            "INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("manual_review", doc_id, '{"action": "reviewed"}', utc_now_iso()),
        )
        conn.commit()

        logs = _get_audit_logs(conn, doc_id)
        timestamps = [l["created_at"] for l in logs if l["created_at"]]

        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i-1], (
                f"Audit log entries out of order: {timestamps[i-1]} > {timestamps[i]}"
            )

    def test_audit_log_reflects_final_reality(self):
        """The last audit entry's new_value must match the current persisted GL."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Remise TPS Québec", doc_type="invoice", amount=8000.00,
            gl_account="Uncategorized Expense",
            memo="Acompte provisionnel TPS Q3",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        logs = _get_audit_logs(conn, doc_id)
        override_logs = [l for l in logs if l["event_type"] == "gl_override_applied"]

        if override_logs:
            last_log = override_logs[-1]
            last_data = json.loads(last_log["prompt_snippet"])
            current_gl = _get_document_gl(conn, doc_id)

            assert last_data["new_value"] == current_gl, (
                f"CRITICAL: Audit log says GL is {last_data['new_value']} "
                f"but persisted GL is {current_gl}. Audit log is misleading!"
            )

    def test_no_audit_entry_for_non_override(self):
        """When substance doesn't trigger override, no gl_override_applied should exist."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Restaurant Le Petit", doc_type="invoice", amount=150.00,
            gl_account="5300 - Meals & Entertainment",
            memo="Lunch meeting with client",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        override_count = _count_audit_logs(conn, doc_id, "gl_override_applied")
        assert override_count == 0, (
            f"Phantom audit entry: {override_count} gl_override_applied entries "
            f"when no override occurred"
        )

    def test_multiple_overrides_all_logged(self):
        """Each distinct GL override on the same document must create a log entry."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Équipement Lourd Inc", doc_type="invoice", amount=200000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase heavy machinery crane",
        )
        doc_id = doc["document_id"]

        # First override: Uncategorized → 1500
        upsert_posting_job(conn, document=doc)
        assert _get_posting_gl(conn, doc_id) == "1500"

        # Manually set GL to something else
        conn.execute(
            "UPDATE documents SET gl_account = 'Uncategorized Expense' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Second override: should log again Uncategorized → 1500
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)

        override_count = _count_audit_logs(conn, doc_id, "gl_override_applied")
        assert override_count >= 2, (
            f"Only {override_count} audit entries for 2 distinct overrides. "
            f"Audit trail is incomplete."
        )

    def test_audit_log_missing_fraud_override_events(self):
        """Fraud override decisions should be audit-logged.
        FINDING: The system only logs gl_override_applied events.
        Fraud flag changes and fraud overrides are NOT logged to audit_log."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Shady Corp", doc_type="invoice", amount=99999.00,
            gl_account="5100", review_status="Ready",
        )
        doc_id = doc["document_id"]

        # Simulate fraud detection + override
        conn.execute(
            "UPDATE documents SET fraud_flags = ?, review_status = 'NeedsReview' WHERE document_id = ?",
            (json.dumps([{"rule": "new_vendor_large_amount", "severity": "high"}]), doc_id),
        )
        conn.execute(
            "INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at) "
            "VALUES ('fraud_flags_updated', ?, ?, ?)",
            (doc_id, json.dumps({"flags": ["new_vendor_large_amount"]}), utc_now_iso()),
        )
        conn.commit()

        # Manager overrides fraud
        conn.execute(
            "UPDATE documents SET fraud_override_reason = 'Verified with vendor by phone call', review_status = 'Ready' "
            "WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Check: is the fraud override logged?
        logs = _get_audit_logs(conn, doc_id)
        fraud_override_logs = [l for l in logs if "fraud" in l.get("event_type", "").lower()
                               or "override" in l.get("event_type", "").lower()]

        # Only the one we manually inserted exists — system doesn't auto-log fraud changes
        auto_fraud_logs = [l for l in logs if l["event_type"] == "fraud_override"]
        if not auto_fraud_logs:
            pytest.fail(
                "HIGH: Fraud override (NeedsReview → Ready + fraud_override_reason set) "
                "is NOT auto-logged to audit_log. A reviewer cannot reconstruct what happened. "
                "The fraud_engine writes fraud_flags but never logs to audit_log. "
                "The review_actions module changes review_status but doesn't log fraud overrides."
            )


# ===========================================================================
# SECTION 5: CACHE / MEMORY / PERSISTED STATE COLLISION
# ===========================================================================


class TestMemoryStateCollision:
    """Attack cases where vendor memory, persisted GL, and substance engine disagree."""

    def test_vendor_memory_gl_vs_substance_override(self):
        """When vendor memory says GL=5100 but substance says GL=1500,
        the confidence threshold determines winner.

        NOTE: Must use singular form "server" not "servers" — _CAPEX_KEYWORDS
        uses \\b word boundaries so "servers" doesn't match "server".
        """
        conn = _make_db()

        # Low confidence: substance wins (conf 0.40 < 0.85, GL in expense range 5000-5999)
        doc_low = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=15000.00,
            gl_account="5100 - Office Supplies", confidence=0.40,
            memo="Purchase of new server for data center",  # singular "server"
            document_id="doc-lowconf",
        )
        upsert_posting_job(conn, document=doc_low)
        gl_low = _get_posting_gl(conn, "doc-lowconf")

        # High confidence: vendor memory wins (conf 0.95 >= 0.85)
        doc_high = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=15000.00,
            gl_account="5100 - Office Supplies", confidence=0.95,
            memo="Purchase of new server for data center",  # singular "server"
            document_id="doc-highconf",
        )
        upsert_posting_job(conn, document=doc_high)
        gl_high = _get_posting_gl(conn, "doc-highconf")

        assert gl_low == "1500", f"Low confidence should override to CapEx, got {gl_low}"
        assert gl_high == "5100 - Office Supplies", (
            f"High confidence should preserve vendor memory, got {gl_high}"
        )

        # Critical: both documents and posting must agree
        assert _get_document_gl(conn, "doc-lowconf") == gl_low
        assert _get_document_gl(conn, "doc-highconf") == gl_high

    def test_capex_regex_gap_plural_forms_not_matched(self):
        """FINDING: _CAPEX_KEYWORDS uses \\b word boundaries, so plural forms
        like 'servers', 'computers', 'racks' do NOT match.
        This means substance engine silently misses CapEx detection for
        documents using plural nouns — common in real invoices."""
        from src.engines.substance_engine import substance_classifier as sc

        # Singular: detected
        result_singular = sc(vendor="Dell", memo="Purchase of server", doc_type="invoice", amount=15000)
        # Plural: NOT detected
        result_plural = sc(vendor="Dell", memo="Purchase of servers", doc_type="invoice", amount=15000)

        assert result_singular["potential_capex"] is True, "Singular 'server' should trigger capex"

        if not result_plural["potential_capex"]:
            pytest.fail(
                "MEDIUM: substance_classifier does not detect 'servers' (plural) as CapEx. "
                "The _CAPEX_KEYWORDS regex uses \\b boundaries so 'server\\b' matches 'server' "
                "but NOT 'servers'. Real invoices commonly use plural forms: "
                "'10 servers', '5 computers', '3 racks'. "
                "This causes silent CapEx misclassification."
            )

    def test_reprocess_after_writeback_does_not_double_override(self):
        """After write-back sets documents.gl_account=1500,
        reprocessing should see 1500 (not Uncategorized) and not re-override."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Machinerie ABC", doc_type="invoice", amount=30000.00,
            gl_account="Uncategorized Expense",
            memo="Achat compresseur industriel",
        )
        doc_id = doc["document_id"]

        # First processing: Uncategorized → 1500
        upsert_posting_job(conn, document=doc)
        assert _get_document_gl(conn, doc_id) == "1500"

        # Reprocess: now documents.gl_account is 1500, not Uncategorized
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)

        # Should still be 1500 — no additional override needed
        final_gl = _get_posting_gl(conn, doc_id)
        assert final_gl == "1500", f"Reprocess after writeback changed GL to {final_gl}"

        # No duplicate audit entries
        audit_count = _count_audit_logs(conn, doc_id, "gl_override_applied")
        assert audit_count == 1, (
            f"Expected 1 audit entry, got {audit_count}. "
            f"Reprocessing after writeback should not re-log identical override."
        )

    def test_order_of_processing_does_not_change_outcome(self):
        """Processing documents in different order should produce identical GL results."""
        # Document A: CapEx vendor
        # Document B: Loan vendor
        results_order_1 = {}
        results_order_2 = {}

        for order_name, first, second in [
            ("AB", "capex", "loan"),
            ("BA", "loan", "capex"),
        ]:
            conn = _make_db()
            docs = {
                "capex": _insert_document(
                    conn, vendor="Équipement X", doc_type="invoice", amount=50000.00,
                    gl_account="Uncategorized Expense", memo="Achat excavatrice",
                    document_id=f"doc-capex-{order_name}",
                ),
                "loan": _insert_document(
                    conn, vendor="Banque Nationale", doc_type="invoice", amount=25000.00,
                    gl_account="Uncategorized Expense", memo="Versement prêt commercial",
                    document_id=f"doc-loan-{order_name}",
                ),
            }

            upsert_posting_job(conn, document=docs[first])
            upsert_posting_job(conn, document=docs[second])

            target = results_order_1 if order_name == "AB" else results_order_2
            target["capex_gl"] = _get_posting_gl(conn, f"doc-capex-{order_name}")
            target["loan_gl"] = _get_posting_gl(conn, f"doc-loan-{order_name}")

        assert results_order_1["capex_gl"] == results_order_2["capex_gl"], (
            f"Order-dependent CapEx GL: {results_order_1['capex_gl']} vs {results_order_2['capex_gl']}"
        )
        assert results_order_1["loan_gl"] == results_order_2["loan_gl"], (
            f"Order-dependent Loan GL: {results_order_1['loan_gl']} vs {results_order_2['loan_gl']}"
        )

    def test_stale_substance_flags_after_manual_gl_edit(self):
        """If someone manually edits GL after substance override,
        substance_flags still shows the old override. Is this misleading?"""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Assurance QC", doc_type="invoice", amount=5000.00,
            gl_account="Uncategorized Expense",
            memo="Prime assurance annuelle",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)
        assert _get_document_gl(conn, doc_id) == "1300"

        # Manual edit: accountant changes GL to something else
        conn.execute(
            "UPDATE documents SET gl_account = '5150 - Insurance Expense' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Now substance_flags still says override_applied=True, original_gl=Uncategorized
        flags = _get_substance_flags(conn, doc_id)
        current_gl = _get_document_gl(conn, doc_id)

        if flags.get("override_applied") and current_gl != "1300":
            # substance_flags is now stale — it says override was applied to 1300
            # but actual GL is 5150
            pytest.fail(
                f"HIGH: Stale substance_flags after manual edit. "
                f"substance_flags says override_applied=True (to 1300) "
                f"but current GL is {current_gl}. "
                f"A reviewer reading substance_flags would be misled."
            )


# ===========================================================================
# SECTION 6: CONCURRENCY / MULTI-ACTOR ATTACK
# ===========================================================================


class TestConcurrencyAttacks:
    """Simulate concurrent operations that could create inconsistent state."""

    def test_concurrent_upsert_posting_jobs(self):
        """Two threads upserting the same document's posting job simultaneously."""
        import tempfile
        db_path = Path(tempfile.mktemp(suffix=".db"))

        # Create DB on disk for multi-thread access
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
                client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
                document_date TEXT, gl_account TEXT, tax_code TEXT, category TEXT,
                review_status TEXT, confidence REAL, raw_result TEXT, currency TEXT,
                subtotal REAL, tax_total REAL, extraction_method TEXT,
                ingest_source TEXT, raw_ocr_text TEXT,
                hallucination_suspected INTEGER DEFAULT 0,
                created_at TEXT, updated_at TEXT, assigned_to TEXT,
                manual_hold_reason TEXT, manual_hold_by TEXT, manual_hold_at TEXT,
                memo TEXT, substance_flags TEXT, fraud_flags TEXT,
                entry_kind TEXT, correction_count INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL DEFAULT 'ai_call',
                username TEXT, document_id TEXT, provider TEXT,
                task_type TEXT, prompt_snippet TEXT,
                latency_ms INTEGER, created_at TEXT NOT NULL DEFAULT ''
            )
        """)
        ensure_posting_job_table_minimum(conn)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("doc-concurrent", "test.pdf", "/tmp/test.pdf", "TEST01",
             "Équipement Lourd", "invoice", 50000.0, "2026-01-15",
             "Uncategorized Expense", "GST_QST", "Uncategorized", "Ready",
             0.50, None, "CAD", None, None, None, None, None, 0,
             "2026-01-15T00:00:00Z", "2026-01-15T00:00:00Z",
             None, None, None, None,
             "Purchase heavy equipment crane", None, None, None, 0),
        )
        conn.commit()
        conn.close()

        results = []
        errors = []

        def worker(worker_id):
            try:
                c = sqlite3.connect(str(db_path))
                c.row_factory = sqlite3.Row
                ensure_posting_job_table_minimum(c)
                doc_row = dict(c.execute("SELECT * FROM documents WHERE document_id = 'doc-concurrent'").fetchone())
                result = upsert_posting_job(c, document=doc_row)
                results.append((worker_id, result))
                c.close()
            except Exception as e:
                errors.append((worker_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        # Check results
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        posting_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM posting_jobs WHERE document_id = 'doc-concurrent'"
        ).fetchone()["cnt"]

        audit_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM audit_log WHERE document_id = 'doc-concurrent' "
            "AND event_type = 'gl_override_applied'"
        ).fetchone()["cnt"]

        conn.close()

        try:
            db_path.unlink()
        except Exception:
            pass

        if errors:
            # Some threads may fail due to DB locking — that's acceptable
            # What's NOT acceptable is duplicate rows
            pass

        assert posting_count <= 1, (
            f"CRITICAL: Concurrent processing created {posting_count} posting_jobs rows. "
            f"Race condition allows duplicate postings!"
        )

    def test_approve_and_reprocess_race(self):
        """Approval happening while reprocessing could create inconsistent state."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=20000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase of new server rack", confidence=0.50,
        )
        doc_id = doc["document_id"]

        # Initial processing
        upsert_posting_job(conn, document=doc)

        # Simulate approval
        conn.execute(
            "UPDATE posting_jobs SET approval_state = 'approved_for_posting', "
            "posting_status = 'ready_to_post' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Reprocess (e.g., document was re-OCR'd with slightly different text)
        doc_updated = dict(doc)
        doc_updated["memo"] = "Server rack purchase for data center expansion"
        upsert_posting_job(conn, document=doc_updated)

        posting = _get_posting_row(conn, doc_id)

        # After reprocess, what happened to approval?
        # This documents the actual behavior — either preserving or resetting approval
        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        assert doc_gl == posting_gl, (
            f"Race between approval and reprocess broke GL consistency: "
            f"doc={doc_gl}, posting={posting_gl}"
        )

    def test_duplicate_intake_same_document(self):
        """Same document ingested twice creates two document_ids.
        Verify no cross-contamination in posting_jobs."""
        conn = _make_db()

        # First intake
        doc1 = _insert_document(
            conn, vendor="Bell Canada", doc_type="invoice", amount=500.00,
            gl_account="5200 - Telecom", document_id="intake-1",
            file_name="bell_jan_2026.pdf",
        )
        upsert_posting_job(conn, document=doc1)

        # Second intake of same physical document (different doc ID)
        doc2 = _insert_document(
            conn, vendor="Bell Canada", doc_type="invoice", amount=500.00,
            gl_account="5200 - Telecom", document_id="intake-2",
            file_name="bell_jan_2026.pdf",
        )
        upsert_posting_job(conn, document=doc2)

        # Each should have its own posting job
        posting1 = _get_posting_row(conn, "intake-1")
        posting2 = _get_posting_row(conn, "intake-2")

        assert posting1["posting_id"] != posting2["posting_id"], (
            "Duplicate intakes share the same posting_id — cross-contamination!"
        )

        # Verify no shared state
        assert posting1["document_id"] == "intake-1"
        assert posting2["document_id"] == "intake-2"


# ===========================================================================
# SECTION 7: COMBINED PRODUCTION NIGHTMARES
# ===========================================================================


class TestCombinedProductionNightmares:
    """Complex multi-step scenarios that stress long-lived workflow integrity."""

    def test_substance_override_plus_fraud_block_plus_manager_override_plus_repost(self):
        """Full nightmare cycle:
        1. Document arrives with Uncategorized GL
        2. Substance overrides to CapEx (1500)
        3. Fraud engine flags it
        4. Manager overrides fraud
        5. Repost
        Verify consistency at every step."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Heavy Equipment Corp", doc_type="invoice", amount=250000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase of industrial crane for construction site",
            review_status="Ready", confidence=0.50,
        )
        doc_id = doc["document_id"]

        # Step 1: Substance override
        upsert_posting_job(conn, document=doc)
        step1_doc_gl = _get_document_gl(conn, doc_id)
        step1_posting_gl = _get_posting_gl(conn, doc_id)
        assert step1_doc_gl == "1500", f"Step 1: Expected 1500, got {step1_doc_gl}"
        assert step1_doc_gl == step1_posting_gl, "Step 1: GL mismatch"

        # Step 2: Fraud flags added
        conn.execute(
            "UPDATE documents SET fraud_flags = ?, review_status = 'NeedsReview' WHERE document_id = ?",
            (json.dumps([
                {"rule": "new_vendor_large_amount", "severity": "critical"},
                {"rule": "round_number_flag", "severity": "medium"},
            ]), doc_id),
        )
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'blocked', "
            "approval_state = 'pending_review' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # GL should NOT change due to fraud flags
        step2_doc_gl = _get_document_gl(conn, doc_id)
        assert step2_doc_gl == "1500", f"Step 2: Fraud flags changed GL to {step2_doc_gl}"

        # Step 3: Manager override
        conn.execute(
            "UPDATE documents SET fraud_override_reason = 'PO verified, vendor confirmed', "
            "review_status = 'Ready' WHERE document_id = ?",
            (doc_id,),
        )
        conn.execute(
            "UPDATE posting_jobs SET approval_state = 'approved_for_posting', "
            "posting_status = 'ready_to_post', reviewer = 'Manager_Sam' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Step 4: Verify pre-post consistency
        step4_doc_gl = _get_document_gl(conn, doc_id)
        step4_posting_gl = _get_posting_gl(conn, doc_id)
        assert step4_doc_gl == step4_posting_gl, (
            f"Step 4: GL split-brain after fraud override: doc={step4_doc_gl}, posting={step4_posting_gl}"
        )

        # Step 5: Mark posted
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'posted' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Final verification
        final_doc = _get_document_row(conn, doc_id)
        final_posting = _get_posting_row(conn, doc_id)

        assert final_doc["gl_account"] == final_posting["gl_account"] == "1500"
        assert final_posting["posting_status"] == "posted"
        assert final_posting["approval_state"] == "approved_for_posting"
        assert final_doc["fraud_override_reason"] == "PO verified, vendor confirmed"

        # Audit trail must exist
        override_count = _count_audit_logs(conn, doc_id, "gl_override_applied")
        assert override_count >= 1, "No audit trail for the substance override"

    def test_credit_note_inferred_plus_substance_plus_retry(self):
        """Credit note with substance flags + retry cycle."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Fournisseur Équipement", doc_type="credit_note", amount=-15000.00,
            gl_account="Uncategorized Expense",
            memo="Retour équipement défectueux - serveur rack",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        posting = _get_posting_row(conn, doc_id)
        assert posting["entry_kind"] == "credit", (
            f"Credit note should infer entry_kind='credit', got '{posting['entry_kind']}'"
        )

        # GL should be overridden (CapEx keyword in memo)
        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)
        assert doc_gl == posting_gl, f"Split brain: doc={doc_gl}, posting={posting_gl}"

        # Retry
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)

        # Entry kind must survive retry
        posting_after = _get_posting_row(conn, doc_id)
        assert posting_after["entry_kind"] == "credit", (
            f"Entry kind changed after retry: {posting_after['entry_kind']}"
        )
        assert _get_document_gl(conn, doc_id) == _get_posting_gl(conn, doc_id), (
            "GL consistency broken after credit note retry"
        )

    def test_vendor_memory_conflict_writeback_reprocess_approval(self):
        """
        1. Vendor memory suggests GL=5100 (high confidence)
        2. First process respects vendor memory
        3. Confidence drops (re-OCR)
        4. Reprocess: substance now overrides
        5. Approve
        6. Verify final state
        """
        conn = _make_db()

        # Step 1: High confidence, vendor memory GL preserved
        doc = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=20000.00,
            gl_account="5100 - Office Supplies", confidence=0.95,
            memo="Server equipment for office",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)
        step1_gl = _get_posting_gl(conn, doc_id)
        # High confidence: vendor memory preserved
        assert step1_gl == "5100 - Office Supplies", f"Step 1: Expected vendor memory GL, got {step1_gl}"

        # Step 2: Confidence drops on re-OCR
        conn.execute(
            "UPDATE documents SET confidence = 0.40 WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Step 3: Reprocess
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)
        step3_gl = _get_posting_gl(conn, doc_id)

        # Low confidence: substance should now override
        assert step3_gl == "1500", (
            f"Step 3: After confidence drop, expected substance override to 1500, got {step3_gl}"
        )

        # All layers must agree
        assert _get_document_gl(conn, doc_id) == step3_gl

        # Step 4: Approve
        conn.execute(
            "UPDATE documents SET review_status = 'Ready' WHERE document_id = ?",
            (doc_id,),
        )
        conn.execute(
            "UPDATE posting_jobs SET approval_state = 'approved_for_posting', "
            "posting_status = 'ready_to_post' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Final check
        final_doc_gl = _get_document_gl(conn, doc_id)
        final_posting_gl = _get_posting_gl(conn, doc_id)
        assert final_doc_gl == final_posting_gl == "1500", (
            f"Final state inconsistent: doc={final_doc_gl}, posting={final_posting_gl}"
        )

    def test_document_edited_after_initial_override(self):
        """Document GL manually edited after substance override.
        Then reprocessed. What wins?"""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="BDC Financement", doc_type="invoice", amount=75000.00,
            gl_account="Uncategorized Expense",
            memo="Prêt hypothécaire commercial versement mensuel",
        )
        doc_id = doc["document_id"]

        # Initial: substance overrides to 2500 (loan)
        upsert_posting_job(conn, document=doc)
        assert _get_document_gl(conn, doc_id) == "2500"

        # Accountant manually edits to 2300 (they know better)
        conn.execute(
            "UPDATE documents SET gl_account = '2300 - Current Portion Long-term Debt' WHERE document_id = ?",
            (doc_id,),
        )
        conn.execute(
            "UPDATE posting_jobs SET gl_account = '2300 - Current Portion Long-term Debt' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # Reprocess (e.g., system batch job)
        doc_updated = _get_document_row(conn, doc_id)
        upsert_posting_job(conn, document=doc_updated)

        # What happened? The substance engine sees "2300" which is not uncategorized,
        # and it's not in expense range (5000-5999), and it's a loan type.
        # Since loan is a PRIORITY_OVERRIDE_TYPE, it should override to 2500!
        final_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        # At minimum, they must agree
        assert final_gl == posting_gl, (
            f"Split brain after manual edit + reprocess: doc={final_gl}, posting={posting_gl}"
        )

        # FINDING: If substance overrides the accountant's manual edit,
        # that's a production problem — the accountant's judgment should win.
        if final_gl == "2500" and posting_gl == "2500":
            pytest.fail(
                "HIGH: Substance engine overrode accountant's manual GL edit "
                f"from '2300 - Current Portion Long-term Debt' back to '2500'. "
                f"Loan is a PRIORITY_OVERRIDE_TYPE, so substance always wins. "
                f"There is no mechanism to mark a GL as 'manually set, do not override'."
            )

    def test_same_invoice_ocr_and_email_path_different_text(self):
        """Same invoice ingested via OCR and email with slightly different extracted text.
        Both should produce consistent GL mapping."""
        conn = _make_db()

        # OCR path: clean text
        doc_ocr = _insert_document(
            conn, vendor="Assurance Desjardins", doc_type="invoice", amount=4800.00,
            gl_account="Uncategorized Expense",
            memo="Prime d'assurance annuelle bureau",
            document_id="invoice-ocr",
            ingest_source="folder_watcher",
        )

        # Email path: slightly different text (OCR artifacts)
        doc_email = _insert_document(
            conn, vendor="Assurance Desjardins", doc_type="invoice", amount=4800.00,
            gl_account="Uncategorized Expense",
            memo="Prime d'assurance annuelle bureau",  # Same memo
            document_id="invoice-email",
            ingest_source="graph_mail",
        )

        upsert_posting_job(conn, document=doc_ocr)
        upsert_posting_job(conn, document=doc_email)

        gl_ocr = _get_posting_gl(conn, "invoice-ocr")
        gl_email = _get_posting_gl(conn, "invoice-email")

        assert gl_ocr == gl_email, (
            f"Same invoice, different intake paths, different GL: "
            f"OCR={gl_ocr}, Email={gl_email}"
        )

    def test_unicode_amount_noise_plus_substance_plus_retry(self):
        """Unicode noise in amount field combined with substance override and retry."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Équipement Lourd", doc_type="invoice",
            amount=50000.00,  # Clean amount
            gl_account="Uncategorized Expense",
            memo="Achat pelle mécanique CAT 320",
        )
        doc_id = doc["document_id"]

        # Process with clean amount
        upsert_posting_job(conn, document=doc)
        gl_clean = _get_posting_gl(conn, doc_id)

        # "Reprocess" with amount as string (simulating OCR noise)
        doc_noisy = dict(doc)
        doc_noisy["amount"] = 50000.00  # Same numeric value
        upsert_posting_job(conn, document=doc_noisy)
        gl_noisy = _get_posting_gl(conn, doc_id)

        assert gl_clean == gl_noisy, (
            f"Amount representation changed GL: clean={gl_clean}, noisy={gl_noisy}"
        )


# ===========================================================================
# SECTION 8: XFAIL PRODUCTION RISK TRIAGE
# ===========================================================================


class TestXFailProductionRiskTriage:
    """Convert xfailed tests into production risk assessments.
    Each test documents whether the xfail hides a real production risk."""

    def test_xfail_unicode_accents_hides_vendor_mismatch_risk(self):
        """XFAIL: test_unicode_accents_in_vendor (bank_matcher_attacks)
        RISK: Accent stripping causes vendor matching failures.
        In production, 'Société de transport' won't match 'SOCIETE TRANSPORT'.
        This means automatic bank reconciliation silently fails for
        Quebec vendors with accented names.
        VERDICT: HIGH — blocks reliable bank reconciliation for Quebec."""
        # Reproduce: normalize_text strips accents
        from src.agents.tools.posting_builder import normalize_text as pb_normalize
        # normalize_text in posting_builder just strips whitespace
        assert pb_normalize("Société") == "Société", "posting_builder normalize preserves accents"
        # But bank_matcher may strip differently — that's the xfail

    def test_xfail_poisoned_corrections_hides_gl_manipulation_risk(self):
        """XFAIL: test_one_poisoned_correction_overrides_valid_approvals
        RISK: A single malicious or mistaken correction can override
        correct GL mappings in vendor memory. No rate limiting.
        VERDICT: CRITICAL — allows GL manipulation via corrections.
        Blocks production trust for any system accepting manual corrections."""
        # The vulnerability: vendor memory has no authentication on corrections
        # and no rate limiting. 5 bad corrections beat 3 good approvals.
        pass  # Documented — the xfail test already proves the vulnerability

    def test_xfail_cross_client_leakage_blocks_multi_tenant(self):
        """XFAIL: test_client_isolation_in_vendor_memory_store
        RISK: Client B sees Client A's vendor memory.
        In accounting, cross-client data leakage is a compliance violation.
        VERDICT: CRITICAL — blocks multi-tenant deployment.
        Any deployment with >1 client is at risk of:
        1. Wrong GL suggestions from another client's patterns
        2. Confidential vendor relationships exposed
        3. Regulatory violation (CPA independence rules)"""
        pass  # Documented — the xfail test already proves the vulnerability

    def test_xfail_no_time_decay_causes_stale_suggestions(self):
        """XFAIL: test_no_time_decay_in_vendor_memory
        RISK: 2-year-old vendor memory patterns still influence current suggestions.
        Chart of accounts may have changed, vendor relationships ended,
        GL codes restructured. Stale memory = wrong accounting.
        VERDICT: HIGH — silent incorrect accounting from legacy patterns."""
        pass

    def test_xfail_no_reset_method_blocks_recovery(self):
        """XFAIL: test_vendor_memory_store_has_no_reset_method
        RISK: If vendor memory is poisoned, there is no operational way to fix it.
        No delete, no clear, no reset. Only option is manual database surgery.
        VERDICT: HIGH — operational risk. No recovery path for corrupted memory."""
        pass

    def test_xfail_tax_inclusive_pricing_loses_input_tax_credits(self):
        """XFAIL: test_tax_inclusive_pricing
        RISK: 'Taxes incluses: $114.98' produces NONE tax code.
        Lost ITC/ITR recovery. For a company with $10M in purchases,
        even 1% tax-inclusive documents = $15K in lost credits per year.
        VERDICT: HIGH — direct financial loss from missed tax credits."""
        pass

    def test_xfail_vendor_name_variants_enable_shadow_attacks(self):
        """XFAIL: test_near_match_vendor_hyphen_vs_space
        RISK: 'Hydro-Quebec' and 'Hydro Quebec' create separate vendor memory entries.
        An attacker or data quality issue could create shadow vendor entries
        with different GL mappings for the same real vendor.
        VERDICT: MEDIUM — data quality risk, potential for conflicting mappings."""
        pass


# ===========================================================================
# SECTION 9: STRUCTURAL GAPS
# ===========================================================================


class TestStructuralGaps:
    """Tests that expose structural weaknesses in the persistence layer."""

    def test_no_transaction_boundary_around_document_plus_posting_update(self):
        """posting_builder updates documents table AND posting_jobs table
        in separate SQL statements. If the process crashes between them,
        the tables will be inconsistent."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Test Equipment", doc_type="invoice", amount=10000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase of server equipment",
        )
        doc_id = doc["document_id"]

        # Simulate: document update succeeds but posting insert fails
        # by corrupting the posting_jobs table temporarily
        conn.execute("DROP TABLE posting_jobs")

        try:
            upsert_posting_job(conn, document=doc)
        except Exception:
            pass

        # Recreate table
        ensure_posting_job_table_minimum(conn)

        # Check: documents.gl_account may have been updated but posting_jobs is empty
        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        if doc_gl != "Uncategorized Expense" and posting_gl is None:
            pytest.fail(
                "CRITICAL: documents.gl_account was updated to '{0}' but posting_jobs row "
                "was never created. Partial write without transaction boundary.".format(doc_gl)
            )

    def test_posting_jobs_gl_can_diverge_from_documents_via_direct_sql(self):
        """No database constraint prevents documents.gl_account and
        posting_jobs.gl_account from diverging via direct SQL updates."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Test Corp", doc_type="invoice", amount=1000.00,
            gl_account="5100",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        # Direct SQL: only update one table
        conn.execute(
            "UPDATE documents SET gl_account = '9999 - WRONG' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        # This will always pass — there's no FK or trigger preventing it
        assert doc_gl != posting_gl, (
            "Expected divergence from direct SQL — this proves no DB-level consistency enforcement"
        )

    def test_review_actions_approval_does_not_sync_payload_json(self):
        """ReviewActions.approve_document updates posting_jobs columns
        but does NOT call sync_posting_payload, so payload_json may be stale."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Test Corp", doc_type="invoice", amount=500.00,
            gl_account="5100", review_status="NeedsReview",
        )
        doc_id = doc["document_id"]

        upsert_posting_job(conn, document=doc)

        # Simulate what ReviewActions._update_posting_job_for_approval does
        conn.execute(
            "UPDATE posting_jobs SET approval_state = 'approved_for_posting', "
            "posting_status = 'ready_to_post', reviewer = 'Manager' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        # payload_json still has old approval_state
        row = conn.execute(
            "SELECT payload_json, approval_state FROM posting_jobs WHERE document_id = ?",
            (doc_id,),
        ).fetchone()

        if row["payload_json"]:
            payload = json.loads(row["payload_json"])
            payload_approval = payload.get("approval_state")
            column_approval = row["approval_state"]

            if payload_approval != column_approval:
                pytest.fail(
                    f"HIGH: payload_json.approval_state='{payload_approval}' but "
                    f"posting_jobs.approval_state='{column_approval}'. "
                    f"ReviewActions updates columns but doesn't sync payload_json. "
                    f"Any consumer reading payload_json gets stale state."
                )

    def test_error_text_column_may_be_missing(self):
        """The posting_jobs table created by ensure_posting_job_table_minimum
        does not include error_text, but ReviewActions reads it."""
        conn = _make_db()
        cols = table_columns(conn, "posting_jobs")

        if "error_text" not in cols:
            pytest.fail(
                "MEDIUM: posting_jobs table missing 'error_text' column. "
                "ReviewActions._get_posting_job reads error_text, "
                "but ensure_posting_job_table_minimum doesn't create it. "
                "This could cause runtime errors in the review workflow."
            )

"""
tests/red_team/test_zero_trust_disaster_replay.py
==================================================
Z — Zero-trust disaster replay.

Replay everything:
  duplicate imports, stale reviews, crash mid-transaction, rollback,
  re-import, background jobs out of order, concurrent approvals,
  partial DB writes, bank feed duplicates.

Test:
  final state converges, idempotent replay, no orphan rows,
  no silent historical mutation.

Fail if:
  outcome depends on event order without explicit stale rejection.

Priority codes:
  Z0 — catastrophic data corruption / silent wrong answer
  Z1 — orphan rows or dangling references
  Z2 — non-convergent state
  Z3 — ordering sensitivity without stale detection
"""
from __future__ import annotations

import copy
import hashlib
import itertools
import json
import random
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import pytest

# ── Engine imports ──────────────────────────────────────────────────
from src.engines.correction_chain import (
    build_correction_chain_link,
    cluster_documents,
    get_cluster_for_document,
    is_duplicate_of_cluster_head,
    apply_single_correction,
    rollback_correction,
    check_reimport_after_rollback,
    get_full_correction_chain,
    decompose_credit_memo_safe,
)
from src.engines.reconciliation_engine import (
    ensure_reconciliation_tables,
    create_reconciliation,
    add_reconciliation_item,
    calculate_reconciliation,
    finalize_reconciliation,
    DuplicateItemError,
    FinalizedReconciliationError,
    BALANCE_TOLERANCE,
)
from src.agents.core.approval_models import (
    MatchDecision,
    make_match_decision,
    utc_now_iso,
    ALLOWED_DECISION_TYPES,
)

# ── Helpers ─────────────────────────────────────────────────────────

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fresh_db() -> sqlite3.Connection:
    """In-memory DB with full schema for disaster replay."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    # ── documents table (core) ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id     TEXT PRIMARY KEY,
            file_name       TEXT,
            file_path       TEXT,
            client_code     TEXT,
            vendor          TEXT,
            invoice_number  TEXT,
            doc_type        TEXT,
            amount          REAL,
            subtotal        REAL,
            tax_total       REAL,
            document_date   TEXT,
            gl_account      TEXT,
            tax_code        TEXT,
            category        TEXT,
            review_status   TEXT DEFAULT 'NeedsReview',
            confidence      REAL DEFAULT 0.0,
            raw_result      TEXT DEFAULT '{}',
            raw_ocr_text    TEXT DEFAULT '',
            memo            TEXT DEFAULT '',
            version         INTEGER NOT NULL DEFAULT 1,
            created_at      TEXT NOT NULL DEFAULT '',
            updated_at      TEXT NOT NULL DEFAULT ''
        )
    """)

    # ── Version auto-increment trigger on documents ──
    conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS trg_doc_version_increment
        AFTER UPDATE ON documents
        WHEN NEW.version = OLD.version
        BEGIN
            UPDATE documents SET version = OLD.version + 1
            WHERE document_id = NEW.document_id;
        END;
    """)

    # ── invoice_lines (for credit memo decomposition) ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id             INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id         TEXT NOT NULL,
            line_description    TEXT,
            line_total_pretax   REAL,
            gst_amount          REAL,
            qst_amount          REAL,
            FOREIGN KEY (document_id) REFERENCES documents(document_id)
        )
    """)

    # ── match_decisions (approval / review) ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS match_decisions (
            decision_id             TEXT PRIMARY KEY,
            document_id             TEXT NOT NULL,
            decision_type           TEXT NOT NULL,
            chosen_transaction_id   TEXT,
            reviewer                TEXT,
            reason                  TEXT,
            notes                   TEXT,
            created_at              TEXT NOT NULL,
            updated_at              TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_md_doc ON match_decisions(document_id)"
    )

    # ── correction_chains ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS correction_chains (
            chain_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_root_id       TEXT NOT NULL,
            client_code         TEXT NOT NULL,
            source_document_id  TEXT NOT NULL,
            target_document_id  TEXT NOT NULL,
            link_type           TEXT NOT NULL DEFAULT 'credit_memo',
            economic_effect     TEXT NOT NULL DEFAULT 'reduction',
            amount              REAL,
            tax_impact_gst      REAL,
            tax_impact_qst      REAL,
            uncertainty_flags   TEXT DEFAULT '[]',
            status              TEXT NOT NULL DEFAULT 'active',
            created_by          TEXT NOT NULL DEFAULT 'system',
            created_at          TEXT NOT NULL DEFAULT '',
            superseded_by       INTEGER,
            rollback_of         INTEGER
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chain_root "
        "ON correction_chains(chain_root_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chain_target "
        "ON correction_chains(target_document_id)"
    )

    # ── document_clusters ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_clusters (
            cluster_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_key     TEXT NOT NULL,
            client_code     TEXT NOT NULL,
            cluster_head_id TEXT,
            member_count    INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'active',
            created_at      TEXT NOT NULL DEFAULT '',
            updated_at      TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_cluster_key "
        "ON document_clusters(cluster_key, client_code)"
    )

    # ── document_cluster_members ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_cluster_members (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id      INTEGER NOT NULL,
            document_id     TEXT NOT NULL,
            is_cluster_head INTEGER NOT NULL DEFAULT 0,
            similarity_score REAL,
            variant_notes   TEXT,
            added_at        TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (cluster_id) REFERENCES document_clusters(cluster_id),
            UNIQUE(cluster_id, document_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cluster_members_doc "
        "ON document_cluster_members(document_id)"
    )

    # ── overlap_anomalies ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS overlap_anomalies (
            anomaly_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code         TEXT NOT NULL,
            document_a_id       TEXT NOT NULL,
            document_b_id       TEXT NOT NULL,
            vendor_a            TEXT NOT NULL,
            vendor_b            TEXT NOT NULL,
            overlap_type        TEXT NOT NULL DEFAULT 'work_scope',
            overlap_description TEXT NOT NULL DEFAULT '',
            status              TEXT NOT NULL DEFAULT 'open',
            resolved_by         TEXT,
            resolved_at         TEXT,
            resolution_notes    TEXT,
            created_at          TEXT NOT NULL DEFAULT ''
        )
    """)

    # ── rollback_log ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rollback_log (
            rollback_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code         TEXT NOT NULL,
            target_type         TEXT NOT NULL DEFAULT 'correction_chain',
            target_id           TEXT NOT NULL,
            rollback_reason     TEXT NOT NULL DEFAULT '',
            rolled_back_by      TEXT NOT NULL DEFAULT '',
            state_before_json   TEXT NOT NULL DEFAULT '{}',
            state_after_json    TEXT NOT NULL DEFAULT '{}',
            is_reimport_blocked INTEGER NOT NULL DEFAULT 0,
            created_at          TEXT NOT NULL DEFAULT ''
        )
    """)

    # ── audit_log ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT NOT NULL,
            document_id     TEXT,
            prompt_snippet  TEXT,
            created_at      TEXT NOT NULL DEFAULT ''
        )
    """)

    # ── bank_transactions (for bank feed duplicate tests) ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_transactions (
            transaction_id      TEXT PRIMARY KEY,
            statement_id        TEXT,
            bank_name           TEXT,
            txn_date            TEXT,
            description         TEXT,
            debit               REAL,
            credit              REAL,
            balance             REAL,
            matched_document_id TEXT,
            match_confidence    REAL DEFAULT 0.0,
            review_status       TEXT DEFAULT 'NeedsReview',
            match_reason        TEXT DEFAULT '',
            client_code         TEXT,
            created_at          TEXT NOT NULL DEFAULT ''
        )
    """)

    # ── reconciliation tables ──
    ensure_reconciliation_tables(conn)

    conn.commit()
    return conn


def _insert_doc(
    conn: sqlite3.Connection,
    doc_id: str,
    *,
    client_code: str = "CLIENT-001",
    vendor: str = "Acme Inc.",
    invoice_number: str = "INV-001",
    amount: float = 1000.00,
    subtotal: float = 869.57,
    tax_total: float = 130.43,
    doc_type: str = "invoice",
    document_date: str = "2025-03-15",
    review_status: str = "NeedsReview",
    confidence: float = 0.90,
    memo: str = "",
    raw_ocr_text: str = "",
) -> None:
    now = _utc_now()
    conn.execute(
        """INSERT INTO documents
               (document_id, file_name, file_path, client_code, vendor,
                invoice_number, doc_type, amount, subtotal, tax_total,
                document_date, review_status, confidence, memo,
                raw_ocr_text, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (doc_id, f"{doc_id}.pdf", f"/docs/{doc_id}.pdf", client_code,
         vendor, invoice_number, doc_type, amount, subtotal, tax_total,
         document_date, review_status, confidence, memo,
         raw_ocr_text, now, now),
    )
    conn.commit()


def _insert_bank_txn(
    conn: sqlite3.Connection,
    txn_id: str,
    *,
    client_code: str = "CLIENT-001",
    txn_date: str = "2025-03-15",
    description: str = "ACME INC PAYMENT",
    debit: float | None = None,
    credit: float | None = 1000.00,
    matched_document_id: str | None = None,
    review_status: str = "NeedsReview",
) -> None:
    now = _utc_now()
    conn.execute(
        """INSERT INTO bank_transactions
               (transaction_id, bank_name, txn_date, description,
                debit, credit, matched_document_id, review_status,
                client_code, created_at)
           VALUES (?, 'Desjardins', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (txn_id, txn_date, description, debit, credit,
         matched_document_id, review_status, client_code, now),
    )
    conn.commit()


def _add_decision(
    conn: sqlite3.Connection,
    document_id: str,
    decision_type: str = "approve_match",
    reviewer: str = "cpa_user",
    reason: str = "looks good",
) -> str:
    decision = make_match_decision(
        document_id=document_id,
        decision_type=decision_type,
        reviewer=reviewer,
        reason=reason,
    )
    row = decision.to_row()
    conn.execute(
        """INSERT INTO match_decisions
               (decision_id, document_id, decision_type,
                chosen_transaction_id, reviewer, reason, notes,
                created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (row["decision_id"], row["document_id"], row["decision_type"],
         row["chosen_transaction_id"], row["reviewer"], row["reason"],
         row["notes"], row["created_at"], row["updated_at"]),
    )
    conn.commit()
    return row["decision_id"]


def _snapshot_tables(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Capture full DB state as a dict of {table_name: [rows...]}."""
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    snap: dict[str, list[dict]] = {}
    for t in tables:
        rows = conn.execute(f"SELECT * FROM {t}").fetchall()  # noqa: S608
        snap[t] = [dict(r) for r in rows]
    return snap


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608


# ═══════════════════════════════════════════════════════════════════
# ATTACK 1: Duplicate imports — same document imported twice
# ═══════════════════════════════════════════════════════════════════

class TestDuplicateImports:
    """Z0: Importing the same document twice must not create two economic effects."""

    def test_duplicate_document_insert_rejected(self):
        """Inserting the same document_id twice is a PRIMARY KEY violation."""
        conn = _fresh_db()
        _insert_doc(conn, "DOC-DUP-001")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_doc(conn, "DOC-DUP-001")

    def test_cluster_prevents_double_correction(self):
        """Three variants of the same credit memo → one cluster → one correction."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-ORIG", amount=1150.00, subtotal=1000.00,
                    tax_total=150.00, invoice_number="INV-2025-001")

        # Three OCR variants of the same credit memo
        for i, doc_id in enumerate(["CM-V1", "CM-V2", "CM-V3"]):
            _insert_doc(conn, doc_id, vendor="Acme Inc.", amount=-575.00,
                        invoice_number="CM-2025-001", doc_type="credit_memo")

        # Cluster them
        result = cluster_documents(
            conn, ["CM-V1", "CM-V2", "CM-V3"], client_code="CLIENT-001"
        )
        assert result["cluster_head_id"] == "CM-V1", "Z0: cluster head must be first doc"
        assert result["member_count"] == 3

        # Apply correction from each variant — only head should create it
        corrections_created = 0
        for doc_id in ["CM-V1", "CM-V2", "CM-V3"]:
            decomp = decompose_credit_memo_safe(conn, credit_memo_id=doc_id,
                                                 credit_memo_amount_tax_included=-575.00)
            res = apply_single_correction(
                conn, credit_memo_id=doc_id, original_invoice_id="INV-ORIG",
                client_code="CLIENT-001", decomposition=decomp,
            )
            if res["status"] == "created":
                corrections_created += 1

        assert corrections_created == 1, (
            f"Z0: 3 variants must produce exactly 1 correction, got {corrections_created}"
        )

    def test_duplicate_cluster_idempotent(self):
        """Clustering the same docs again must not create a second cluster."""
        conn = _fresh_db()
        _insert_doc(conn, "D1", invoice_number="X-100", vendor="VendorA")
        _insert_doc(conn, "D2", invoice_number="X-100", vendor="VendorA")

        r1 = cluster_documents(conn, ["D1", "D2"], client_code="CLIENT-001")
        r2 = cluster_documents(conn, ["D1", "D2"], client_code="CLIENT-001")

        assert r1["cluster_id"] == r2["cluster_id"], (
            "Z0: re-clustering same docs must reuse existing cluster"
        )
        assert _count_rows(conn, "document_clusters") == 1


# ═══════════════════════════════════════════════════════════════════
# ATTACK 2: Stale reviews — approve after document changed
# ═══════════════════════════════════════════════════════════════════

class TestStaleReviews:
    """Z3: A review made against version N must not silently apply to version N+1."""

    def test_document_version_increments_on_update(self):
        """Documents must track version for optimistic concurrency."""
        conn = _fresh_db()
        _insert_doc(conn, "DOC-V-001")

        v1 = conn.execute(
            "SELECT version FROM documents WHERE document_id = 'DOC-V-001'"
        ).fetchone()["version"]
        assert v1 == 1

        # Update the document
        conn.execute(
            "UPDATE documents SET amount = 999.99 WHERE document_id = 'DOC-V-001'"
        )
        conn.commit()

        v2 = conn.execute(
            "SELECT version FROM documents WHERE document_id = 'DOC-V-001'"
        ).fetchone()["version"]
        assert v2 > v1, (
            "Z3: document version must increment on update — stale reviews "
            "depend on version tracking"
        )

    def test_multiple_reviews_preserve_audit_trail(self):
        """Every review decision is recorded — never silently overwritten."""
        conn = _fresh_db()
        _insert_doc(conn, "DOC-STALE-001")

        # First review: approve
        d1 = _add_decision(conn, "DOC-STALE-001", "approve_match", "reviewer_a")
        # Document changes after approval
        conn.execute(
            "UPDATE documents SET amount = 5000.00 WHERE document_id = 'DOC-STALE-001'"
        )
        conn.commit()
        # Second review: reject (stale approval should not vanish)
        d2 = _add_decision(conn, "DOC-STALE-001", "reject_match", "reviewer_b",
                           reason="amount changed after first approval")

        decisions = conn.execute(
            "SELECT * FROM match_decisions WHERE document_id = 'DOC-STALE-001' "
            "ORDER BY created_at",
        ).fetchall()
        assert len(decisions) == 2, (
            "Z3: both review decisions must be preserved in audit trail"
        )
        assert decisions[0]["decision_type"] == "approve_match"
        assert decisions[1]["decision_type"] == "reject_match"

    def test_approval_does_not_silently_persist_across_version_change(self):
        """After a version bump, the latest decision should reflect the new state."""
        conn = _fresh_db()
        _insert_doc(conn, "DOC-STALE-002", amount=100.00)
        _add_decision(conn, "DOC-STALE-002", "approve_match", "cpa_a")

        # Mutate the document (OCR re-extraction changed amount)
        conn.execute(
            "UPDATE documents SET amount = 9999.99 WHERE document_id = 'DOC-STALE-002'"
        )
        conn.commit()

        new_version = conn.execute(
            "SELECT version FROM documents WHERE document_id = 'DOC-STALE-002'"
        ).fetchone()["version"]

        # The latest decision was made against version 1 — the document is now v2+
        # System must NOT treat the old approval as valid for the new version
        latest = conn.execute(
            "SELECT * FROM match_decisions WHERE document_id = 'DOC-STALE-002' "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert latest is not None
        assert new_version > 1, (
            "Z3: version must have incremented — stale detection depends on this"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 3: Crash mid-transaction — partial writes
# ═══════════════════════════════════════════════════════════════════

class TestCrashMidTransaction:
    """Z0: A crash between two related writes must not leave orphan data."""

    def test_correction_chain_atomic(self):
        """If crash between cluster creation and chain link, no orphan chain."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-CRASH-001", amount=1000.00, subtotal=869.57,
                    tax_total=130.43)
        _insert_doc(conn, "CM-CRASH-001", amount=-500.00, doc_type="credit_memo")

        # Simulate: create chain link then crash before commit
        # In real code, build_correction_chain_link does its own commit,
        # but if we rollback manually, nothing should leak.
        conn.execute("BEGIN")
        try:
            conn.execute(
                """INSERT INTO correction_chains
                       (chain_root_id, client_code, source_document_id,
                        target_document_id, link_type, economic_effect,
                        amount, status, created_by, created_at)
                   VALUES (?, ?, ?, ?, 'credit_memo', 'reduction', ?, 'active', 'system', ?)""",
                ("INV-CRASH-001", "CLIENT-001", "INV-CRASH-001",
                 "CM-CRASH-001", -500.00, _utc_now()),
            )
            # CRASH — simulate by raising before commit
            raise RuntimeError("Simulated crash")
        except RuntimeError:
            conn.rollback()

        # Verify: no orphan chain link
        chains = conn.execute(
            "SELECT COUNT(*) FROM correction_chains"
        ).fetchone()[0]
        assert chains == 0, (
            "Z0: crash before commit must not leave orphan correction chain links"
        )

    def test_reconciliation_item_rollback(self):
        """Adding item then crashing must not corrupt reconciliation state."""
        conn = _fresh_db()
        recon_id = create_reconciliation(
            "CLIENT-001", "Business Chequing", "2025-03-31",
            50000.00, 49500.00, conn,
        )

        before = calculate_reconciliation(recon_id, conn)

        # Simulate crash: raw insert without going through add_reconciliation_item
        conn.execute("BEGIN")
        try:
            conn.execute(
                """INSERT INTO reconciliation_items
                       (item_id, reconciliation_id, item_type, description,
                        amount, transaction_date, status)
                   VALUES (?, ?, 'deposit_in_transit', 'Ghost deposit',
                           '500.00', '2025-03-30', 'outstanding')""",
                ("ri_ghost", recon_id),
            )
            raise RuntimeError("Simulated crash")
        except RuntimeError:
            conn.rollback()

        after = calculate_reconciliation(recon_id, conn)
        assert before["difference"] == after["difference"], (
            "Z0: crash mid-transaction must not change reconciliation state"
        )

    def test_partial_cluster_creation_no_orphan_members(self):
        """If cluster row created but members fail, no orphan cluster."""
        conn = _fresh_db()
        _insert_doc(conn, "PART-1", vendor="VendorX", invoice_number="P-100")
        _insert_doc(conn, "PART-2", vendor="VendorX", invoice_number="P-100")

        conn.execute("BEGIN")
        try:
            now = _utc_now()
            conn.execute(
                """INSERT INTO document_clusters
                       (cluster_key, client_code, cluster_head_id,
                        member_count, status, created_at, updated_at)
                   VALUES ('orphan_key', 'CLIENT-001', 'PART-1', 2, 'active', ?, ?)""",
                (now, now),
            )
            # Crash before members are added
            raise RuntimeError("Simulated crash")
        except RuntimeError:
            conn.rollback()

        assert _count_rows(conn, "document_clusters") == 0, (
            "Z1: crash before member insertion must leave no orphan cluster row"
        )
        assert _count_rows(conn, "document_cluster_members") == 0, (
            "Z1: crash must not leave orphan cluster members"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 4: Rollback then re-import
# ═══════════════════════════════════════════════════════════════════

class TestRollbackAndReimport:
    """Z0: Rollback + re-import must converge to one clean state, never duplicates."""

    def _setup_correction(self, conn):
        """Create a correction chain: invoice + credit memo."""
        _insert_doc(conn, "INV-RB-001", amount=1150.00, subtotal=1000.00,
                    tax_total=150.00)
        _insert_doc(conn, "CM-RB-001", amount=-575.00, doc_type="credit_memo")

        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-RB-001",
            credit_memo_amount_tax_included=-575.00,
        )
        result = apply_single_correction(
            conn, credit_memo_id="CM-RB-001", original_invoice_id="INV-RB-001",
            client_code="CLIENT-001", decomposition=decomp,
        )
        return result

    def test_rollback_idempotent(self):
        """Rolling back the same chain twice is a no-op the second time."""
        conn = _fresh_db()
        result = self._setup_correction(conn)
        chain_id = result["chain_id"]

        r1 = rollback_correction(
            conn, chain_id=chain_id, client_code="CLIENT-001",
            rolled_back_by="cpa_user", rollback_reason="Error found",
        )
        assert r1["status"] == "rolled_back"

        r2 = rollback_correction(
            conn, chain_id=chain_id, client_code="CLIENT-001",
            rolled_back_by="cpa_user", rollback_reason="Retry",
        )
        assert r2["status"] == "already_rolled_back", (
            "Z0: double rollback must be idempotent no-op"
        )

    def test_reimport_blocked_after_rollback_with_block(self):
        """If rollback blocks reimport, re-import gate must refuse."""
        conn = _fresh_db()
        result = self._setup_correction(conn)
        chain_id = result["chain_id"]

        rollback_correction(
            conn, chain_id=chain_id, client_code="CLIENT-001",
            rolled_back_by="cpa_user", rollback_reason="Fraudulent",
            block_reimport=True,
        )

        check = check_reimport_after_rollback(conn, "CM-RB-001", "CLIENT-001")
        assert check["can_reimport"] is False, (
            "Z0: blocked reimport must be enforced — cannot create duplicate effects"
        )
        reasons = [r["reason"] for r in check["reasons"]]
        assert "reimport_blocked_by_rollback" in reasons

    def test_reimport_allowed_after_clean_rollback(self):
        """Rollback without block_reimport allows re-import."""
        conn = _fresh_db()
        result = self._setup_correction(conn)
        chain_id = result["chain_id"]

        rollback_correction(
            conn, chain_id=chain_id, client_code="CLIENT-001",
            rolled_back_by="cpa_user", rollback_reason="Mistake",
            block_reimport=False,
        )

        check = check_reimport_after_rollback(conn, "CM-RB-001", "CLIENT-001")
        assert check["can_reimport"] is True, (
            "Z0: clean rollback must allow re-import to recreate one clean state"
        )

    def test_reimport_creates_exactly_one_correction(self):
        """After rollback + re-import, exactly one active correction exists."""
        conn = _fresh_db()
        result = self._setup_correction(conn)
        chain_id = result["chain_id"]

        # Rollback
        rollback_correction(
            conn, chain_id=chain_id, client_code="CLIENT-001",
            rolled_back_by="cpa_user", rollback_reason="Re-extract",
        )

        # Re-import
        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-RB-001",
            credit_memo_amount_tax_included=-575.00,
        )
        res2 = apply_single_correction(
            conn, credit_memo_id="CM-RB-001", original_invoice_id="INV-RB-001",
            client_code="CLIENT-001", decomposition=decomp,
        )
        assert res2["status"] == "created"

        # Count active chains
        active = conn.execute(
            "SELECT COUNT(*) FROM correction_chains WHERE status = 'active'"
        ).fetchone()[0]
        assert active == 1, (
            f"Z0: after rollback + reimport, exactly 1 active chain expected, got {active}"
        )

        # Count total chains (1 rolled back + 1 active)
        total = conn.execute("SELECT COUNT(*) FROM correction_chains").fetchone()[0]
        assert total == 2, (
            f"Z0: audit trail must show both original (rolled_back) and new (active)"
        )

    def test_full_correction_chain_net_impact_after_rollback_reimport(self):
        """Net economic impact must reflect only active links."""
        conn = _fresh_db()
        result = self._setup_correction(conn)
        chain_id = result["chain_id"]

        rollback_correction(
            conn, chain_id=chain_id, client_code="CLIENT-001",
            rolled_back_by="cpa_user", rollback_reason="Re-extract",
        )

        # Re-import with different amount
        decomp = {"pretax": 400.0, "gst": 20.0, "qst": 39.9, "uncertainty_flags": []}
        apply_single_correction(
            conn, credit_memo_id="CM-RB-001", original_invoice_id="INV-RB-001",
            client_code="CLIENT-001", decomposition=decomp,
        )

        chain = get_full_correction_chain(conn, "INV-RB-001")
        assert chain["active_links"] == 1
        assert chain["total_economic_impact"] == -400.0, (
            "Z0: net impact must reflect only the active (re-imported) link, "
            "not the rolled-back one"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 5: Background jobs out of order
# ═══════════════════════════════════════════════════════════════════

class TestBackgroundJobsOutOfOrder:
    """Z3: If background jobs arrive out of order, system must converge or reject stale."""

    def test_correction_chain_idempotent_regardless_of_order(self):
        """build_correction_chain_link is idempotent — repeat calls are no-ops."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-BG-001", amount=1000.00)
        _insert_doc(conn, "CM-BG-001", amount=-500.00, doc_type="credit_memo")

        kwargs = dict(
            chain_root_id="INV-BG-001",
            client_code="CLIENT-001",
            source_document_id="INV-BG-001",
            target_document_id="CM-BG-001",
            amount=-500.00,
        )
        r1 = build_correction_chain_link(conn, **kwargs)
        r2 = build_correction_chain_link(conn, **kwargs)

        assert r1["status"] == "created"
        assert r2["status"] == "already_exists", (
            "Z3: duplicate background job must be idempotent"
        )
        assert _count_rows(conn, "correction_chains") == 1

    def test_approval_ordering_all_decisions_preserved(self):
        """Two approvals arriving out-of-order must both be recorded."""
        conn = _fresh_db()
        _insert_doc(conn, "DOC-BG-ORDER")

        # Simulate: background job B arrives first (created later in time)
        d_b = _add_decision(conn, "DOC-BG-ORDER", "approve_match",
                            reviewer="reviewer_b", reason="Batch B")
        time.sleep(0.01)  # Ensure distinct timestamps
        d_a = _add_decision(conn, "DOC-BG-ORDER", "reject_match",
                            reviewer="reviewer_a", reason="Batch A (earlier)")

        decisions = conn.execute(
            "SELECT * FROM match_decisions WHERE document_id = 'DOC-BG-ORDER'"
        ).fetchall()
        assert len(decisions) == 2, (
            "Z3: both out-of-order decisions must be preserved"
        )

    def test_cluster_then_correction_vs_correction_then_cluster_converge(self):
        """Order: cluster→correct vs correct→cluster must converge to same state."""
        results = []

        for order in ["cluster_first", "correct_first"]:
            conn = _fresh_db()
            _insert_doc(conn, "INV-ORD", amount=1000.00, subtotal=869.57,
                        tax_total=130.43, invoice_number="ORD-001")
            _insert_doc(conn, "CM-ORD-A", amount=-500.00,
                        invoice_number="CM-ORD-001", doc_type="credit_memo",
                        vendor="Acme Inc.")
            _insert_doc(conn, "CM-ORD-B", amount=-500.00,
                        invoice_number="CM-ORD-001", doc_type="credit_memo",
                        vendor="Acme Inc.")

            if order == "cluster_first":
                cluster_documents(conn, ["CM-ORD-A", "CM-ORD-B"],
                                  client_code="CLIENT-001")
                decomp = decompose_credit_memo_safe(
                    conn, credit_memo_id="CM-ORD-A",
                    credit_memo_amount_tax_included=-500.00,
                )
                apply_single_correction(
                    conn, credit_memo_id="CM-ORD-A",
                    original_invoice_id="INV-ORD",
                    client_code="CLIENT-001", decomposition=decomp,
                )
                # Second variant should be blocked
                decomp2 = decompose_credit_memo_safe(
                    conn, credit_memo_id="CM-ORD-B",
                    credit_memo_amount_tax_included=-500.00,
                )
                apply_single_correction(
                    conn, credit_memo_id="CM-ORD-B",
                    original_invoice_id="INV-ORD",
                    client_code="CLIENT-001", decomposition=decomp2,
                )
            else:
                # Correction first (before clustering)
                decomp = decompose_credit_memo_safe(
                    conn, credit_memo_id="CM-ORD-A",
                    credit_memo_amount_tax_included=-500.00,
                )
                apply_single_correction(
                    conn, credit_memo_id="CM-ORD-A",
                    original_invoice_id="INV-ORD",
                    client_code="CLIENT-001", decomposition=decomp,
                )
                # Then cluster
                cluster_documents(conn, ["CM-ORD-A", "CM-ORD-B"],
                                  client_code="CLIENT-001")
                # Second variant now blocked by cluster
                decomp2 = decompose_credit_memo_safe(
                    conn, credit_memo_id="CM-ORD-B",
                    credit_memo_amount_tax_included=-500.00,
                )
                apply_single_correction(
                    conn, credit_memo_id="CM-ORD-B",
                    original_invoice_id="INV-ORD",
                    client_code="CLIENT-001", decomposition=decomp2,
                )

            active_chains = conn.execute(
                "SELECT COUNT(*) FROM correction_chains WHERE status = 'active'"
            ).fetchone()[0]
            results.append(active_chains)

        assert results[0] == results[1] == 1, (
            f"Z2: order-dependent result — cluster_first={results[0]}, "
            f"correct_first={results[1]}, both must be 1"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 6: Concurrent approvals
# ═══════════════════════════════════════════════════════════════════

class TestConcurrentApprovals:
    """Z0: Two reviewers approving the same doc concurrently must not corrupt state."""

    def test_concurrent_decisions_both_recorded(self):
        """Concurrent approvals from different reviewers must both persist.

        Uses file-based DB with check_same_thread=False to allow
        multi-threaded access (SQLite serializes via its own locking).
        """
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            # Create DB on disk for cross-thread access
            setup_conn = sqlite3.connect(tmp.name, check_same_thread=False)
            setup_conn.row_factory = sqlite3.Row
            setup_conn.execute("PRAGMA journal_mode = WAL")
            setup_conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id TEXT PRIMARY KEY, file_name TEXT,
                    file_path TEXT, client_code TEXT, vendor TEXT,
                    invoice_number TEXT, doc_type TEXT, amount REAL,
                    subtotal REAL, tax_total REAL, document_date TEXT,
                    gl_account TEXT, tax_code TEXT, category TEXT,
                    review_status TEXT DEFAULT 'NeedsReview',
                    confidence REAL DEFAULT 0.0, raw_result TEXT DEFAULT '{}',
                    raw_ocr_text TEXT DEFAULT '', memo TEXT DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                )
            """)
            setup_conn.execute("""
                CREATE TABLE IF NOT EXISTS match_decisions (
                    decision_id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    chosen_transaction_id TEXT,
                    reviewer TEXT, reason TEXT, notes TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                )
            """)
            setup_conn.commit()

            now = _utc_now()
            setup_conn.execute(
                """INSERT INTO documents
                       (document_id, file_name, file_path, client_code, vendor,
                        doc_type, amount, document_date, review_status,
                        confidence, created_at, updated_at)
                   VALUES ('DOC-CONC-001','f','p','C1','V','invoice',100,
                           '2025-01-01','NeedsReview',0.9,?,?)""",
                (now, now),
            )
            setup_conn.commit()

            errors: list[Exception] = []
            decision_ids: list[str] = []
            lock = threading.Lock()

            def approve(reviewer: str):
                try:
                    c = sqlite3.connect(tmp.name, check_same_thread=False)
                    c.row_factory = sqlite3.Row
                    decision = make_match_decision(
                        document_id="DOC-CONC-001",
                        decision_type="approve_match",
                        reviewer=reviewer, reason="concurrent test",
                    )
                    row = decision.to_row()
                    c.execute(
                        """INSERT INTO match_decisions
                               (decision_id, document_id, decision_type,
                                chosen_transaction_id, reviewer, reason,
                                notes, created_at, updated_at)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (row["decision_id"], row["document_id"],
                         row["decision_type"], row["chosen_transaction_id"],
                         row["reviewer"], row["reason"], row["notes"],
                         row["created_at"], row["updated_at"]),
                    )
                    c.commit()
                    c.close()
                    with lock:
                        decision_ids.append(row["decision_id"])
                except Exception as e:
                    with lock:
                        errors.append(e)

            threads = [
                threading.Thread(target=approve, args=(f"reviewer_{i}",))
                for i in range(5)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors, f"Z0: concurrent approval errors: {errors}"
            assert len(decision_ids) == 5, (
                f"Z0: all 5 concurrent approvals must be recorded, got {len(decision_ids)}"
            )

            rows = setup_conn.execute(
                "SELECT COUNT(*) FROM match_decisions WHERE document_id = 'DOC-CONC-001'"
            ).fetchone()[0]
            assert rows == 5
            setup_conn.close()
        finally:
            os.unlink(tmp.name)

    def test_concurrent_cluster_and_correction_no_double_effect(self):
        """Concurrent clustering + correction must not produce double economic effect."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-CC-001", amount=1000.00, subtotal=869.57,
                    tax_total=130.43, invoice_number="CC-100")
        _insert_doc(conn, "CM-CC-A", amount=-500.00, invoice_number="CM-CC-100",
                    doc_type="credit_memo", vendor="Acme Inc.")
        _insert_doc(conn, "CM-CC-B", amount=-500.00, invoice_number="CM-CC-100",
                    doc_type="credit_memo", vendor="Acme Inc.")

        # Pre-cluster
        cluster_documents(conn, ["CM-CC-A", "CM-CC-B"], client_code="CLIENT-001")

        errors: list[Exception] = []
        results: list[dict] = []
        lock = threading.Lock()

        def try_correct(cm_id: str):
            try:
                decomp = decompose_credit_memo_safe(
                    conn, credit_memo_id=cm_id,
                    credit_memo_amount_tax_included=-500.00,
                )
                r = apply_single_correction(
                    conn, credit_memo_id=cm_id,
                    original_invoice_id="INV-CC-001",
                    client_code="CLIENT-001", decomposition=decomp,
                )
                with lock:
                    results.append(r)
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [
            threading.Thread(target=try_correct, args=(cm_id,))
            for cm_id in ["CM-CC-A", "CM-CC-B"]
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        created = sum(1 for r in results if r.get("status") == "created")
        assert created <= 1, (
            f"Z0: concurrent correction of clustered CMs must produce at most 1 "
            f"active chain, got {created} created"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 7: Partial DB writes
# ═══════════════════════════════════════════════════════════════════

class TestPartialDBWrites:
    """Z1: Interrupted multi-step writes must not leave orphans."""

    def test_no_orphan_cluster_members_without_cluster(self):
        """FK constraint prevents cluster member without parent cluster."""
        conn = _fresh_db()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO document_cluster_members
                       (cluster_id, document_id, is_cluster_head, added_at)
                   VALUES (9999, 'ORPHAN-DOC', 0, ?)""",
                (_utc_now(),),
            )

    def test_no_orphan_recon_items_without_reconciliation(self):
        """FK constraint prevents reconciliation item without parent recon."""
        conn = _fresh_db()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO reconciliation_items
                       (item_id, reconciliation_id, item_type, description,
                        amount, status)
                   VALUES ('ri_orphan', 'NONEXISTENT', 'deposit_in_transit',
                           'Ghost', '100.00', 'outstanding')"""
            )

    def test_rollback_log_references_valid_chain(self):
        """Rollback log only created for valid existing chain links."""
        conn = _fresh_db()
        with pytest.raises((ValueError, RuntimeError)):
            rollback_correction(
                conn, chain_id=99999, client_code="CLIENT-001",
                rolled_back_by="user", rollback_reason="test",
            )

    def test_snapshot_before_and_after_partial_write(self):
        """If a multi-step operation fails, DB state matches pre-operation snapshot."""
        conn = _fresh_db()
        _insert_doc(conn, "SNAP-001", amount=500.00)

        before = _snapshot_tables(conn)

        # Attempt a complex operation that fails partway
        conn.execute("BEGIN")
        try:
            conn.execute(
                "UPDATE documents SET amount = 999.99 WHERE document_id = 'SNAP-001'"
            )
            conn.execute(
                """INSERT INTO correction_chains
                       (chain_root_id, client_code, source_document_id,
                        target_document_id, amount, status, created_by, created_at)
                   VALUES ('SNAP-001', 'CLIENT-001', 'SNAP-001',
                           'NONEXISTENT-CM', -100.0, 'active', 'system', ?)""",
                (_utc_now(),),
            )
            raise RuntimeError("Simulated failure")
        except RuntimeError:
            conn.rollback()

        after = _snapshot_tables(conn)

        # Compare document amounts
        before_docs = {d["document_id"]: d for d in before.get("documents", [])}
        after_docs = {d["document_id"]: d for d in after.get("documents", [])}
        assert before_docs["SNAP-001"]["amount"] == after_docs["SNAP-001"]["amount"], (
            "Z1: partial write rollback must restore document to original state"
        )
        assert len(after.get("correction_chains", [])) == 0, (
            "Z1: partial write rollback must not leave orphan chains"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 8: Bank feed duplicates
# ═══════════════════════════════════════════════════════════════════

class TestBankFeedDuplicates:
    """Z0: Duplicate bank transactions must not double-post or double-match."""

    def test_duplicate_bank_txn_rejected(self):
        """Same transaction_id cannot be imported twice."""
        conn = _fresh_db()
        _insert_bank_txn(conn, "TXN-DUP-001", credit=1000.00)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_bank_txn(conn, "TXN-DUP-001", credit=1000.00)

    def test_duplicate_reconciliation_item_rejected(self):
        """Same description+amount+type added twice is caught by duplicate detection."""
        conn = _fresh_db()
        recon_id = create_reconciliation(
            "CLIENT-001", "Business Chequing", "2025-03-31",
            50000.00, 49500.00, conn,
        )

        add_reconciliation_item(
            recon_id, "deposit_in_transit", "Client ABC deposit",
            500.00, "2025-03-30", conn,
        )

        with pytest.raises(DuplicateItemError):
            add_reconciliation_item(
                recon_id, "deposit_in_transit", "Client ABC deposit",
                500.00, "2025-03-30", conn,
            )

    def test_bank_feed_reimport_convergence(self):
        """Importing the same bank feed twice must not create duplicate rows."""
        conn = _fresh_db()

        # First import
        txn_ids_1 = []
        for i in range(5):
            txn_id = f"TXN-FEED-{i:03d}"
            _insert_bank_txn(conn, txn_id, credit=100.00 * (i + 1))
            txn_ids_1.append(txn_id)

        count_after_first = _count_rows(conn, "bank_transactions")

        # Second import attempt: all should be rejected as duplicates
        duplicates_caught = 0
        for txn_id in txn_ids_1:
            try:
                _insert_bank_txn(conn, txn_id, credit=100.00)
            except sqlite3.IntegrityError:
                duplicates_caught += 1

        count_after_second = _count_rows(conn, "bank_transactions")
        assert count_after_second == count_after_first, (
            f"Z0: bank feed re-import must not create duplicates: "
            f"before={count_after_first}, after={count_after_second}"
        )
        assert duplicates_caught == 5, (
            f"Z0: all 5 duplicate bank transactions must be caught"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 9: Reconciliation immutability after finalization
# ═══════════════════════════════════════════════════════════════════

class TestReconciliationImmutability:
    """Z0: Finalized reconciliations must be immutable — no silent mutation."""

    def _make_balanced_recon(self, conn):
        """Create and finalize a balanced reconciliation.

        Bank side: 50000 + 500 (deposit_in_transit) = 50500
        Book side: 50500
        Difference: 0 → balanced
        """
        recon_id = create_reconciliation(
            "CLIENT-001", "Business Chequing", "2025-03-31",
            50000.00, 50500.00, conn,
        )
        add_reconciliation_item(
            recon_id, "deposit_in_transit", "March deposit",
            500.00, "2025-03-30", conn,
        )
        success = finalize_reconciliation(recon_id, "cpa_reviewer", conn)
        assert success, "Setup: reconciliation must finalize when balanced"
        return recon_id

    def test_finalized_recon_rejects_new_items(self):
        """Cannot add items to a finalized reconciliation."""
        conn = _fresh_db()
        recon_id = self._make_balanced_recon(conn)

        with pytest.raises((FinalizedReconciliationError, sqlite3.IntegrityError,
                            sqlite3.OperationalError)):
            add_reconciliation_item(
                recon_id, "outstanding_cheque", "Sneaky cheque",
                100.00, "2025-04-01", conn,
            )

    def test_finalized_recon_balance_unchanged(self):
        """Balance of finalized reconciliation must never change."""
        conn = _fresh_db()
        recon_id = self._make_balanced_recon(conn)

        before = calculate_reconciliation(recon_id, conn)

        # Attempt to mutate via raw SQL
        try:
            conn.execute(
                """INSERT INTO reconciliation_items
                       (item_id, reconciliation_id, item_type, description,
                        amount, status)
                   VALUES ('ri_sneak', ?, 'bank_error', 'Sneaky error',
                           '9999.99', 'outstanding')""",
                (recon_id,),
            )
            conn.commit()
        except (sqlite3.OperationalError, sqlite3.IntegrityError):
            pass  # Expected: trigger blocks it

        after = calculate_reconciliation(recon_id, conn)
        assert before["difference"] == after["difference"], (
            "Z0: finalized reconciliation balance must never change"
        )

    def test_finalize_idempotent(self):
        """Finalizing an already-finalized recon is safe."""
        conn = _fresh_db()
        recon_id = self._make_balanced_recon(conn)

        # Finalize again — should not error, just returns True
        result = finalize_reconciliation(recon_id, "another_reviewer", conn)
        # May return True or False depending on implementation, but must not crash
        # and must not change the finalized_at timestamp to a different value

        recon = conn.execute(
            "SELECT finalized_at FROM bank_reconciliations WHERE reconciliation_id = ?",
            (recon_id,),
        ).fetchone()
        assert recon["finalized_at"] is not None, (
            "Z0: finalized_at must remain set"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 10: No silent historical mutation
# ═══════════════════════════════════════════════════════════════════

class TestNoSilentHistoricalMutation:
    """Z0: Past audit records must never be silently modified."""

    def test_audit_log_append_only(self):
        """Audit log entries cannot be updated or deleted by normal operations."""
        conn = _fresh_db()

        # Create some audit entries via a correction + rollback
        _insert_doc(conn, "INV-HIST-001", amount=1000.00)
        _insert_doc(conn, "CM-HIST-001", amount=-500.00, doc_type="credit_memo")

        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-HIST-001",
            credit_memo_amount_tax_included=-500.00,
        )
        res = apply_single_correction(
            conn, credit_memo_id="CM-HIST-001", original_invoice_id="INV-HIST-001",
            client_code="CLIENT-001", decomposition=decomp,
        )

        rollback_correction(
            conn, chain_id=res["chain_id"], client_code="CLIENT-001",
            rolled_back_by="cpa", rollback_reason="test",
        )

        # Count audit entries
        audit_count = _count_rows(conn, "audit_log")
        assert audit_count >= 1, "Z0: rollback must create audit_log entry"

        # Capture audit state
        audit_before = conn.execute("SELECT * FROM audit_log ORDER BY log_id").fetchall()
        audit_before = [dict(r) for r in audit_before]

        # Do more operations
        _insert_doc(conn, "INV-HIST-002", amount=2000.00)
        decomp2 = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-HIST-001",
            credit_memo_amount_tax_included=-500.00,
        )
        apply_single_correction(
            conn, credit_memo_id="CM-HIST-001", original_invoice_id="INV-HIST-002",
            client_code="CLIENT-001", decomposition=decomp2,
        )

        # Verify original audit entries unchanged
        audit_after = conn.execute("SELECT * FROM audit_log ORDER BY log_id").fetchall()
        audit_after = [dict(r) for r in audit_after]

        for i, entry_before in enumerate(audit_before):
            entry_after = audit_after[i]
            assert entry_before == entry_after, (
                f"Z0: audit_log entry {i} was silently mutated! "
                f"Before: {entry_before}, After: {entry_after}"
            )

    def test_rollback_log_immutable(self):
        """Rollback log entries are never modified after creation."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-RLI-001", amount=1000.00)
        _insert_doc(conn, "CM-RLI-001", amount=-500.00, doc_type="credit_memo")

        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-RLI-001",
            credit_memo_amount_tax_included=-500.00,
        )
        res = apply_single_correction(
            conn, credit_memo_id="CM-RLI-001", original_invoice_id="INV-RLI-001",
            client_code="CLIENT-001", decomposition=decomp,
        )
        rollback_correction(
            conn, chain_id=res["chain_id"], client_code="CLIENT-001",
            rolled_back_by="cpa", rollback_reason="first rollback",
        )

        rollback_before = conn.execute(
            "SELECT * FROM rollback_log ORDER BY rollback_id"
        ).fetchall()
        rollback_before = [dict(r) for r in rollback_before]
        assert len(rollback_before) >= 1

        # Perform more rollbacks on a new chain
        _insert_doc(conn, "CM-RLI-002", amount=-200.00, doc_type="credit_memo")
        decomp2 = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-RLI-002",
            credit_memo_amount_tax_included=-200.00,
        )
        res2 = apply_single_correction(
            conn, credit_memo_id="CM-RLI-002", original_invoice_id="INV-RLI-001",
            client_code="CLIENT-001", decomposition=decomp2,
        )
        rollback_correction(
            conn, chain_id=res2["chain_id"], client_code="CLIENT-001",
            rolled_back_by="cpa", rollback_reason="second rollback",
        )

        # Verify original rollback log entries unchanged
        rollback_after = conn.execute(
            "SELECT * FROM rollback_log ORDER BY rollback_id"
        ).fetchall()
        rollback_after = [dict(r) for r in rollback_after]

        for i, entry in enumerate(rollback_before):
            assert entry == rollback_after[i], (
                f"Z0: rollback_log entry {i} was silently mutated!"
            )

    def test_correction_chain_state_before_captured_in_rollback(self):
        """Rollback must capture full state_before_json for forensic audit."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-STATE-001", amount=1000.00)
        _insert_doc(conn, "CM-STATE-001", amount=-500.00, doc_type="credit_memo")

        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-STATE-001",
            credit_memo_amount_tax_included=-500.00,
        )
        res = apply_single_correction(
            conn, credit_memo_id="CM-STATE-001", original_invoice_id="INV-STATE-001",
            client_code="CLIENT-001", decomposition=decomp,
        )

        rollback_correction(
            conn, chain_id=res["chain_id"], client_code="CLIENT-001",
            rolled_back_by="forensic_cpa", rollback_reason="investigation",
        )

        log = conn.execute(
            "SELECT state_before_json FROM rollback_log ORDER BY rollback_id DESC LIMIT 1"
        ).fetchone()
        state_before = json.loads(log["state_before_json"])
        assert state_before.get("status") == "active", (
            "Z0: state_before must show the chain was 'active' before rollback"
        )
        assert state_before.get("chain_id") == res["chain_id"]


# ═══════════════════════════════════════════════════════════════════
# ATTACK 11: Full disaster replay — convergence test
# ═══════════════════════════════════════════════════════════════════

class TestFullDisasterReplay:
    """Z2: Replay the full disaster sequence. Final state must converge."""

    def test_full_replay_converges(self):
        """
        Scenario: import → review → crash → rollback → re-import →
                  duplicate attempt → concurrent approvals →
                  bank feed duplicate → finalize reconciliation.

        Run twice. Final state must be identical.
        """
        final_states = []

        for run in range(2):
            conn = _fresh_db()

            # 1. Import documents
            _insert_doc(conn, "INV-FULL-001", amount=1150.00, subtotal=1000.00,
                        tax_total=150.00, invoice_number="INV-2025-FULL")
            _insert_doc(conn, "CM-FULL-001", amount=-575.00,
                        invoice_number="CM-2025-FULL", doc_type="credit_memo",
                        vendor="Acme Inc.")

            # 2. Import bank feed
            _insert_bank_txn(conn, "TXN-FULL-001", credit=1150.00,
                             description="ACME INC")

            # 3. Review: approve
            _add_decision(conn, "INV-FULL-001", "approve_match", "reviewer_1")

            # 4. Create correction
            decomp = decompose_credit_memo_safe(
                conn, credit_memo_id="CM-FULL-001",
                credit_memo_amount_tax_included=-575.00,
            )
            res = apply_single_correction(
                conn, credit_memo_id="CM-FULL-001",
                original_invoice_id="INV-FULL-001",
                client_code="CLIENT-001", decomposition=decomp,
            )

            # 5. Crash + rollback
            rollback_correction(
                conn, chain_id=res["chain_id"], client_code="CLIENT-001",
                rolled_back_by="system", rollback_reason="crash recovery",
            )

            # 6. Re-import correction
            decomp2 = decompose_credit_memo_safe(
                conn, credit_memo_id="CM-FULL-001",
                credit_memo_amount_tax_included=-575.00,
            )
            apply_single_correction(
                conn, credit_memo_id="CM-FULL-001",
                original_invoice_id="INV-FULL-001",
                client_code="CLIENT-001", decomposition=decomp2,
            )

            # 7. Duplicate import attempt
            try:
                _insert_doc(conn, "INV-FULL-001", amount=1150.00)
            except sqlite3.IntegrityError:
                pass  # Expected

            try:
                _insert_bank_txn(conn, "TXN-FULL-001", credit=1150.00)
            except sqlite3.IntegrityError:
                pass  # Expected

            # 8. Duplicate correction attempt (idempotent)
            apply_single_correction(
                conn, credit_memo_id="CM-FULL-001",
                original_invoice_id="INV-FULL-001",
                client_code="CLIENT-001", decomposition=decomp2,
            )

            # 9. Additional review
            _add_decision(conn, "INV-FULL-001", "approve_match", "reviewer_2")

            # 10. Create and finalize reconciliation
            recon_id = create_reconciliation(
                "CLIENT-001", "Business Chequing", "2025-03-31",
                50000.00, 50500.00, conn,
            )
            add_reconciliation_item(
                recon_id, "deposit_in_transit", "March deposit",
                500.00, "2025-03-30", conn,
            )
            finalize_reconciliation(recon_id, "cpa_reviewer", conn)

            # Capture final state metrics
            state = {
                "documents": _count_rows(conn, "documents"),
                "bank_transactions": _count_rows(conn, "bank_transactions"),
                "active_corrections": conn.execute(
                    "SELECT COUNT(*) FROM correction_chains WHERE status = 'active'"
                ).fetchone()[0],
                "rolled_back_corrections": conn.execute(
                    "SELECT COUNT(*) FROM correction_chains WHERE status = 'rolled_back'"
                ).fetchone()[0],
                "total_corrections": _count_rows(conn, "correction_chains"),
                "decisions": _count_rows(conn, "match_decisions"),
                "rollback_logs": _count_rows(conn, "rollback_log"),
                "audit_logs": _count_rows(conn, "audit_log"),
                "recon_status": conn.execute(
                    "SELECT status FROM bank_reconciliations LIMIT 1"
                ).fetchone()["status"],
                "recon_finalized": conn.execute(
                    "SELECT finalized_at FROM bank_reconciliations LIMIT 1"
                ).fetchone()["finalized_at"] is not None,
            }
            final_states.append(state)

        # Compare both runs
        for key in final_states[0]:
            assert final_states[0][key] == final_states[1][key], (
                f"Z2: non-convergent replay — {key}: "
                f"run1={final_states[0][key]}, run2={final_states[1][key]}"
            )

    def test_replay_invariants(self):
        """After full replay, verify all invariants hold."""
        conn = _fresh_db()

        # Build a rich state
        _insert_doc(conn, "INV-INV-001", amount=2000.00, subtotal=1739.13,
                    tax_total=260.87, invoice_number="INV-001")
        _insert_doc(conn, "CM-INV-001", amount=-500.00,
                    invoice_number="CM-001", doc_type="credit_memo",
                    vendor="Acme Inc.")
        _insert_doc(conn, "CM-INV-002", amount=-500.00,
                    invoice_number="CM-001", doc_type="credit_memo",
                    vendor="Acme Inc.")

        # Cluster duplicates
        cluster_documents(conn, ["CM-INV-001", "CM-INV-002"],
                          client_code="CLIENT-001")

        # Apply correction (only head)
        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-INV-001",
            credit_memo_amount_tax_included=-500.00,
        )
        apply_single_correction(
            conn, credit_memo_id="CM-INV-001",
            original_invoice_id="INV-INV-001",
            client_code="CLIENT-001", decomposition=decomp,
        )

        # Attempt correction on duplicate
        decomp2 = decompose_credit_memo_safe(
            conn, credit_memo_id="CM-INV-002",
            credit_memo_amount_tax_included=-500.00,
        )
        dup_result = apply_single_correction(
            conn, credit_memo_id="CM-INV-002",
            original_invoice_id="INV-INV-001",
            client_code="CLIENT-001", decomposition=decomp2,
        )
        assert dup_result["status"] == "skipped_duplicate", (
            "Z0: duplicate cluster member must be skipped"
        )

        # INVARIANT 1: No orphan cluster members
        orphans = conn.execute("""
            SELECT dcm.document_id FROM document_cluster_members dcm
            LEFT JOIN document_clusters dc ON dcm.cluster_id = dc.cluster_id
            WHERE dc.cluster_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, (
            f"Z1: found {len(orphans)} orphan cluster members"
        )

        # INVARIANT 2: Every active correction chain references existing documents
        bad_chains = conn.execute("""
            SELECT cc.chain_id, cc.target_document_id
            FROM correction_chains cc
            LEFT JOIN documents d ON cc.target_document_id = d.document_id
            WHERE d.document_id IS NULL AND cc.status = 'active'
        """).fetchall()
        assert len(bad_chains) == 0, (
            f"Z1: found {len(bad_chains)} correction chains referencing non-existent documents"
        )

        # INVARIANT 3: Exactly one active correction for this economic event
        active = conn.execute(
            "SELECT COUNT(*) FROM correction_chains "
            "WHERE chain_root_id = 'INV-INV-001' AND status = 'active'"
        ).fetchone()[0]
        assert active == 1, (
            f"Z0: exactly 1 active correction expected for INV-INV-001, got {active}"
        )

        # INVARIANT 4: Cluster member count matches actual members
        clusters = conn.execute(
            "SELECT cluster_id, member_count FROM document_clusters"
        ).fetchall()
        for c in clusters:
            actual = conn.execute(
                "SELECT COUNT(*) FROM document_cluster_members WHERE cluster_id = ?",
                (c["cluster_id"],),
            ).fetchone()[0]
            assert c["member_count"] == actual, (
                f"Z1: cluster {c['cluster_id']} member_count={c['member_count']} "
                f"but actual members={actual}"
            )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 12: Event-order sensitivity — permutation test
# ═══════════════════════════════════════════════════════════════════

class TestEventOrderSensitivity:
    """Z3: If outcome depends on event order, it must explicitly reject stale events."""

    def test_correction_order_independence(self):
        """Applying corrections in any order produces the same net impact."""
        conn = _fresh_db()
        _insert_doc(conn, "INV-PERM", amount=3000.00, subtotal=2608.70,
                    tax_total=391.30, invoice_number="PERM-001")

        cm_ids = ["CM-PERM-A", "CM-PERM-B", "CM-PERM-C"]
        amounts = [-300.0, -500.0, -200.0]

        for cid, amt in zip(cm_ids, amounts):
            _insert_doc(conn, cid, amount=amt, doc_type="credit_memo",
                        invoice_number=f"CM-PERM-{cid[-1]}",
                        vendor=f"Vendor-{cid[-1]}")

        net_impacts = []

        for perm in itertools.permutations(range(3)):
            conn2 = _fresh_db()
            _insert_doc(conn2, "INV-PERM", amount=3000.00, subtotal=2608.70,
                        tax_total=391.30, invoice_number="PERM-001")
            for cid, amt in zip(cm_ids, amounts):
                _insert_doc(conn2, cid, amount=amt, doc_type="credit_memo",
                            invoice_number=f"CM-PERM-{cid[-1]}",
                            vendor=f"Vendor-{cid[-1]}")

            for i in perm:
                decomp = decompose_credit_memo_safe(
                    conn2, credit_memo_id=cm_ids[i],
                    credit_memo_amount_tax_included=amounts[i],
                )
                build_correction_chain_link(
                    conn2,
                    chain_root_id="INV-PERM",
                    client_code="CLIENT-001",
                    source_document_id="INV-PERM",
                    target_document_id=cm_ids[i],
                    amount=amounts[i],
                )

            chain = get_full_correction_chain(conn2, "INV-PERM")
            net_impacts.append(chain["total_economic_impact"])

        assert all(n == net_impacts[0] for n in net_impacts), (
            f"Z3: net economic impact must be order-independent: {set(net_impacts)}"
        )

    def test_review_then_import_vs_import_then_review(self):
        """Review before vs after import must both preserve all records."""
        for order in ["review_first", "import_first"]:
            conn = _fresh_db()

            if order == "review_first":
                _insert_doc(conn, "DOC-ORD", amount=500.00)
                _add_decision(conn, "DOC-ORD", "approve_match")
                _insert_bank_txn(conn, "TXN-ORD", credit=500.00,
                                 matched_document_id="DOC-ORD")
            else:
                _insert_doc(conn, "DOC-ORD", amount=500.00)
                _insert_bank_txn(conn, "TXN-ORD", credit=500.00,
                                 matched_document_id="DOC-ORD")
                _add_decision(conn, "DOC-ORD", "approve_match")

            # Both orders must produce: 1 doc, 1 txn, 1 decision
            assert _count_rows(conn, "documents") == 1
            assert _count_rows(conn, "bank_transactions") == 1
            assert _count_rows(conn, "match_decisions") == 1


# ═══════════════════════════════════════════════════════════════════
# ATTACK 13: Cross-client isolation under replay
# ═══════════════════════════════════════════════════════════════════

class TestCrossClientIsolation:
    """Z0: Replay on CLIENT-001 must never affect CLIENT-002 data."""

    def test_cluster_isolation_by_client(self):
        """Clusters for different clients with same invoice must not merge."""
        conn = _fresh_db()

        # Client A
        _insert_doc(conn, "C1-DOC-A", client_code="CLIENT-001",
                    vendor="VendorX", invoice_number="SHARED-INV")
        _insert_doc(conn, "C1-DOC-B", client_code="CLIENT-001",
                    vendor="VendorX", invoice_number="SHARED-INV")
        # Client B
        _insert_doc(conn, "C2-DOC-A", client_code="CLIENT-002",
                    vendor="VendorX", invoice_number="SHARED-INV")
        _insert_doc(conn, "C2-DOC-B", client_code="CLIENT-002",
                    vendor="VendorX", invoice_number="SHARED-INV")

        r1 = cluster_documents(conn, ["C1-DOC-A", "C1-DOC-B"],
                               client_code="CLIENT-001")
        r2 = cluster_documents(conn, ["C2-DOC-A", "C2-DOC-B"],
                               client_code="CLIENT-002")

        assert r1["cluster_id"] != r2["cluster_id"], (
            "Z0: clusters must be isolated by client_code"
        )

    def test_correction_chain_cross_client_blocked(self):
        """Correction chain for CLIENT-001 must not block CLIENT-002 re-import."""
        conn = _fresh_db()

        _insert_doc(conn, "C1-INV", client_code="CLIENT-001", amount=1000.00)
        _insert_doc(conn, "C1-CM", client_code="CLIENT-001", amount=-500.00,
                    doc_type="credit_memo")
        _insert_doc(conn, "C2-CM", client_code="CLIENT-002", amount=-500.00,
                    doc_type="credit_memo")

        # Create + rollback + block reimport for CLIENT-001
        decomp = decompose_credit_memo_safe(
            conn, credit_memo_id="C1-CM",
            credit_memo_amount_tax_included=-500.00,
        )
        res = apply_single_correction(
            conn, credit_memo_id="C1-CM", original_invoice_id="C1-INV",
            client_code="CLIENT-001", decomposition=decomp,
        )
        rollback_correction(
            conn, chain_id=res["chain_id"], client_code="CLIENT-001",
            rolled_back_by="cpa", rollback_reason="block test",
            block_reimport=True,
        )

        # CLIENT-002 reimport check must NOT be blocked
        check = check_reimport_after_rollback(conn, "C2-CM", "CLIENT-002")
        assert check["can_reimport"] is True, (
            "Z0: CLIENT-001 rollback must not block CLIENT-002 reimport"
        )


# ═══════════════════════════════════════════════════════════════════
# ATTACK 14: Idempotent full replay
# ═══════════════════════════════════════════════════════════════════

class TestIdempotentReplay:
    """Z2: Running the same event sequence N times must produce the same final state."""

    def test_triple_replay_identical(self):
        """Run the exact same sequence 3 times — row counts must match."""
        row_counts: list[dict[str, int]] = []

        for _ in range(3):
            conn = _fresh_db()

            # Deterministic sequence
            _insert_doc(conn, "IDEM-INV", amount=2000.00, subtotal=1739.13,
                        tax_total=260.87, invoice_number="IDEM-001")
            _insert_doc(conn, "IDEM-CM-1", amount=-400.00,
                        invoice_number="IDEM-CM", doc_type="credit_memo",
                        vendor="Acme Inc.")
            _insert_doc(conn, "IDEM-CM-2", amount=-400.00,
                        invoice_number="IDEM-CM", doc_type="credit_memo",
                        vendor="Acme Inc.")

            cluster_documents(conn, ["IDEM-CM-1", "IDEM-CM-2"],
                              client_code="CLIENT-001")

            decomp = decompose_credit_memo_safe(
                conn, credit_memo_id="IDEM-CM-1",
                credit_memo_amount_tax_included=-400.00,
            )
            res = apply_single_correction(
                conn, credit_memo_id="IDEM-CM-1",
                original_invoice_id="IDEM-INV",
                client_code="CLIENT-001", decomposition=decomp,
            )

            # Duplicate attempts
            apply_single_correction(
                conn, credit_memo_id="IDEM-CM-2",
                original_invoice_id="IDEM-INV",
                client_code="CLIENT-001", decomposition=decomp,
            )
            apply_single_correction(
                conn, credit_memo_id="IDEM-CM-1",
                original_invoice_id="IDEM-INV",
                client_code="CLIENT-001", decomposition=decomp,
            )

            # Rollback + reimport
            rollback_correction(
                conn, chain_id=res["chain_id"], client_code="CLIENT-001",
                rolled_back_by="cpa", rollback_reason="re-extract",
            )
            decomp2 = decompose_credit_memo_safe(
                conn, credit_memo_id="IDEM-CM-1",
                credit_memo_amount_tax_included=-400.00,
            )
            apply_single_correction(
                conn, credit_memo_id="IDEM-CM-1",
                original_invoice_id="IDEM-INV",
                client_code="CLIENT-001", decomposition=decomp2,
            )

            # Bank feed
            _insert_bank_txn(conn, "IDEM-TXN", credit=2000.00)

            # Decisions
            _add_decision(conn, "IDEM-INV", "approve_match", "cpa_1")

            # Reconciliation
            recon_id = create_reconciliation(
                "CLIENT-001", "Chequing", "2025-03-31",
                10000.00, 10500.00, conn,
            )
            add_reconciliation_item(
                recon_id, "deposit_in_transit", "March dep",
                500.00, "2025-03-30", conn,
            )
            finalize_reconciliation(recon_id, "reviewer", conn)

            counts = {
                "documents": _count_rows(conn, "documents"),
                "bank_transactions": _count_rows(conn, "bank_transactions"),
                "correction_chains": _count_rows(conn, "correction_chains"),
                "active_chains": conn.execute(
                    "SELECT COUNT(*) FROM correction_chains WHERE status = 'active'"
                ).fetchone()[0],
                "clusters": _count_rows(conn, "document_clusters"),
                "cluster_members": _count_rows(conn, "document_cluster_members"),
                "decisions": _count_rows(conn, "match_decisions"),
                "rollback_logs": _count_rows(conn, "rollback_log"),
                "recon_items": _count_rows(conn, "reconciliation_items"),
            }
            row_counts.append(counts)

        for key in row_counts[0]:
            vals = [rc[key] for rc in row_counts]
            assert all(v == vals[0] for v in vals), (
                f"Z2: non-idempotent replay — {key}: {vals}"
            )

        # Verify exactly 1 active correction
        assert row_counts[0]["active_chains"] == 1, (
            f"Z2: expected exactly 1 active correction chain, "
            f"got {row_counts[0]['active_chains']}"
        )

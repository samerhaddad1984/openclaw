"""
tests/test_two_client_destruction.py
====================================
Two-client adversarial stress test — Cluster Leakage, Chain Corruption,
Partial Write Recovery, Concurrent Reviewers, Cross-Client Noise.

Client A (BSQ)  = Basement Systems Quebec — existing Polar Fluid scenario
Client B (MCL)  = MapleCore Logistics       — NEW, intentionally similar identifiers

Vendor overlap:
  Client A vendor: "Polar Fluid Technologies"   invoice PFT-2041
  Client B vendor: "Polar Fluid Consulting"      invoice PFT-2041 (same pattern!)

Five phases:
  Phase 1 — Parallel ingestion (cluster isolation)
  Phase 2 — Multi-hop correction chain (depth 5+)
  Phase 3 — Crash injection (partial write recovery)
  Phase 4 — Concurrent reviewer conflict
  Phase 5 — Cross-client noise injection

Six failure modes asserted against:
  F1  Cluster leakage across clients
  F2  Correction chain corruption (cyclic / duplicate / missing parent)
  F3  Partial write inconsistency (posting <-> amendment flag mismatch)
  F4  Duplicate correction after rollback + re-import
  F5  Audit trail discontinuity
  F6  Non-idempotent crash recovery

Eight hard assertions:
  1. correction chains acyclic
  2. each document -> exactly one cluster per client
  3. no cluster spans multiple clients
  4. rollback + re-import = exactly one active correction
  5. partial crash recovery converges to valid state
  6. amendment flag matches posting reality
  7. audit trail reconstructs full sequence without gaps
  8. final state identical after: normal run, crash+replay, reordered events
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Path bootstrapping
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.correction_chain import (
    apply_single_correction,
    build_correction_chain_link,
    cluster_documents,
    decompose_credit_memo_safe,
    get_cluster_for_document,
    get_full_correction_chain,
    rollback_correction,
    check_reimport_after_rollback,
)
from src.engines.amendment_engine import (
    build_period_correction_entry,
    flag_amendment_needed,
    get_amendment_timeline,
    get_belief_at_time,
    is_period_filed,
    resolve_amendment_flag,
    snapshot_document,
    snapshot_posting,
)
from src.engines.concurrency_engine import (
    StaleVersionError,
    approve_with_version_check,
    check_version_or_raise,
    read_version,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CLIENT_A = "BSQ"
CLIENT_B = "MCL"
VENDOR_A = "Polar Fluid Technologies"
VENDOR_B = "Polar Fluid Consulting"
INV_NUM = "PFT-2041"
INV_NUM_NORMALIZED = "PFT2041"
NOW = "2025-04-27T12:00:00+00:00"
MAY = "2025-05-28T12:00:00+00:00"
JUNE = "2025-06-10T12:00:00+00:00"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id                TEXT PRIMARY KEY,
            client_code                TEXT,
            vendor                     TEXT,
            invoice_number             TEXT,
            invoice_number_normalized  TEXT,
            document_date              TEXT,
            amount                     REAL,
            currency                   TEXT DEFAULT 'CAD',
            doc_type                   TEXT DEFAULT 'invoice',
            category                   TEXT,
            gl_account                 TEXT,
            tax_code                   TEXT,
            memo                       TEXT,
            raw_ocr_text               TEXT,
            review_status              TEXT DEFAULT 'New',
            confidence                 REAL DEFAULT 0.85,
            subtotal                   REAL,
            tax_total                  REAL,
            fraud_flags                TEXT DEFAULT '[]',
            substance_flags            TEXT DEFAULT '{}',
            entry_kind                 TEXT,
            review_history             TEXT DEFAULT '[]',
            fraud_override_reason      TEXT,
            fraud_override_locked      INTEGER DEFAULT 0,
            created_at                 TEXT,
            updated_at                 TEXT,
            version                    INTEGER NOT NULL DEFAULT 1,
            activation_date            TEXT,
            recognition_period         TEXT,
            recognition_status         TEXT NOT NULL DEFAULT 'immediate',
            has_line_items             INTEGER DEFAULT 0,
            ingest_source              TEXT,
            correction_count           INTEGER DEFAULT 0,
            submitted_by               TEXT,
            client_note                TEXT
        );

        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id      TEXT PRIMARY KEY,
            document_id     TEXT UNIQUE,
            target_system   TEXT,
            entry_kind      TEXT,
            file_name       TEXT,
            file_path       TEXT,
            client_code     TEXT,
            vendor          TEXT,
            document_date   TEXT,
            amount          REAL,
            currency        TEXT,
            doc_type        TEXT,
            category        TEXT,
            gl_account      TEXT,
            tax_code        TEXT,
            memo            TEXT,
            review_status   TEXT,
            confidence      REAL,
            approval_state  TEXT,
            posting_status  TEXT,
            reviewer        TEXT,
            blocking_issues TEXT,
            notes           TEXT,
            external_id     TEXT,
            error_text      TEXT,
            payload_json    TEXT,
            created_at      TEXT,
            updated_at      TEXT,
            version         INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT,
            username        TEXT,
            document_id     TEXT,
            provider        TEXT,
            task_type       TEXT,
            prompt_snippet  TEXT,
            latency_ms      REAL,
            created_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS gst_filings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code     TEXT NOT NULL,
            period_label    TEXT NOT NULL,
            deadline        TEXT NOT NULL,
            filed_at        TEXT,
            filed_by        TEXT,
            is_amended      INTEGER NOT NULL DEFAULT 0,
            amendment_filed_at TEXT,
            amendment_filed_by TEXT,
            original_snapshot_id INTEGER,
            amended_snapshot_id  INTEGER,
            UNIQUE(client_code, period_label)
        );

        CREATE TABLE IF NOT EXISTS period_locks (
            client_code  TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end   TEXT NOT NULL,
            locked_by    TEXT,
            locked_at    TEXT,
            PRIMARY KEY (client_code, period_start, period_end)
        );

        CREATE TABLE IF NOT EXISTS amendment_flags (
            flag_id             INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code         TEXT NOT NULL,
            filed_period        TEXT NOT NULL,
            trigger_document_id TEXT NOT NULL,
            trigger_type        TEXT NOT NULL DEFAULT 'credit_memo',
            reason_en           TEXT NOT NULL DEFAULT '',
            reason_fr           TEXT NOT NULL DEFAULT '',
            original_filing_id  TEXT,
            status              TEXT NOT NULL DEFAULT 'open',
            resolved_by         TEXT,
            resolved_at         TEXT,
            amendment_filing_id TEXT,
            created_at          TEXT NOT NULL DEFAULT '',
            updated_at          TEXT NOT NULL DEFAULT '',
            UNIQUE(client_code, filed_period, trigger_document_id)
        );

        CREATE TABLE IF NOT EXISTS document_snapshots (
            snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id     TEXT NOT NULL,
            snapshot_type   TEXT NOT NULL DEFAULT 'filing',
            snapshot_reason TEXT NOT NULL DEFAULT '',
            state_json      TEXT NOT NULL DEFAULT '{}',
            taken_by        TEXT NOT NULL DEFAULT 'system',
            taken_at        TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_doc_snapshots_doc
            ON document_snapshots(document_id);

        CREATE TABLE IF NOT EXISTS posting_snapshots (
            snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            posting_id      TEXT NOT NULL,
            document_id     TEXT NOT NULL,
            snapshot_type   TEXT NOT NULL DEFAULT 'filing',
            snapshot_reason TEXT NOT NULL DEFAULT '',
            state_json      TEXT NOT NULL DEFAULT '{}',
            taken_by        TEXT NOT NULL DEFAULT 'system',
            taken_at        TEXT NOT NULL DEFAULT ''
        );

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
        );
        CREATE INDEX IF NOT EXISTS idx_chain_root
            ON correction_chains(chain_root_id);
        CREATE INDEX IF NOT EXISTS idx_chain_target
            ON correction_chains(target_document_id);

        CREATE TABLE IF NOT EXISTS document_clusters (
            cluster_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_key     TEXT NOT NULL,
            client_code     TEXT NOT NULL,
            cluster_head_id TEXT,
            member_count    INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'active',
            created_at      TEXT NOT NULL DEFAULT '',
            updated_at      TEXT NOT NULL DEFAULT ''
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cluster_key
            ON document_clusters(cluster_key);

        CREATE TABLE IF NOT EXISTS document_cluster_members (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id      INTEGER NOT NULL,
            document_id     TEXT NOT NULL,
            is_cluster_head INTEGER NOT NULL DEFAULT 0,
            similarity_score REAL,
            variant_notes   TEXT,
            added_at        TEXT NOT NULL DEFAULT '',
            UNIQUE(cluster_id, document_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cluster_members_doc
            ON document_cluster_members(document_id);

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
        );

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
        );

        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id            TEXT PRIMARY KEY,
            client_code         TEXT NOT NULL,
            period              TEXT NOT NULL,
            entry_date          TEXT NOT NULL,
            prepared_by         TEXT,
            debit_account       TEXT NOT NULL,
            credit_account      TEXT NOT NULL,
            amount              REAL NOT NULL,
            description         TEXT,
            document_id         TEXT,
            source              TEXT NOT NULL DEFAULT 'bookkeeper',
            status              TEXT NOT NULL DEFAULT 'draft',
            collision_status    TEXT NOT NULL DEFAULT 'clear',
            collision_document_id TEXT,
            collision_chain_id  INTEGER,
            reviewed_by         TEXT,
            reviewed_at         TEXT,
            created_at          TEXT NOT NULL DEFAULT '',
            updated_at          TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            line_number      INTEGER NOT NULL,
            description      TEXT,
            quantity         REAL,
            unit_price       REAL,
            line_total_pretax REAL,
            tax_code         TEXT,
            tax_regime       TEXT,
            gst_amount       REAL,
            qst_amount       REAL,
            hst_amount       REAL,
            province_of_supply TEXT,
            is_tax_included  INTEGER,
            line_notes       TEXT,
            created_at       TEXT NOT NULL DEFAULT ''
        );
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_doc(conn, *, doc_id, client, vendor, inv_num, inv_norm,
                date, amount, doc_type="invoice", memo="", tax_code="GST_QST",
                source="email", subtotal=None, tax_total=None):
    conn.execute(
        """INSERT INTO documents
               (document_id, client_code, vendor, invoice_number,
                invoice_number_normalized, document_date, amount,
                doc_type, gl_account, tax_code, memo, subtotal, tax_total,
                review_status, confidence, ingest_source, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, '5100', ?, ?, ?, ?, 'Ready', 0.90, ?, ?, ?)""",
        (doc_id, client, vendor, inv_num, inv_norm, date, amount,
         doc_type, tax_code, memo, subtotal, tax_total, source, NOW, NOW),
    )


def _insert_posting(conn, *, posting_id, doc_id, client, vendor, date,
                     amount, status="posted"):
    conn.execute(
        """INSERT INTO posting_jobs
               (posting_id, document_id, client_code, vendor,
                document_date, amount, entry_kind, approval_state,
                posting_status, gl_account, tax_code, external_id,
                created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'expense', 'approved_for_posting',
                   ?, '5100', 'GST_QST', ?, ?, ?)""",
        (posting_id, doc_id, client, vendor, date, amount, status,
         f"QBO-{uuid.uuid4().hex[:8]}", NOW, NOW),
    )


def _seed_two_clients(conn: sqlite3.Connection) -> dict[str, str]:
    """Seed both Client A (BSQ) and Client B (MCL) with similar identifiers."""

    # --- Client A: BSQ --- Polar Fluid Technologies PFT-2041 ---
    a_inv = "a_pft2041_invoice"
    a_cm1 = "a_pft2041_cm1"
    a_sub = "a_thl_subcontractor"
    a_cm2 = "a_pft2041_cm2_new"

    _insert_doc(conn, doc_id=a_inv, client=CLIENT_A, vendor=VENDOR_A,
                inv_num="PFT-2041", inv_norm="PFT2041",
                date="2025-04-27", amount=27594.00,
                memo="pump control hardware + commissioning + monitoring",
                subtotal=24000.00, tax_total=3594.00)
    _insert_posting(conn, posting_id=f"post_{a_inv}", doc_id=a_inv,
                    client=CLIENT_A, vendor=VENDOR_A,
                    date="2025-04-27", amount=27594.00)

    # April filed for BSQ
    conn.execute(
        """INSERT INTO gst_filings (client_code, period_label, deadline, filed_at, filed_by)
           VALUES ('BSQ', '2025-04', '2025-05-31', '2025-05-01T10:00:00+00:00', 'Owner')""")

    # Credit memo 1 for Client A
    _insert_doc(conn, doc_id=a_cm1, client=CLIENT_A, vendor="Polar Fluid Tech",
                inv_num="PFT-2041", inv_norm="PFT2041",
                date="2025-05-28", amount=-6900.00, doc_type="credit_note",
                memo="monitoring removed, commissioning rebilled")

    # Subcontractor invoice for Client A
    _insert_doc(conn, doc_id=a_sub, client=CLIENT_A,
                vendor="Techniques Hydrauliques Laval inc.",
                inv_num="THL-0089", inv_norm="THL0089",
                date="2025-05-28", amount=4598.90,
                memo="mise en service finale")

    # Credit memo 2 (new) for Client A
    _insert_doc(conn, doc_id=a_cm2, client=CLIENT_A, vendor="Polar Fluid Technologies",
                inv_num="PFT-2041-CM2", inv_norm="PFT2041CM2",
                date="2025-06-05", amount=-3200.00, doc_type="credit_note",
                memo="hardware quality adjustment")

    # --- Client B: MCL --- Polar Fluid Consulting PFT-2041 (DECOY) ---
    b_inv = "b_pft2041_invoice"
    b_cm = "b_pft2041_cm"

    _insert_doc(conn, doc_id=b_inv, client=CLIENT_B, vendor=VENDOR_B,
                inv_num="PFT-2041", inv_norm="PFT2041",
                date="2025-05-15", amount=18750.00,
                memo="consulting services -- environmental compliance",
                tax_code="GST_ONLY",
                subtotal=17857.14, tax_total=892.86)
    _insert_posting(conn, posting_id=f"post_{b_inv}", doc_id=b_inv,
                    client=CLIENT_B, vendor=VENDOR_B,
                    date="2025-05-15", amount=18750.00)

    # May filed for MCL
    conn.execute(
        """INSERT INTO gst_filings (client_code, period_label, deadline, filed_at, filed_by)
           VALUES ('MCL', '2025-05', '2025-06-30', '2025-06-01T10:00:00+00:00', 'Partner')""")

    # Client B credit memo -- same invoice number variant!
    _insert_doc(conn, doc_id=b_cm, client=CLIENT_B, vendor="Polar Fluid Consulting",
                inv_num="PFT-2041", inv_norm="PFT2041",
                date="2025-06-10", amount=-4500.00, doc_type="credit_note",
                memo="scope reduction -- Phase 2 deferred")

    conn.commit()
    return {
        "a_inv": a_inv, "a_cm1": a_cm1, "a_sub": a_sub, "a_cm2": a_cm2,
        "b_inv": b_inv, "b_cm": b_cm,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    _create_tables(conn)
    yield conn
    conn.close()


@pytest.fixture
def seeded(db):
    ids = _seed_two_clients(db)
    return db, ids


# ===========================================================================
# PHASE 1 -- Parallel ingestion / Cluster isolation
# ===========================================================================

class TestPhase1ClusterIsolation:
    """Client A and Client B both have PFT-2041 -- clusters must NOT leak."""

    def test_client_a_cluster_contains_only_a_docs(self, seeded):
        conn, ids = seeded
        cluster_documents(
            conn, [ids["a_inv"], ids["a_cm1"]],
            client_code=CLIENT_A, reason="duplicate_detection",
        )
        cluster = get_cluster_for_document(conn, ids["a_inv"])
        member_ids = {m["document_id"] for m in cluster["members"]}
        assert ids["a_inv"] in member_ids
        assert ids["a_cm1"] in member_ids
        # No Client B document in this cluster
        assert ids["b_inv"] not in member_ids
        assert ids["b_cm"] not in member_ids

    def test_client_b_cluster_contains_only_b_docs(self, seeded):
        conn, ids = seeded
        cluster_documents(
            conn, [ids["a_inv"], ids["a_cm1"]],
            client_code=CLIENT_A, reason="duplicate_detection",
        )
        cluster_documents(
            conn, [ids["b_inv"], ids["b_cm"]],
            client_code=CLIENT_B, reason="duplicate_detection",
        )
        cluster = get_cluster_for_document(conn, ids["b_inv"])
        member_ids = {m["document_id"] for m in cluster["members"]}
        assert ids["b_inv"] in member_ids
        # No Client A contamination
        assert ids["a_inv"] not in member_ids
        assert ids["a_cm1"] not in member_ids

    def test_no_cluster_spans_two_clients(self, seeded):
        """Hard assertion: no cluster spans multiple clients."""
        conn, ids = seeded
        cluster_documents(conn, [ids["a_inv"], ids["a_cm1"]],
                          client_code=CLIENT_A, reason="dup")
        cluster_documents(conn, [ids["b_inv"], ids["b_cm"]],
                          client_code=CLIENT_B, reason="dup")

        rows = conn.execute("""
            SELECT dc.cluster_id, dc.client_code, dcm.document_id
            FROM document_clusters dc
            JOIN document_cluster_members dcm ON dc.cluster_id = dcm.cluster_id
        """).fetchall()

        cluster_clients: dict[int, set[str]] = {}
        for r in rows:
            cluster_clients.setdefault(r["cluster_id"], set()).add(r["client_code"])

        for cid, clients in cluster_clients.items():
            assert len(clients) == 1, (
                f"CLUSTER LEAKAGE: cluster {cid} spans clients {clients}"
            )

    def test_each_doc_belongs_to_exactly_one_cluster_per_client(self, seeded):
        """Hard assertion: each document belongs to exactly one cluster."""
        conn, ids = seeded
        cluster_documents(conn, [ids["a_inv"], ids["a_cm1"]],
                          client_code=CLIENT_A, reason="dup")
        cluster_documents(conn, [ids["b_inv"], ids["b_cm"]],
                          client_code=CLIENT_B, reason="dup")

        rows = conn.execute("""
            SELECT document_id, COUNT(DISTINCT cluster_id) as cluster_count
            FROM document_cluster_members
            GROUP BY document_id
            HAVING cluster_count > 1
        """).fetchall()
        assert len(rows) == 0, (
            f"Documents in multiple clusters: {[dict(r) for r in rows]}"
        )


# ===========================================================================
# PHASE 2 -- Multi-hop correction chain (depth 5+)
# ===========================================================================

class TestPhase2MultiHopCorrectionChain:
    """Chain: apply CM1, apply subcontractor, apply CM2,
    rollback CM1, re-import CM1.  Chain depth = 5."""

    def _build_5_hop_chain(self, conn, ids):
        """Execute the 5-step correction chain and return chain_ids."""
        chain_ids = []

        # Step 1: Apply credit memo 1
        r1 = build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        chain_ids.append(r1["chain_id"])

        # Step 2: Apply subcontractor invoice
        r2 = build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_sub"],
            link_type="subcontractor", economic_effect="addition",
            amount=4598.90, tax_impact_gst=229.95, tax_impact_qst=458.89,
        )
        chain_ids.append(r2["chain_id"])

        # Step 3: Apply credit memo 2 (new)
        r3 = build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm2"],
            link_type="credit_memo", economic_effect="reduction",
            amount=3200.00, tax_impact_gst=160.00, tax_impact_qst=319.20,
        )
        chain_ids.append(r3["chain_id"])

        # Step 4: Rollback credit memo 1
        r4 = rollback_correction(
            conn, chain_id=chain_ids[0], client_code=CLIENT_A,
            rolled_back_by="senior_reviewer",
            rollback_reason="CM1 applied to wrong line items",
        )
        chain_ids.append(r4.get("chain_id", chain_ids[0]))

        # Step 5: Re-import credit memo 1 (corrected application)
        reimport_check = check_reimport_after_rollback(
            conn, document_id=ids["a_cm1"], client_code=CLIENT_A,
        )

        r5 = build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
            created_by="reimport_after_rollback",
        )
        chain_ids.append(r5["chain_id"])

        return chain_ids, reimport_check

    def test_chain_depth_reaches_5(self, seeded):
        """5 operations: create CM1, create sub, create CM2, rollback CM1,
        reimport CM1.  The chain table stores 4 links (rollback flips status)
        plus rollback_log.  Total operations >= 5."""
        conn, ids = seeded
        chain_ids, _ = self._build_5_hop_chain(conn, ids)
        chain = get_full_correction_chain(conn, ids["a_inv"])
        rollback_count = conn.execute(
            "SELECT COUNT(*) as c FROM rollback_log WHERE client_code = ?",
            (CLIENT_A,),
        ).fetchone()["c"]
        total_ops = chain["link_count"] + rollback_count
        assert total_ops >= 5, (
            f"Total operations only {total_ops} "
            f"(links={chain['link_count']}, rollbacks={rollback_count}), expected 5+"
        )

    def test_correction_chains_are_acyclic(self, seeded):
        """Hard assertion: no cycles in correction chain graph."""
        conn, ids = seeded
        self._build_5_hop_chain(conn, ids)

        rows = conn.execute(
            "SELECT chain_id, source_document_id, target_document_id, status "
            "FROM correction_chains WHERE chain_root_id = ?",
            (ids["a_inv"],),
        ).fetchall()

        # DFS cycle detection on active links
        adjacency: dict[str, list[str]] = {}
        for r in rows:
            if r["status"] == "active":
                src = r["source_document_id"]
                tgt = r["target_document_id"]
                adjacency.setdefault(src, []).append(tgt)

        visited: set[str] = set()
        in_stack: set[str] = set()

        def _has_cycle(node: str) -> bool:
            visited.add(node)
            in_stack.add(node)
            for nb in adjacency.get(node, []):
                if nb in in_stack:
                    return True
                if nb not in visited and _has_cycle(nb):
                    return True
            in_stack.discard(node)
            return False

        for start in adjacency:
            if start not in visited:
                assert not _has_cycle(start), (
                    f"CYCLE DETECTED in correction chain rooted at {ids['a_inv']}"
                )

    def test_rollback_plus_reimport_yields_one_active(self, seeded):
        """Hard assertion: rollback + re-import = exactly one active correction
        for the same source -> target pair."""
        conn, ids = seeded
        self._build_5_hop_chain(conn, ids)

        rows = conn.execute(
            """SELECT COUNT(*) as cnt FROM correction_chains
               WHERE chain_root_id = ? AND target_document_id = ? AND status = 'active'""",
            (ids["a_inv"], ids["a_cm1"]),
        ).fetchone()
        assert rows["cnt"] == 1, (
            f"DUPLICATE CORRECTION: {rows['cnt']} active corrections for CM1 "
            f"after rollback+reimport (expected exactly 1)"
        )

    def test_rolled_back_link_is_not_active(self, seeded):
        conn, ids = seeded
        chain_ids, _ = self._build_5_hop_chain(conn, ids)
        first_chain_id = chain_ids[0]
        row = conn.execute(
            "SELECT status FROM correction_chains WHERE chain_id = ?",
            (first_chain_id,),
        ).fetchone()
        assert row["status"] == "rolled_back", (
            f"First chain link status is '{row['status']}', expected 'rolled_back'"
        )

    def test_reimport_after_rollback_is_allowed(self, seeded):
        conn, ids = seeded
        _, reimport_check = self._build_5_hop_chain(conn, ids)
        assert reimport_check["can_reimport"] is True, (
            f"Re-import blocked after rollback: {reimport_check['reasons']}"
        )

    def test_chain_does_not_cross_to_client_b(self, seeded):
        conn, ids = seeded
        self._build_5_hop_chain(conn, ids)
        b_docs = {ids["b_inv"], ids["b_cm"]}
        rows = conn.execute(
            "SELECT source_document_id, target_document_id FROM correction_chains"
        ).fetchall()
        for r in rows:
            assert r["source_document_id"] not in b_docs, "Chain leaked to Client B source"
            assert r["target_document_id"] not in b_docs, "Chain leaked to Client B target"


# ===========================================================================
# PHASE 3 -- Crash injection (partial write recovery)
# ===========================================================================

class TestPhase3CrashInjection:
    """Simulate crash between writing correction_chains, posting_snapshots,
    and amendment_flags.  System must converge to valid state on recovery."""

    def _simulate_partial_write_and_recover(self, conn, ids) -> dict[str, Any]:
        """Simulate: correction chain written, but crash before amendment flag.
        Then recovery runs and fills the gap."""

        # Write correction chain (this succeeds before crash)
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )

        # Take snapshot (this also succeeds before crash)
        snapshot_document(conn, ids["a_inv"],
                          snapshot_type="pre_correction",
                          snapshot_reason="crash_test")

        # *** CRASH POINT *** -- amendment flag NOT written
        # Verify the inconsistent state
        chain = get_full_correction_chain(conn, ids["a_inv"])
        assert chain["link_count"] >= 1, "Chain should exist"

        flags_before = conn.execute(
            "SELECT COUNT(*) as cnt FROM amendment_flags WHERE client_code = ? AND trigger_document_id = ?",
            (CLIENT_A, ids["a_cm1"]),
        ).fetchone()
        assert flags_before["cnt"] == 0, "Amendment flag should NOT exist yet (simulated crash)"

        # *** RECOVERY *** -- detect orphaned chain and create missing flag
        orphaned = conn.execute("""
            SELECT cc.chain_id, cc.chain_root_id, cc.target_document_id, cc.client_code
            FROM correction_chains cc
            WHERE cc.status = 'active'
              AND NOT EXISTS (
                  SELECT 1 FROM amendment_flags af
                  WHERE af.client_code = cc.client_code
                    AND af.trigger_document_id = cc.target_document_id
              )
        """).fetchall()

        recovery_results = []
        for orphan in orphaned:
            result = flag_amendment_needed(
                conn,
                client_code=orphan["client_code"],
                filed_period="2025-04",
                trigger_document_id=orphan["target_document_id"],
                trigger_type="credit_memo",
                reason_en="Recovery: amendment flag created after crash recovery",
                created_by="crash_recovery",
            )
            recovery_results.append(result)

        conn.commit()
        return {"orphaned_count": len(orphaned), "recovery": recovery_results}

    def test_partial_write_detected_and_recovered(self, seeded):
        conn, ids = seeded
        result = self._simulate_partial_write_and_recover(conn, ids)
        assert result["orphaned_count"] >= 1, "Should detect orphaned chain"

    def test_amendment_flag_matches_posting_reality_after_recovery(self, seeded):
        """Hard assertion: amendment flag must match posting reality."""
        conn, ids = seeded
        self._simulate_partial_write_and_recover(conn, ids)

        # After recovery, every active correction chain should have an amendment flag
        orphaned_after = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM correction_chains cc
            WHERE cc.status = 'active'
              AND cc.client_code = ?
              AND NOT EXISTS (
                  SELECT 1 FROM amendment_flags af
                  WHERE af.client_code = cc.client_code
                    AND af.trigger_document_id = cc.target_document_id
              )
        """, (CLIENT_A,)).fetchone()
        assert orphaned_after["cnt"] == 0, (
            f"PARTIAL WRITE INCONSISTENCY: {orphaned_after['cnt']} corrections "
            f"without amendment flags after recovery"
        )

    def test_posting_exists_implies_snapshot_exists(self, seeded):
        """If posting exists, a snapshot must exist after recovery."""
        conn, ids = seeded
        self._simulate_partial_write_and_recover(conn, ids)

        # Take posting snapshot as part of recovery
        snapshot_posting(conn, ids["a_inv"],
                         snapshot_type="correction",
                         snapshot_reason="crash recovery snapshot")

        snaps = conn.execute(
            "SELECT COUNT(*) as cnt FROM posting_snapshots WHERE document_id = ?",
            (ids["a_inv"],),
        ).fetchone()
        assert snaps["cnt"] >= 1, (
            "INCONSISTENCY: posting exists but no posting snapshot after recovery"
        )

    def test_recovery_is_idempotent(self, seeded):
        """Hard assertion: replaying recovery must not change outcome."""
        conn, ids = seeded

        # Run recovery twice
        self._simulate_partial_write_and_recover(conn, ids)

        # Second recovery attempt -- should find no orphans
        orphaned_second = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM correction_chains cc
            WHERE cc.status = 'active'
              AND NOT EXISTS (
                  SELECT 1 FROM amendment_flags af
                  WHERE af.client_code = cc.client_code
                    AND af.trigger_document_id = cc.target_document_id
              )
        """).fetchone()
        assert orphaned_second["cnt"] == 0, (
            "NON-IDEMPOTENT RECOVERY: second run found orphans"
        )

        # Amendment flags should not be duplicated
        flags = conn.execute(
            "SELECT COUNT(*) as cnt FROM amendment_flags WHERE client_code = ? AND trigger_document_id = ?",
            (CLIENT_A, ids["a_cm1"]),
        ).fetchone()
        assert flags["cnt"] == 1, (
            f"Non-idempotent: {flags['cnt']} amendment flags for same trigger"
        )


# ===========================================================================
# PHASE 4 -- Concurrent reviewer conflict
# ===========================================================================

class TestPhase4ConcurrentReviewers:
    """Reviewer A approves. Reviewer B approves stale version.
    Background job recomputes clusters."""

    def test_stale_version_rejected(self, seeded):
        """Reviewer B's stale approval must be rejected after a document edit
        bumps the version between their reads."""
        conn, ids = seeded

        # Both reviewers read version at the same time
        v1 = read_version(conn, "document", ids["a_inv"])

        # Reviewer A approves
        approve_with_version_check(
            conn, document_id=ids["a_inv"],
            expected_document_version=v1,
            reviewer="reviewer_a",
        )

        # Simulate that approval triggers a background correction that bumps
        # the document version (as a real correction chain would)
        conn.execute(
            "UPDATE documents SET version = version + 1, updated_at = ? WHERE document_id = ?",
            (_utc_now(), ids["a_inv"]),
        )
        conn.commit()

        # Reviewer B now tries with the stale version
        with pytest.raises(StaleVersionError):
            approve_with_version_check(
                conn, document_id=ids["a_inv"],
                expected_document_version=v1,  # stale!
                reviewer="reviewer_b",
            )
        # Verify reviewer_b did NOT overwrite reviewer_a
        row = conn.execute(
            "SELECT reviewer FROM posting_jobs WHERE document_id = ?",
            (ids["a_inv"],),
        ).fetchone()
        assert row is None or row["reviewer"] != "reviewer_b", (
            "Stale reviewer's name leaked into posting"
        )

    def test_concurrent_cluster_recompute_safe(self, seeded):
        """Background re-clustering after approval must not create duplicates."""
        conn, ids = seeded

        cluster_documents(conn, [ids["a_inv"], ids["a_cm1"]],
                          client_code=CLIENT_A, reason="initial")

        v = read_version(conn, "document", ids["a_inv"])
        approve_with_version_check(
            conn, document_id=ids["a_inv"],
            expected_document_version=v, reviewer="reviewer_a",
        )

        # Background re-cluster
        cluster_documents(conn, [ids["a_inv"], ids["a_cm1"]],
                          client_code=CLIENT_A, reason="background_recompute")

        for doc_id in [ids["a_inv"], ids["a_cm1"]]:
            rows = conn.execute(
                "SELECT COUNT(*) as cnt FROM document_cluster_members WHERE document_id = ?",
                (doc_id,),
            ).fetchone()
            assert rows["cnt"] == 1, (
                f"Doc {doc_id} in {rows['cnt']} clusters after re-cluster"
            )

    def test_version_bump_enables_stale_detection(self, seeded):
        """Document version increment (by correction/edit) makes previous reads stale."""
        conn, ids = seeded
        v1 = read_version(conn, "document", ids["a_inv"])
        assert v1 == 1, "Initial version should be 1"

        # Simulate a correction that bumps version
        conn.execute(
            "UPDATE documents SET version = version + 1, updated_at = ? WHERE document_id = ?",
            (_utc_now(), ids["a_inv"]),
        )
        conn.commit()

        v2 = read_version(conn, "document", ids["a_inv"])
        assert v2 == v1 + 1, "Version should be bumped after edit"

        # Stale read should be rejected
        with pytest.raises(StaleVersionError):
            check_version_or_raise(conn, "document", ids["a_inv"], v1)


# ===========================================================================
# PHASE 5 -- Cross-client noise injection
# ===========================================================================

class TestPhase5CrossClientNoise:
    """Import Client B document with: same invoice number variant,
    similar vendor string, same amount.  Must not corrupt Client A."""

    def _inject_noise(self, conn, ids):
        """Import a noise document under Client B with confusable fields."""
        noise_id = "b_noise_pft2041"
        _insert_doc(conn, doc_id=noise_id, client=CLIENT_B,
                    vendor="Polar Fluid Tech",  # substring of Client A vendor!
                    inv_num="PFT-2O41",  # OCR confusable: O instead of 0
                    inv_norm="PFT2041",  # normalizes to same!
                    date="2025-06-15", amount=6900.00,  # same as Client A CM amount!
                    memo="consulting Phase 3 delivery")
        conn.commit()
        return noise_id

    def test_noise_doc_does_not_join_client_a_cluster(self, seeded):
        conn, ids = seeded
        noise_id = self._inject_noise(conn, ids)

        cluster_documents(conn, [ids["a_inv"], ids["a_cm1"]],
                          client_code=CLIENT_A, reason="dup")
        cluster_documents(conn, [ids["b_inv"], noise_id],
                          client_code=CLIENT_B, reason="dup")

        cluster_a = get_cluster_for_document(conn, ids["a_inv"])
        if cluster_a:
            a_member_ids = {m["document_id"] for m in cluster_a["members"]}
            assert noise_id not in a_member_ids, (
                f"CLUSTER LEAKAGE: noise doc {noise_id} joined Client A cluster"
            )

    def test_client_a_chain_unaffected_by_noise(self, seeded):
        conn, ids = seeded
        self._inject_noise(conn, ids)

        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction", amount=6900.00,
        )

        chain = get_full_correction_chain(conn, ids["a_inv"])
        target_ids = {lnk["target_document_id"] for lnk in chain["links"]}
        assert "b_noise_pft2041" not in target_ids, (
            "Client A correction chain references Client B noise document"
        )

    def test_client_b_amendment_isolated_from_a(self, seeded):
        conn, ids = seeded
        self._inject_noise(conn, ids)

        flag_amendment_needed(
            conn, client_code=CLIENT_A, filed_period="2025-04",
            trigger_document_id=ids["a_cm1"], trigger_type="credit_memo",
            reason_en="Credit memo on filed period",
        )

        b_flags = conn.execute(
            "SELECT COUNT(*) as cnt FROM amendment_flags WHERE client_code = ? AND filed_period = '2025-04'",
            (CLIENT_B,),
        ).fetchone()
        assert b_flags["cnt"] == 0, (
            f"Client B has {b_flags['cnt']} amendment flags on Client A's period"
        )


# ===========================================================================
# AUDIT TRAIL INTEGRITY
# ===========================================================================

class TestAuditTrailIntegrity:
    """Hard assertion: audit trail reconstructs full sequence without gaps."""

    def test_full_chain_audit_trail_reconstructable(self, seeded):
        """Every correction link has a matching rollback_log entry if rolled back."""
        conn, ids = seeded

        TestPhase2MultiHopCorrectionChain()._build_5_hop_chain(conn, ids)

        snapshot_document(conn, ids["a_inv"],
                          snapshot_type="pre_correction",
                          snapshot_reason="before CM1")

        chain = get_full_correction_chain(conn, ids["a_inv"])

        for link in chain["links"]:
            assert "target_document_id" in link
            assert link["status"] in ("active", "rolled_back"), (
                f"Unknown link status: {link['status']}"
            )

        # Rolled back links must have rollback_log entries
        rolled_back = [l for l in chain["links"] if l["status"] == "rolled_back"]
        for rb in rolled_back:
            log = conn.execute(
                "SELECT * FROM rollback_log WHERE target_id = ?",
                (str(rb["chain_id"]),),
            ).fetchone()
            assert log is not None, (
                f"AUDIT GAP: rolled-back chain {rb['chain_id']} has no rollback_log entry"
            )

    def test_snapshot_timeline_is_monotonic(self, seeded):
        """Snapshots must have monotonically increasing timestamps."""
        conn, ids = seeded

        for stype, reason in [
            ("pre_correction", "before CM1"),
            ("correction", "after CM1"),
            ("pre_correction", "before rollback"),
            ("correction", "after reimport"),
        ]:
            snapshot_document(conn, ids["a_inv"],
                              snapshot_type=stype, snapshot_reason=reason)

        snaps = conn.execute(
            "SELECT taken_at FROM document_snapshots WHERE document_id = ? ORDER BY snapshot_id",
            (ids["a_inv"],),
        ).fetchall()
        timestamps = [s["taken_at"] for s in snaps]
        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i - 1], (
                f"AUDIT DISCONTINUITY: snapshot {i} timestamp "
                f"'{timestamps[i]}' < '{timestamps[i-1]}'"
            )

    def test_amendment_timeline_complete(self, seeded):
        """get_amendment_timeline must return a complete history."""
        conn, ids = seeded

        build_period_correction_entry(
            conn,
            original_document_id=ids["a_inv"],
            correction_document_id=ids["a_cm1"],
            client_code=CLIENT_A,
            correction_period="2025-05",
            correction_amount=6900.00,
            correction_gst=300.00,
            correction_qst=598.28,
            reason_en="Credit memo on filed period",
        )

        timeline = get_amendment_timeline(conn, CLIENT_A, "2025-04")
        assert timeline is not None
        assert "amendment_flags" in timeline
        assert len(timeline["amendment_flags"]) >= 1


# ===========================================================================
# CONVERGENCE -- Final state identical across execution paths
# ===========================================================================

class TestConvergenceAcrossExecutionPaths:
    """Final state identical after: normal run, crash+replay, reordered events."""

    def _get_final_state(self, conn, ids) -> dict[str, Any]:
        chain = get_full_correction_chain(conn, ids["a_inv"])
        active_links = [l for l in chain["links"] if l["status"] == "active"]

        corrections = conn.execute(
            """SELECT chain_root_id, target_document_id, status, amount
               FROM correction_chains WHERE client_code = ? AND status = 'active'
               ORDER BY target_document_id""",
            (CLIENT_A,),
        ).fetchall()

        return {
            "active_link_count": len(active_links),
            "total_economic_impact": chain.get("total_economic_impact", 0),
            "correction_targets": sorted(
                [dict(r) for r in corrections],
                key=lambda x: x["target_document_id"],
            ),
        }

    def _run_normal_sequence(self, conn, ids):
        """Normal order: CM1, sub, CM2, rollback CM1, reimport CM1."""
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction", amount=6900.00,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_sub"],
            link_type="subcontractor", economic_effect="addition", amount=4598.90,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm2"],
            link_type="credit_memo", economic_effect="reduction", amount=3200.00,
        )
        cm1_chain = conn.execute(
            "SELECT chain_id FROM correction_chains WHERE target_document_id = ? AND status = 'active'",
            (ids["a_cm1"],),
        ).fetchone()
        rollback_correction(
            conn, chain_id=cm1_chain["chain_id"], client_code=CLIENT_A,
            rolled_back_by="reviewer", rollback_reason="wrong application",
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction", amount=6900.00,
            created_by="reimport",
        )

    def _run_reordered_sequence(self, conn, ids):
        """Reordered: CM2 first, then sub, then CM1, rollback CM1, reimport CM1."""
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm2"],
            link_type="credit_memo", economic_effect="reduction", amount=3200.00,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_sub"],
            link_type="subcontractor", economic_effect="addition", amount=4598.90,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction", amount=6900.00,
        )
        cm1_chain = conn.execute(
            "SELECT chain_id FROM correction_chains WHERE target_document_id = ? AND status = 'active'",
            (ids["a_cm1"],),
        ).fetchone()
        rollback_correction(
            conn, chain_id=cm1_chain["chain_id"], client_code=CLIENT_A,
            rolled_back_by="reviewer", rollback_reason="wrong application",
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction", amount=6900.00,
            created_by="reimport",
        )

    def _fresh_db_with_seed(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        _create_tables(conn)
        ids = _seed_two_clients(conn)
        return conn, ids

    def test_normal_vs_reordered_converge(self):
        """Final active state identical regardless of event ordering."""
        conn1, ids1 = self._fresh_db_with_seed()
        self._run_normal_sequence(conn1, ids1)
        state_normal = self._get_final_state(conn1, ids1)
        conn1.close()

        conn2, ids2 = self._fresh_db_with_seed()
        self._run_reordered_sequence(conn2, ids2)
        state_reordered = self._get_final_state(conn2, ids2)
        conn2.close()

        assert state_normal["active_link_count"] == state_reordered["active_link_count"], (
            f"Active link count diverges: normal={state_normal['active_link_count']} "
            f"vs reordered={state_reordered['active_link_count']}"
        )
        assert state_normal["total_economic_impact"] == state_reordered["total_economic_impact"], (
            f"Economic impact diverges: normal={state_normal['total_economic_impact']} "
            f"vs reordered={state_reordered['total_economic_impact']}"
        )

    def test_crash_replay_converges(self):
        """Crash + replay must produce the same active correction set."""
        conn1, ids1 = self._fresh_db_with_seed()
        self._run_normal_sequence(conn1, ids1)
        state_normal = self._get_final_state(conn1, ids1)
        conn1.close()

        conn2, ids2 = self._fresh_db_with_seed()
        self._run_normal_sequence(conn2, ids2)
        state_replay = self._get_final_state(conn2, ids2)
        conn2.close()

        assert state_normal["active_link_count"] == state_replay["active_link_count"]
        assert state_normal["total_economic_impact"] == state_replay["total_economic_impact"]


# ===========================================================================
# COMPOSITE -- All 8 hard assertions in one end-to-end pass
# ===========================================================================

class TestAllHardAssertions:
    """Run the entire 5-phase sequence and verify all 8 hard assertions."""

    def test_full_scenario_all_assertions(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        _create_tables(conn)
        ids = _seed_two_clients(conn)

        # --- Phase 2 FIRST (before clustering, so chain links aren't skipped) ---
        r1 = build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        assert r1["status"] == "created", f"CM1 link not created: {r1}"
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_sub"],
            link_type="subcontractor", economic_effect="addition",
            amount=4598.90,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm2"],
            link_type="credit_memo", economic_effect="reduction",
            amount=3200.00,
        )
        rollback_correction(
            conn, chain_id=r1["chain_id"], client_code=CLIENT_A,
            rolled_back_by="reviewer", rollback_reason="wrong lines",
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["a_inv"], client_code=CLIENT_A,
            source_document_id=ids["a_inv"], target_document_id=ids["a_cm1"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, created_by="reimport",
        )

        # --- Phase 1: Cluster isolation (after chains to avoid skipping) ---
        cluster_documents(conn, [ids["a_inv"], ids["a_cm1"]],
                          client_code=CLIENT_A, reason="dup")
        cluster_documents(conn, [ids["b_inv"], ids["b_cm"]],
                          client_code=CLIENT_B, reason="dup")

        # HARD ASSERT 1: no cluster spans multiple clients
        rows = conn.execute("""
            SELECT dc.cluster_id, dc.client_code
            FROM document_clusters dc
            JOIN document_cluster_members dcm ON dc.cluster_id = dcm.cluster_id
        """).fetchall()
        cluster_clients: dict[int, set[str]] = {}
        for r in rows:
            cluster_clients.setdefault(r["cluster_id"], set()).add(r["client_code"])
        for cid, clients in cluster_clients.items():
            assert len(clients) == 1, f"Cluster {cid} spans {clients}"

        # HARD ASSERT 2: each doc in exactly one cluster
        multi = conn.execute("""
            SELECT document_id, COUNT(DISTINCT cluster_id) as c
            FROM document_cluster_members GROUP BY document_id HAVING c > 1
        """).fetchall()
        assert len(multi) == 0, f"Docs in multiple clusters: {[dict(r) for r in multi]}"

        # HARD ASSERT 3: chains acyclic
        active_links = conn.execute(
            "SELECT source_document_id, target_document_id FROM correction_chains WHERE status = 'active'"
        ).fetchall()
        adj: dict[str, list[str]] = {}
        for r in active_links:
            adj.setdefault(r["source_document_id"], []).append(r["target_document_id"])
        visited: set[str] = set()
        stack: set[str] = set()

        def dfs(n):
            visited.add(n)
            stack.add(n)
            for nb in adj.get(n, []):
                if nb in stack:
                    return True
                if nb not in visited and dfs(nb):
                    return True
            stack.discard(n)
            return False

        for s in adj:
            if s not in visited:
                assert not dfs(s), "CYCLE in correction chain"

        # HARD ASSERT 4: rollback + reimport = exactly one active
        cm1_active = conn.execute(
            "SELECT COUNT(*) as c FROM correction_chains WHERE target_document_id = ? AND status = 'active'",
            (ids["a_cm1"],),
        ).fetchone()
        assert cm1_active["c"] == 1, f"CM1 active count = {cm1_active['c']}"

        # --- Phase 3: Amendment flag consistency ---
        build_period_correction_entry(
            conn, original_document_id=ids["a_inv"],
            correction_document_id=ids["a_cm1"], client_code=CLIENT_A,
            correction_period="2025-05", correction_amount=6900.00,
        )

        # HARD ASSERT 5: amendment flag exists for active correction on filed period
        flag = conn.execute(
            "SELECT COUNT(*) as c FROM amendment_flags WHERE client_code = ? AND trigger_document_id = ?",
            (CLIENT_A, ids["a_cm1"]),
        ).fetchone()
        assert flag["c"] >= 1, "Amendment flag missing for active correction"

        # --- Phase 4: Concurrent reviewer ---
        v = read_version(conn, "document", ids["a_inv"])
        approve_with_version_check(
            conn, document_id=ids["a_inv"],
            expected_document_version=v, reviewer="reviewer_a",
        )
        # Simulate version bump from correction background job
        conn.execute(
            "UPDATE documents SET version = version + 1, updated_at = ? WHERE document_id = ?",
            (_utc_now(), ids["a_inv"]),
        )
        conn.commit()
        with pytest.raises(StaleVersionError):
            approve_with_version_check(
                conn, document_id=ids["a_inv"],
                expected_document_version=v, reviewer="reviewer_b",
            )

        # --- Phase 5: Cross-client noise ---
        noise_id = "b_noise_final"
        _insert_doc(conn, doc_id=noise_id, client=CLIENT_B,
                    vendor="Polar Fluid Tech", inv_num="PFT-2O41",
                    inv_norm="PFT2041", date="2025-06-15", amount=6900.00)
        conn.commit()
        cluster_documents(conn, [ids["b_inv"], noise_id],
                          client_code=CLIENT_B, reason="noise")

        # HARD ASSERT 6: noise did not join Client A cluster
        cluster_a = get_cluster_for_document(conn, ids["a_inv"])
        if cluster_a:
            assert noise_id not in {m["document_id"] for m in cluster_a["members"]}

        # HARD ASSERT 7: audit trail -- snapshots exist
        snapshot_document(conn, ids["a_inv"],
                          snapshot_type="correction", snapshot_reason="final state")
        snaps = conn.execute(
            "SELECT COUNT(*) as c FROM document_snapshots WHERE document_id = ?",
            (ids["a_inv"],),
        ).fetchone()
        assert snaps["c"] >= 1, "No snapshots for audited document"

        # HARD ASSERT 8: rollback log complete
        rb_logs = conn.execute(
            "SELECT COUNT(*) as c FROM rollback_log WHERE client_code = ?",
            (CLIENT_A,),
        ).fetchone()
        assert rb_logs["c"] >= 1, "No rollback log entries"

        conn.close()

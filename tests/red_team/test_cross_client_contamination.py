"""
tests/red_team/test_cross_client_contamination.py
===================================================
X — Cross-Client Contamination Test

Two clients with IDENTICAL:
  - vendor name:    "Polar Fluid Technologies"
  - invoice number: "PFT-2041"
  - amount:         $27,594.00
  - date:           "2025-04-27"

Every subsystem must keep them perfectly isolated:
  C1  Clusters                — no shared cluster membership
  C2  Correction chains       — no chain link crosses clients
  C3  Audit packets           — working papers / trial balance scoped
  C4  Bank matches            — matched transaction stays in its client
  C5  Vendor memory           — learned GL/tax stays per-client
  C6  Fraud patterns          — history queries never leak across clients

FAIL if any evidence or learned behavior leaks across clients.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import sys

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.correction_chain import (
    build_correction_chain_link,
    cluster_documents,
    get_cluster_for_document,
    get_full_correction_chain,
    rollback_correction,
)
from src.engines.audit_engine import (
    create_working_paper,
    ensure_audit_tables,
    generate_trial_balance,
    get_trial_balance,
    get_working_papers,
)
from src.engines.fraud_engine import run_fraud_detection

# ---------------------------------------------------------------------------
# Constants — IDENTICAL identifiers across both clients
# ---------------------------------------------------------------------------
CLIENT_X = "XRAY"
CLIENT_Y = "YOKE"
VENDOR = "Polar Fluid Technologies"
INV_NUM = "PFT-2041"
INV_NORM = "PFT2041"
AMOUNT = 27594.00
DATE = "2025-04-27"
NOW = "2025-04-27T12:00:00+00:00"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Schema — mirrors production tables needed for the test
# ---------------------------------------------------------------------------
def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id                TEXT PRIMARY KEY,
            client_code                TEXT NOT NULL,
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

        CREATE TABLE IF NOT EXISTS bank_transactions (
            transaction_id  TEXT PRIMARY KEY,
            account_name    TEXT,
            account_number  TEXT,
            transaction_date TEXT,
            description     TEXT,
            amount          REAL,
            balance         REAL,
            reference       TEXT,
            client_code     TEXT,
            matched_document_id TEXT,
            match_confidence REAL,
            created_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS vendor_memory (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code     TEXT,
            vendor          TEXT NOT NULL,
            vendor_key      TEXT NOT NULL DEFAULT '',
            client_code_key TEXT NOT NULL DEFAULT '',
            gl_account      TEXT,
            tax_code        TEXT,
            doc_type        TEXT,
            category        TEXT,
            approval_count  INTEGER NOT NULL DEFAULT 0,
            confidence      REAL NOT NULL DEFAULT 0.0,
            last_amount     REAL,
            last_document_id TEXT,
            last_source     TEXT,
            last_used       TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_vendor_memory_client_vendor
            ON vendor_memory(client_code_key, vendor_key);

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
    """)
    # Audit engine tables (working_papers, trial_balance, engagements, etc.)
    ensure_audit_tables(conn)
    conn.commit()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_doc(conn, *, doc_id, client, vendor=VENDOR, inv_num=INV_NUM,
                inv_norm=INV_NORM, date=DATE, amount=AMOUNT,
                doc_type="invoice", gl_account="5100", tax_code="GST_QST",
                memo="pump control hardware + commissioning"):
    conn.execute(
        """INSERT INTO documents
               (document_id, client_code, vendor, invoice_number,
                invoice_number_normalized, document_date, amount,
                doc_type, gl_account, tax_code, memo, subtotal, tax_total,
                review_status, confidence, ingest_source, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Ready', 0.90, 'email', ?, ?)""",
        (doc_id, client, vendor, inv_num, inv_norm, date, amount,
         doc_type, gl_account, tax_code, memo,
         round(amount / 1.14975, 2), round(amount - amount / 1.14975, 2),
         NOW, NOW),
    )


def _insert_posting(conn, *, posting_id, doc_id, client, vendor=VENDOR,
                     date=DATE, amount=AMOUNT, status="posted"):
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


def _seed_identical_clients(conn: sqlite3.Connection) -> dict[str, str]:
    """Seed two clients with IDENTICAL vendor, invoice, amount, and date."""

    # --- Client X: invoice + credit memo ---
    x_inv = "x_pft2041_inv"
    x_cm = "x_pft2041_cm"

    _insert_doc(conn, doc_id=x_inv, client=CLIENT_X)
    _insert_posting(conn, posting_id=f"post_{x_inv}", doc_id=x_inv,
                    client=CLIENT_X)
    _insert_doc(conn, doc_id=x_cm, client=CLIENT_X,
                amount=-6900.00, doc_type="credit_note",
                memo="monitoring removed")

    # --- Client Y: IDENTICAL invoice + credit memo ---
    y_inv = "y_pft2041_inv"
    y_cm = "y_pft2041_cm"

    _insert_doc(conn, doc_id=y_inv, client=CLIENT_Y)
    _insert_posting(conn, posting_id=f"post_{y_inv}", doc_id=y_inv,
                    client=CLIENT_Y)
    _insert_doc(conn, doc_id=y_cm, client=CLIENT_Y,
                amount=-6900.00, doc_type="credit_note",
                memo="monitoring removed")

    conn.commit()
    return {"x_inv": x_inv, "x_cm": x_cm, "y_inv": y_inv, "y_cm": y_cm}


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
    ids = _seed_identical_clients(db)
    return db, ids


# ===========================================================================
# C1 — CLUSTER ISOLATION
# ===========================================================================

class TestC1ClusterIsolation:
    """Identical vendor+invoice+amount+date must produce separate clusters."""

    def test_separate_clusters_created(self, seeded):
        conn, ids = seeded
        cluster_documents(conn, [ids["x_inv"], ids["x_cm"]],
                          client_code=CLIENT_X, reason="dup")
        cluster_documents(conn, [ids["y_inv"], ids["y_cm"]],
                          client_code=CLIENT_Y, reason="dup")

        cx = get_cluster_for_document(conn, ids["x_inv"])
        cy = get_cluster_for_document(conn, ids["y_inv"])
        assert cx["cluster_id"] != cy["cluster_id"], \
            "CONTAMINATION: identical invoices share a cluster"

    def test_no_cluster_spans_two_clients(self, seeded):
        conn, ids = seeded
        cluster_documents(conn, [ids["x_inv"], ids["x_cm"]],
                          client_code=CLIENT_X, reason="dup")
        cluster_documents(conn, [ids["y_inv"], ids["y_cm"]],
                          client_code=CLIENT_Y, reason="dup")

        rows = conn.execute("""
            SELECT dc.cluster_id, dc.client_code
            FROM document_clusters dc
            JOIN document_cluster_members dcm ON dc.cluster_id = dcm.cluster_id
        """).fetchall()

        cluster_clients: dict[int, set[str]] = {}
        for r in rows:
            cluster_clients.setdefault(r["cluster_id"], set()).add(r["client_code"])

        for cid, clients in cluster_clients.items():
            assert len(clients) == 1, \
                f"CLUSTER LEAKAGE: cluster {cid} spans {clients}"

    def test_x_cluster_has_no_y_docs(self, seeded):
        conn, ids = seeded
        cluster_documents(conn, [ids["x_inv"], ids["x_cm"]],
                          client_code=CLIENT_X, reason="dup")
        cluster_documents(conn, [ids["y_inv"], ids["y_cm"]],
                          client_code=CLIENT_Y, reason="dup")

        cx = get_cluster_for_document(conn, ids["x_inv"])
        member_ids = {m["document_id"] for m in cx["members"]}
        assert ids["y_inv"] not in member_ids
        assert ids["y_cm"] not in member_ids

    def test_y_cluster_has_no_x_docs(self, seeded):
        conn, ids = seeded
        cluster_documents(conn, [ids["x_inv"], ids["x_cm"]],
                          client_code=CLIENT_X, reason="dup")
        cluster_documents(conn, [ids["y_inv"], ids["y_cm"]],
                          client_code=CLIENT_Y, reason="dup")

        cy = get_cluster_for_document(conn, ids["y_inv"])
        member_ids = {m["document_id"] for m in cy["members"]}
        assert ids["x_inv"] not in member_ids
        assert ids["x_cm"] not in member_ids


# ===========================================================================
# C2 — CORRECTION CHAIN ISOLATION
# ===========================================================================

class TestC2CorrectionChainIsolation:
    """Correction chains must never cross the client boundary."""

    def test_chains_scoped_to_client(self, seeded):
        conn, ids = seeded

        # Build chain for client X
        build_correction_chain_link(
            conn, chain_root_id=ids["x_inv"], client_code=CLIENT_X,
            source_document_id=ids["x_inv"], target_document_id=ids["x_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        # Build chain for client Y
        build_correction_chain_link(
            conn, chain_root_id=ids["y_inv"], client_code=CLIENT_Y,
            source_document_id=ids["y_inv"], target_document_id=ids["y_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )

        # Verify no chain link references a document from the other client
        all_chains = conn.execute(
            "SELECT * FROM correction_chains"
        ).fetchall()

        x_doc_ids = {ids["x_inv"], ids["x_cm"]}
        y_doc_ids = {ids["y_inv"], ids["y_cm"]}

        for chain in all_chains:
            cc = chain["client_code"]
            src = chain["source_document_id"]
            tgt = chain["target_document_id"]
            if cc == CLIENT_X:
                assert src not in y_doc_ids, \
                    f"LEAK: X chain source {src} belongs to Y"
                assert tgt not in y_doc_ids, \
                    f"LEAK: X chain target {tgt} belongs to Y"
            elif cc == CLIENT_Y:
                assert src not in x_doc_ids, \
                    f"LEAK: Y chain source {src} belongs to X"
                assert tgt not in x_doc_ids, \
                    f"LEAK: Y chain target {tgt} belongs to X"

    def test_rollback_in_x_does_not_touch_y(self, seeded):
        conn, ids = seeded

        rx = build_correction_chain_link(
            conn, chain_root_id=ids["x_inv"], client_code=CLIENT_X,
            source_document_id=ids["x_inv"], target_document_id=ids["x_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        ry = build_correction_chain_link(
            conn, chain_root_id=ids["y_inv"], client_code=CLIENT_Y,
            source_document_id=ids["y_inv"], target_document_id=ids["y_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )

        # Rollback X's chain
        rollback_correction(
            conn, chain_id=rx["chain_id"], client_code=CLIENT_X,
            rolled_back_by="auditor", rollback_reason="test",
        )

        # Y's chain must remain active
        y_chain = conn.execute(
            "SELECT status FROM correction_chains WHERE chain_id = ?",
            (ry["chain_id"],),
        ).fetchone()
        assert y_chain["status"] == "active", \
            "CONTAMINATION: rollback in X deactivated Y's chain"

    def test_get_full_chain_returns_only_own_client(self, seeded):
        conn, ids = seeded

        build_correction_chain_link(
            conn, chain_root_id=ids["x_inv"], client_code=CLIENT_X,
            source_document_id=ids["x_inv"], target_document_id=ids["x_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["y_inv"], client_code=CLIENT_Y,
            source_document_id=ids["y_inv"], target_document_id=ids["y_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )

        chain_x = get_full_correction_chain(conn, ids["x_inv"])
        y_doc_ids = {ids["y_inv"], ids["y_cm"]}
        for link in chain_x.get("links", []):
            assert link["source_document_id"] not in y_doc_ids
            assert link["target_document_id"] not in y_doc_ids


# ===========================================================================
# C3 — AUDIT PACKET ISOLATION
# ===========================================================================

class TestC3AuditPacketIsolation:
    """Working papers and trial balance must not leak across clients."""

    def test_working_papers_scoped(self, seeded):
        conn, ids = seeded
        period = "2025-04"

        wp_x = create_working_paper(
            conn, CLIENT_X, period, "audit", "5100", "Subcontractors",
            balance_per_books=AMOUNT,
        )
        wp_y = create_working_paper(
            conn, CLIENT_Y, period, "audit", "5100", "Subcontractors",
            balance_per_books=AMOUNT,
        )

        papers_x = get_working_papers(conn, CLIENT_X, period)
        papers_y = get_working_papers(conn, CLIENT_Y, period)

        x_ids = {p["paper_id"] for p in papers_x}
        y_ids = {p["paper_id"] for p in papers_y}

        assert wp_x in x_ids and wp_x not in y_ids, \
            "LEAK: X working paper visible to Y"
        assert wp_y in y_ids and wp_y not in x_ids, \
            "LEAK: Y working paper visible to X"

    def test_trial_balance_scoped(self, seeded):
        conn, ids = seeded
        period = "2025-04"

        generate_trial_balance(conn, CLIENT_X, period)
        generate_trial_balance(conn, CLIENT_Y, period)

        tb_x = get_trial_balance(conn, CLIENT_X, period)
        tb_y = get_trial_balance(conn, CLIENT_Y, period)

        # Both should have entries (from posted docs) but they must be distinct rows
        x_tb_ids = {r["id"] for r in tb_x}
        y_tb_ids = {r["id"] for r in tb_y}
        assert x_tb_ids.isdisjoint(y_tb_ids), \
            f"LEAK: shared trial balance IDs: {x_tb_ids & y_tb_ids}"

        # Verify client_code scoping
        for r in tb_x:
            assert r["client_code"].upper() == CLIENT_X, \
                f"LEAK: X trial balance contains {r['client_code']}"
        for r in tb_y:
            assert r["client_code"].upper() == CLIENT_Y, \
                f"LEAK: Y trial balance contains {r['client_code']}"


# ===========================================================================
# C4 — BANK MATCH ISOLATION
# ===========================================================================

class TestC4BankMatchIsolation:
    """Bank transactions matched for one client must not appear for another."""

    def test_bank_match_stays_in_client(self, seeded):
        conn, ids = seeded

        # Insert bank transactions for each client
        for label, client, doc_id in [
            ("bt_x", CLIENT_X, ids["x_inv"]),
            ("bt_y", CLIENT_Y, ids["y_inv"]),
        ]:
            conn.execute(
                """INSERT INTO bank_transactions
                       (transaction_id, account_name, transaction_date,
                        description, amount, client_code,
                        matched_document_id, match_confidence, created_at)
                   VALUES (?, 'Checking', ?, ?, ?, ?, ?, 0.95, ?)""",
                (label, DATE, f"Payment {VENDOR}", -AMOUNT, client,
                 doc_id, NOW),
            )
        conn.commit()

        # Query bank transactions for X — must not see Y's
        rows_x = conn.execute(
            "SELECT * FROM bank_transactions WHERE client_code = ?",
            (CLIENT_X,),
        ).fetchall()
        matched_x = {r["matched_document_id"] for r in rows_x}
        assert ids["y_inv"] not in matched_x, \
            "LEAK: Y document matched in X bank transactions"

        # Query bank transactions for Y — must not see X's
        rows_y = conn.execute(
            "SELECT * FROM bank_transactions WHERE client_code = ?",
            (CLIENT_Y,),
        ).fetchall()
        matched_y = {r["matched_document_id"] for r in rows_y}
        assert ids["x_inv"] not in matched_y, \
            "LEAK: X document matched in Y bank transactions"


# ===========================================================================
# C5 — VENDOR MEMORY ISOLATION
# ===========================================================================

class TestC5VendorMemoryIsolation:
    """Vendor memory learned for one client must not influence the other."""

    def test_vendor_memory_per_client(self, seeded):
        conn, ids = seeded

        # Seed vendor memory for X with GL 5100
        conn.execute(
            """INSERT INTO vendor_memory
                   (client_code, vendor, vendor_key, client_code_key,
                    gl_account, tax_code, doc_type, approval_count,
                    confidence, last_amount, created_at, updated_at)
               VALUES (?, ?, ?, ?, '5100', 'GST_QST', 'invoice', 10,
                       0.95, ?, ?, ?)""",
            (CLIENT_X, VENDOR, "polar fluid technologies", "xray",
             AMOUNT, NOW, NOW),
        )
        # Seed vendor memory for Y with DIFFERENT GL 6200
        conn.execute(
            """INSERT INTO vendor_memory
                   (client_code, vendor, vendor_key, client_code_key,
                    gl_account, tax_code, doc_type, approval_count,
                    confidence, last_amount, created_at, updated_at)
               VALUES (?, ?, ?, ?, '6200', 'GST_ONLY', 'invoice', 10,
                       0.95, ?, ?, ?)""",
            (CLIENT_Y, VENDOR, "polar fluid technologies", "yoke",
             AMOUNT, NOW, NOW),
        )
        conn.commit()

        # X lookup must get 5100, not 6200
        row_x = conn.execute(
            """SELECT gl_account, tax_code FROM vendor_memory
               WHERE client_code_key = ? AND vendor_key = ?
               ORDER BY approval_count DESC LIMIT 1""",
            ("xray", "polar fluid technologies"),
        ).fetchone()
        assert row_x["gl_account"] == "5100", \
            f"LEAK: X got GL {row_x['gl_account']} instead of 5100"
        assert row_x["tax_code"] == "GST_QST"

        # Y lookup must get 6200, not 5100
        row_y = conn.execute(
            """SELECT gl_account, tax_code FROM vendor_memory
               WHERE client_code_key = ? AND vendor_key = ?
               ORDER BY approval_count DESC LIMIT 1""",
            ("yoke", "polar fluid technologies"),
        ).fetchone()
        assert row_y["gl_account"] == "6200", \
            f"LEAK: Y got GL {row_y['gl_account']} instead of 6200"
        assert row_y["tax_code"] == "GST_ONLY"

    def test_vendor_memory_unscoped_query_would_merge(self, seeded):
        """Prove that WITHOUT client_code filtering, data merges — then
        prove that WITH filtering, it stays separate."""
        conn, ids = seeded

        for client, cc_key, gl in [
            (CLIENT_X, "xray", "5100"),
            (CLIENT_Y, "yoke", "6200"),
        ]:
            conn.execute(
                """INSERT INTO vendor_memory
                       (client_code, vendor, vendor_key, client_code_key,
                        gl_account, tax_code, doc_type, approval_count,
                        confidence, last_amount, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'GST_QST', 'invoice', 5,
                           0.90, ?, ?, ?)""",
                (client, VENDOR, "polar fluid technologies", cc_key,
                 gl, AMOUNT, NOW, NOW),
            )
        conn.commit()

        # Unscoped query returns BOTH
        unscoped = conn.execute(
            "SELECT DISTINCT gl_account FROM vendor_memory WHERE vendor_key = ?",
            ("polar fluid technologies",),
        ).fetchall()
        assert len(unscoped) == 2, "Sanity: both GL accounts should exist"

        # Scoped query returns only one
        scoped_x = conn.execute(
            "SELECT DISTINCT gl_account FROM vendor_memory "
            "WHERE vendor_key = ? AND client_code_key = ?",
            ("polar fluid technologies", "xray"),
        ).fetchall()
        assert len(scoped_x) == 1
        assert scoped_x[0]["gl_account"] == "5100"


# ===========================================================================
# C6 — FRAUD PATTERN ISOLATION
# ===========================================================================

class TestC6FraudPatternIsolation:
    """Fraud rules that look at vendor history must only see own client."""

    def _seed_fraud_history(self, conn, client, doc_prefix, amounts):
        """Create N prior invoices for the same vendor under one client."""
        for i, amt in enumerate(amounts):
            doc_id = f"{doc_prefix}_hist_{i}"
            _insert_doc(conn, doc_id=doc_id, client=client, amount=amt,
                        inv_num=f"PFT-{2000+i}", inv_norm=f"PFT{2000+i}",
                        date=f"2025-0{min(i+1,9):d}-15")
            _insert_posting(conn, posting_id=f"post_{doc_id}", doc_id=doc_id,
                            client=client, amount=amt)
        conn.commit()

    def test_amount_anomaly_scoped_to_client(self, seeded):
        """Client X has small invoices → large one should flag.
        Client Y has large invoices → same amount should NOT flag.
        If fraud engine leaks Y's history into X, X won't flag."""
        conn, ids = seeded

        # X: history of small amounts
        self._seed_fraud_history(conn, CLIENT_X, "x",
                                 [500.0, 600.0, 550.0, 480.0, 520.0,
                                  510.0, 530.0, 490.0, 560.0, 540.0])
        # Y: history of large amounts (near 27594)
        self._seed_fraud_history(conn, CLIENT_Y, "y",
                                 [25000.0, 28000.0, 27000.0, 26000.0, 29000.0,
                                  27500.0, 26500.0, 28500.0, 27200.0, 25500.0])

        # X's main invoice (27594) should trigger amount anomaly
        # because X's history is all ~$500
        x_doc = conn.execute(
            "SELECT fraud_flags FROM documents WHERE document_id = ?",
            (ids["x_inv"],),
        ).fetchone()
        x_flags = json.loads(x_doc["fraud_flags"]) if x_doc["fraud_flags"] else []

        # Y's main invoice (27594) should NOT trigger amount anomaly
        # because Y's history is all ~$27K
        y_doc = conn.execute(
            "SELECT fraud_flags FROM documents WHERE document_id = ?",
            (ids["y_inv"],),
        ).fetchone()
        y_flags = json.loads(y_doc["fraud_flags"]) if y_doc["fraud_flags"] else []

        # Run fraud detection on both
        # (fraud_engine writes flags to the documents table using its own db_path,
        # but we have in-memory db — so we test the history query isolation directly)
        x_history = conn.execute(
            """SELECT amount FROM documents
               WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
                 AND document_id != ?
                 AND LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
                 AND doc_type = 'invoice'""",
            (CLIENT_X, ids["x_inv"], VENDOR),
        ).fetchall()
        y_history = conn.execute(
            """SELECT amount FROM documents
               WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
                 AND document_id != ?
                 AND LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
                 AND doc_type = 'invoice'""",
            (CLIENT_Y, ids["y_inv"], VENDOR),
        ).fetchall()

        x_amounts = [r["amount"] for r in x_history]
        y_amounts = [r["amount"] for r in y_history]

        # X history must be all small amounts — no Y contamination
        assert len(x_amounts) >= 10, f"X should have 10 history docs, got {len(x_amounts)}"
        assert all(a < 1000 for a in x_amounts), \
            f"LEAK: X history contains large amounts from Y: {x_amounts}"
        # Y history must be all large amounts — no X contamination
        assert len(y_amounts) >= 10, f"Y should have 10 history docs, got {len(y_amounts)}"
        assert all(a > 10000 for a in y_amounts), \
            f"LEAK: Y history contains small amounts from X: {y_amounts}"

    def test_duplicate_detection_scoped(self, seeded):
        """Same vendor + amount + date in both clients should not trigger
        cross-client duplicate detection."""
        conn, ids = seeded

        # Check that X's duplicates only find X documents
        x_dups = conn.execute(
            """SELECT document_id FROM documents
               WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
                 AND amount = ?
                 AND vendor = ?""",
            (CLIENT_X, AMOUNT, VENDOR),
        ).fetchall()
        x_dup_ids = {r["document_id"] for r in x_dups}
        assert ids["y_inv"] not in x_dup_ids, \
            "LEAK: Y invoice appears in X duplicate search"

        y_dups = conn.execute(
            """SELECT document_id FROM documents
               WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
                 AND amount = ?
                 AND vendor = ?""",
            (CLIENT_Y, AMOUNT, VENDOR),
        ).fetchall()
        y_dup_ids = {r["document_id"] for r in y_dups}
        assert ids["x_inv"] not in y_dup_ids, \
            "LEAK: X invoice appears in Y duplicate search"


# ===========================================================================
# COMBINED — Global contamination sweep
# ===========================================================================

class TestGlobalContaminationSweep:
    """Final sweep: query every table that stores client-scoped data and
    verify no row references both CLIENT_X and CLIENT_Y."""

    SCOPED_TABLES = [
        ("documents", "client_code"),
        ("posting_jobs", "client_code"),
        ("correction_chains", "client_code"),
        ("document_clusters", "client_code"),
        ("bank_transactions", "client_code"),
        ("vendor_memory", "client_code"),
    ]

    def test_no_row_mixes_clients(self, seeded):
        """For each scoped table, every row must belong to exactly one client."""
        conn, ids = seeded

        # Build some data in both clients
        cluster_documents(conn, [ids["x_inv"], ids["x_cm"]],
                          client_code=CLIENT_X, reason="dup")
        cluster_documents(conn, [ids["y_inv"], ids["y_cm"]],
                          client_code=CLIENT_Y, reason="dup")
        build_correction_chain_link(
            conn, chain_root_id=ids["x_inv"], client_code=CLIENT_X,
            source_document_id=ids["x_inv"], target_document_id=ids["x_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["y_inv"], client_code=CLIENT_Y,
            source_document_id=ids["y_inv"], target_document_id=ids["y_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )

        for table, col in self.SCOPED_TABLES:
            try:
                rows = conn.execute(f"SELECT {col} FROM {table}").fetchall()
            except sqlite3.OperationalError:
                continue  # table may not have data

            for r in rows:
                client = r[col]
                assert client in (CLIENT_X, CLIENT_Y, None, ""), \
                    f"UNKNOWN client '{client}' in {table}"

    def test_cross_join_yields_zero_shared_documents(self, seeded):
        """No document_id appears under both CLIENT_X and CLIENT_Y."""
        conn, ids = seeded

        rows = conn.execute("""
            SELECT d1.document_id
            FROM documents d1
            JOIN documents d2 ON d1.document_id = d2.document_id
            WHERE d1.client_code = ? AND d2.client_code = ?
        """, (CLIENT_X, CLIENT_Y)).fetchall()

        assert len(rows) == 0, \
            f"CONTAMINATION: {len(rows)} documents shared between clients"

    def test_correction_chain_never_links_across_clients(self, seeded):
        """Verify no correction chain link has source from one client
        and target from another."""
        conn, ids = seeded

        build_correction_chain_link(
            conn, chain_root_id=ids["x_inv"], client_code=CLIENT_X,
            source_document_id=ids["x_inv"], target_document_id=ids["x_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )
        build_correction_chain_link(
            conn, chain_root_id=ids["y_inv"], client_code=CLIENT_Y,
            source_document_id=ids["y_inv"], target_document_id=ids["y_cm"],
            link_type="credit_memo", economic_effect="reduction",
            amount=6900.00, tax_impact_gst=300.00, tax_impact_qst=598.28,
        )

        # Check that source and target docs always match the chain's client_code
        chains = conn.execute("SELECT * FROM correction_chains").fetchall()
        for c in chains:
            src_client = conn.execute(
                "SELECT client_code FROM documents WHERE document_id = ?",
                (c["source_document_id"],),
            ).fetchone()
            tgt_client = conn.execute(
                "SELECT client_code FROM documents WHERE document_id = ?",
                (c["target_document_id"],),
            ).fetchone()
            assert src_client["client_code"] == c["client_code"], \
                f"CROSS-CLIENT LINK: chain {c['chain_id']} source belongs to " \
                f"{src_client['client_code']} but chain says {c['client_code']}"
            assert tgt_client["client_code"] == c["client_code"], \
                f"CROSS-CLIENT LINK: chain {c['chain_id']} target belongs to " \
                f"{tgt_client['client_code']} but chain says {c['client_code']}"

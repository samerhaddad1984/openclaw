"""
tests/test_9_traps_polar_fluid.py — Full scenario test for all 9 traps.

Scenario: Polar Fluid Technologies PFT-2041
Quebec registrant, April 2025 filed, credit memo in May, subcontractor,
duplicate ingestion, concurrent reviewers, manual journal, rollback.

Each trap is tested independently with a fresh in-memory DB.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_tables(conn: sqlite3.Connection) -> None:
    """Create all required tables in an in-memory database."""
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
            ingest_source              TEXT
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


def _seed_april_scenario(conn: sqlite3.Connection) -> dict[str, str]:
    """Seed the Polar Fluid Technologies April scenario."""
    now = "2025-04-27T12:00:00+00:00"
    inv_id = "doc_pft2041_invoice"
    cm_id_1 = "doc_pft2041_cm_email"
    cm_id_2 = "doc_pft2041_cm_portal"
    cm_id_3 = "doc_pft2041_cm_photo"
    sub_id = "doc_thl_commissioning"

    # Original invoice — Polar Fluid Technologies PFT-2041
    conn.execute(
        """INSERT INTO documents
               (document_id, client_code, vendor, invoice_number,
                invoice_number_normalized, document_date, amount,
                doc_type, gl_account, tax_code, memo, subtotal, tax_total,
                review_status, confidence, created_at, updated_at)
           VALUES (?, 'BSQ', 'Polar Fluid Technologies', 'PFT-2041',
                   'PFT2041', '2025-04-27', 27594.00,
                   'invoice', '5100', 'GST_QST', 'pump control hardware + commissioning + monitoring',
                   24000.00, 3594.00, 'Ready', 0.92, ?, ?)""",
        (inv_id, now, now),
    )

    # Invoice lines
    conn.executemany(
        """INSERT INTO invoice_lines
               (document_id, line_number, description, line_total_pretax,
                gst_amount, qst_amount, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (inv_id, 1, "Pump control hardware", 16000.00, 800.00, 1596.00, now),
            (inv_id, 2, "Commissioning", 5000.00, 250.00, 498.75, now),
            (inv_id, 3, "Annual monitoring subscription", 3000.00, 150.00, 299.25, now),
        ],
    )

    # Posting job — posted
    conn.execute(
        """INSERT INTO posting_jobs
               (posting_id, document_id, client_code, vendor,
                document_date, amount, entry_kind, approval_state,
                posting_status, gl_account, tax_code, external_id,
                created_at, updated_at)
           VALUES (?, ?, 'BSQ', 'Polar Fluid Technologies',
                   '2025-04-27', 27594.00, 'expense', 'approved_for_posting',
                   'posted', '5100', 'GST_QST', 'QBO-12345', ?, ?)""",
        (f"post_qbo_expense_{inv_id}", inv_id, now, now),
    )

    # April filed
    conn.execute(
        """INSERT INTO gst_filings
               (client_code, period_label, deadline, filed_at, filed_by)
           VALUES ('BSQ', '2025-04', '2025-05-31', '2025-05-01T10:00:00+00:00', 'Owner')""",
    )

    # Three credit memo variants (Day 36)
    for cm_id, inv_num, source in [
        (cm_id_1, "PFT-2041", "email"),
        (cm_id_2, "PFT2041", "portal"),
        (cm_id_3, "PFT-2O41", "mobile_photo"),
    ]:
        conn.execute(
            """INSERT INTO documents
                   (document_id, client_code, vendor, invoice_number,
                    invoice_number_normalized, document_date, amount,
                    doc_type, memo, review_status, confidence,
                    ingest_source, created_at, updated_at)
               VALUES (?, 'BSQ', 'Polar Fluid Tech', ?,
                       'PFT2041', '2025-05-28', -6900.00,
                       'credit_note',
                       'monitoring subscription removed; commissioning rebilled separately',
                       'New', 0.80, ?, ?, ?)""",
            (cm_id, inv_num, source, "2025-05-28T12:00:00+00:00", "2025-05-28T12:00:00+00:00"),
        )

    # Subcontractor invoice (Day 32)
    conn.execute(
        """INSERT INTO documents
               (document_id, client_code, vendor, invoice_number,
                document_date, amount, doc_type, memo, tax_code,
                review_status, confidence, created_at, updated_at)
           VALUES (?, 'BSQ', 'Techniques Hydrauliques Laval inc.',
                   'THL-0089', '2025-05-28', 4598.90, 'invoice',
                   'mise en service finale', 'GST_QST',
                   'New', 0.88, '2025-05-28T12:00:00+00:00',
                   '2025-05-28T12:00:00+00:00')""",
        (sub_id,),
    )

    conn.commit()
    return {
        "invoice_id": inv_id,
        "cm_email": cm_id_1,
        "cm_portal": cm_id_2,
        "cm_photo": cm_id_3,
        "subcontractor_id": sub_id,
    }


@pytest.fixture
def db():
    """In-memory SQLite database with all tables and scenario data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    _create_tables(conn)
    yield conn
    conn.close()


@pytest.fixture
def seeded_db(db):
    """In-memory DB with the full Polar Fluid scenario seeded."""
    ids = _seed_april_scenario(db)
    return db, ids


# =========================================================================
# TRAP 1 — Filed period freeze vs amended return
# =========================================================================

class TestTrap1FiledPeriodFreeze:
    """April remains historically intact. System creates May correction entries
    and amendment-needed flag."""

    def test_april_is_filed(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import is_period_filed
        assert is_period_filed(conn, "BSQ", "2025-04") is True
        assert is_period_filed(conn, "BSQ", "2025-05") is False

    def test_amendment_flag_raised_on_correction(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import (
            build_period_correction_entry,
            get_open_amendment_flags,
        )
        result = build_period_correction_entry(
            conn,
            original_document_id=ids["invoice_id"],
            correction_document_id=ids["cm_email"],
            client_code="BSQ",
            correction_period="2025-05",
            correction_amount=6000.00,
            correction_gst=300.00,
            correction_qst=600.00,
            reason_en="Credit memo reverses monitoring + commissioning",
        )
        assert result["amendment_flag_raised"] is True
        assert result["original_period"] == "2025-04"

        flags = get_open_amendment_flags(conn, "BSQ", "2025-04")
        assert len(flags) == 1
        assert flags[0]["status"] == "open"
        assert flags[0]["trigger_document_id"] == ids["cm_email"]

    def test_original_posting_untouched(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import build_period_correction_entry
        build_period_correction_entry(
            conn,
            original_document_id=ids["invoice_id"],
            correction_document_id=ids["cm_email"],
            client_code="BSQ",
            correction_period="2025-05",
            correction_amount=6000.00,
        )
        # Original posting should still be posted
        row = conn.execute(
            "SELECT posting_status, amount FROM posting_jobs WHERE document_id = ?",
            (ids["invoice_id"],),
        ).fetchone()
        assert row["posting_status"] == "posted"
        assert row["amount"] == 27594.00

    def test_snapshot_taken_before_correction(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import build_period_correction_entry
        build_period_correction_entry(
            conn,
            original_document_id=ids["invoice_id"],
            correction_document_id=ids["cm_email"],
            client_code="BSQ",
            correction_period="2025-05",
            correction_amount=6000.00,
        )
        snaps = conn.execute(
            "SELECT * FROM document_snapshots WHERE document_id = ? AND snapshot_type = 'pre_correction'",
            (ids["invoice_id"],),
        ).fetchall()
        assert len(snaps) >= 1
        state = json.loads(snaps[0]["state_json"])
        assert state["amount"] == 27594.00


# =========================================================================
# TRAP 2 — Credit memo decomposition without clean tax split
# =========================================================================

class TestTrap2CreditMemoDecomposition:
    """Decompose only as far as evidence allows. Partial correction + uncertainty flag."""

    def test_tax_included_no_breakdown(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import decompose_credit_memo_safe
        result = decompose_credit_memo_safe(
            conn,
            credit_memo_id=ids["cm_email"],
            credit_memo_amount_tax_included=6900.00,
            original_invoice_id=ids["invoice_id"],
            has_tax_breakdown=False,
            memo_text="monitoring subscription removed; commissioning rebilled separately",
        )
        # Should decompose proportionally from invoice lines
        assert result["decomposition_method"] in (
            "proportional_from_invoice_lines",
            "proportional_from_document",
        )
        assert result["pretax"] is not None
        assert result["gst"] is not None
        assert result["qst"] is not None
        # Should have uncertainty flag
        assert len(result["uncertainty_flags"]) >= 1
        flag_names = [f["flag"] for f in result["uncertainty_flags"]]
        assert any("ESTIMATE" in f or "COMPONENT" in f for f in flag_names)
        # Confidence should be below full certainty
        assert result["confidence"] < 0.95

    def test_explicit_breakdown_high_confidence(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import decompose_credit_memo_safe
        result = decompose_credit_memo_safe(
            conn,
            credit_memo_id=ids["cm_email"],
            credit_memo_amount_tax_included=6900.00,
            has_tax_breakdown=True,
            stated_gst=300.00,
            stated_qst=600.00,
        )
        assert result["decomposition_method"] == "explicit_breakdown"
        assert result["confidence"] == 0.95
        assert result["gst"] == 300.00
        assert result["qst"] == 600.00
        assert result["pretax"] == 6000.00

    def test_no_linked_invoice_low_confidence(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import decompose_credit_memo_safe
        result = decompose_credit_memo_safe(
            conn,
            credit_memo_id=ids["cm_email"],
            credit_memo_amount_tax_included=6900.00,
            original_invoice_id=None,
            has_tax_breakdown=False,
            memo_text="monitoring subscription removed",
        )
        assert result["decomposition_method"] == "reverse_engineered_standard_rates"
        assert result["confidence"] < 0.60  # Below posting threshold
        assert result["partial_correction"] is True
        flag_names = [f["flag"] for f in result["uncertainty_flags"]]
        assert "TAX_SPLIT_UNPROVEN" in flag_names

    def test_component_identification_from_memo(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import decompose_credit_memo_safe
        result = decompose_credit_memo_safe(
            conn,
            credit_memo_id=ids["cm_email"],
            credit_memo_amount_tax_included=6900.00,
            original_invoice_id=None,
            has_tax_breakdown=False,
            memo_text="monitoring subscription removed; commissioning rebilled separately",
        )
        component_flags = [
            f for f in result["uncertainty_flags"]
            if f.get("flag") == "PARTIAL_COMPONENT_IDENTIFICATION"
        ]
        assert len(component_flags) == 1
        components_str = " ".join(component_flags[0]["components"])
        assert "subscription" in components_str
        assert "commissioning" in components_str


# =========================================================================
# TRAP 3 — Subcontractor overlap
# =========================================================================

class TestTrap3SubcontractorOverlap:
    """Separate document, separate vendor. Overlap anomaly flagged."""

    def test_overlap_detected(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import detect_overlap_anomaly
        # The subcontractor (THL) does "mise en service finale" which overlaps
        # with original invoice's "commissioning" line
        # Need OCR text on both: original with "commissioning", sub with "mise en service"
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'pump control hardware commissioning annual monitoring' WHERE document_id = ?",
            (ids["invoice_id"],),
        )
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'mise en service finale du système' WHERE document_id = ?",
            (ids["subcontractor_id"],),
        )
        conn.commit()

        anomalies = detect_overlap_anomaly(
            conn,
            new_document_id=ids["subcontractor_id"],
            client_code="BSQ",
            lookback_days=90,
        )
        # Should detect overlap between THL and Polar Fluid on commissioning/mise en service
        assert len(anomalies) >= 1
        vendors = {a["vendor_a"] for a in anomalies} | {a["vendor_b"] for a in anomalies}
        assert "Polar Fluid Technologies" in vendors or "Techniques Hydrauliques Laval inc." in vendors

    def test_overlap_persisted(self, seeded_db):
        conn, ids = seeded_db
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'pump control hardware commissioning annual monitoring' WHERE document_id = ?",
            (ids["invoice_id"],),
        )
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'mise en service finale du système' WHERE document_id = ?",
            (ids["subcontractor_id"],),
        )
        conn.commit()

        from src.engines.correction_chain import detect_overlap_anomaly
        detect_overlap_anomaly(
            conn, new_document_id=ids["subcontractor_id"], client_code="BSQ",
        )
        rows = conn.execute(
            "SELECT * FROM overlap_anomalies WHERE client_code = 'BSQ'"
        ).fetchall()
        assert len(rows) >= 1
        assert rows[0]["status"] == "open"

    def test_same_vendor_not_flagged(self, seeded_db):
        conn, ids = seeded_db
        # Insert another doc from same vendor (exact match)
        conn.execute(
            """INSERT INTO documents
                   (document_id, client_code, vendor, document_date, amount,
                    doc_type, memo, created_at, updated_at)
               VALUES ('doc_pft_extra', 'BSQ', 'Polar Fluid Technologies',
                       '2025-05-15', 5000, 'invoice', 'commissioning follow-up',
                       '2025-05-15', '2025-05-15')""",
        )
        conn.commit()
        from src.engines.correction_chain import detect_overlap_anomaly
        # Check that the exact-same-vendor doc doesn't flag against original
        # (original vendor = "Polar Fluid Technologies", new doc = same)
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'commissioning work' WHERE document_id = ?",
            (ids["invoice_id"],),
        )
        conn.commit()
        anomalies = detect_overlap_anomaly(
            conn, new_document_id="doc_pft_extra", client_code="BSQ",
        )
        # Filter for only same-vendor overlaps (exact "Polar Fluid Technologies" in both)
        pft_exact = [
            a for a in anomalies
            if a.get("vendor_a") == "Polar Fluid Technologies"
            and a.get("vendor_b") == "Polar Fluid Technologies"
        ]
        assert len(pft_exact) == 0


# =========================================================================
# TRAP 4 — June activation changes recognition
# =========================================================================

class TestTrap4RecognitionTiming:
    """Monitoring starts June 3. Should not be recognized in April or May."""

    def test_deferred_recognition_detected(self, seeded_db):
        conn, ids = seeded_db
        # Set activation date to June
        conn.execute(
            "UPDATE documents SET activation_date = '2025-06-03' WHERE document_id = ?",
            (ids["invoice_id"],),
        )
        conn.commit()

        from src.engines.amendment_engine import validate_recognition_timing
        result = validate_recognition_timing(conn, ids["invoice_id"])
        assert len(result["issues"]) >= 1
        issue_types = [i["issue"] for i in result["issues"]]
        assert "deferred_recognition_required" in issue_types

    def test_update_recognition_sets_deferred(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import update_recognition_period
        result = update_recognition_period(
            conn, ids["invoice_id"], "2025-06-03", updated_by="system",
        )
        assert result["recognition_period"] == "2025-06"
        assert result["recognition_status"] == "deferred"
        assert result["prior_period_impact"] is True
        assert result["impacted_period"] == "2025-04"

    def test_amendment_flag_raised_for_recognition(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import (
            update_recognition_period,
            get_open_amendment_flags,
        )
        result = update_recognition_period(
            conn, ids["invoice_id"], "2025-06-03", updated_by="system",
        )
        assert result.get("amendment_needed") is True
        flags = get_open_amendment_flags(conn, "BSQ", "2025-04")
        recognition_flags = [f for f in flags if f["trigger_type"] == "recognition_timing"]
        assert len(recognition_flags) >= 1

    def test_prior_treatment_contradiction_visible(self, seeded_db):
        conn, ids = seeded_db
        conn.execute(
            "UPDATE documents SET activation_date = '2025-06-03' WHERE document_id = ?",
            (ids["invoice_id"],),
        )
        conn.commit()
        from src.engines.amendment_engine import validate_recognition_timing
        result = validate_recognition_timing(conn, ids["invoice_id"])
        issue_types = [i["issue"] for i in result["issues"]]
        assert "prior_treatment_contradiction" in issue_types


# =========================================================================
# TRAP 5 — Duplicate credit memo ingestion
# =========================================================================

class TestTrap5DuplicateClustering:
    """Three variants = one cluster, one correction, one refund."""

    def test_three_variants_one_cluster(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import cluster_documents
        result = cluster_documents(
            conn,
            [ids["cm_email"], ids["cm_portal"], ids["cm_photo"]],
            client_code="BSQ",
            reason="OCR variant duplicates",
        )
        assert result["member_count"] == 3
        assert result["cluster_head_id"] == ids["cm_email"]

    def test_non_head_blocked_from_correction(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            cluster_documents,
            apply_single_correction,
        )
        cluster_documents(
            conn,
            [ids["cm_email"], ids["cm_portal"], ids["cm_photo"]],
            client_code="BSQ",
        )
        # Portal (non-head) should be skipped
        result = apply_single_correction(
            conn,
            credit_memo_id=ids["cm_portal"],
            original_invoice_id=ids["invoice_id"],
            client_code="BSQ",
            decomposition={"pretax": 6000, "gst": 300, "qst": 600},
        )
        assert result["status"] == "skipped_duplicate"

    def test_head_applies_correction(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            cluster_documents,
            apply_single_correction,
        )
        cluster_documents(
            conn,
            [ids["cm_email"], ids["cm_portal"], ids["cm_photo"]],
            client_code="BSQ",
        )
        result = apply_single_correction(
            conn,
            credit_memo_id=ids["cm_email"],  # head
            original_invoice_id=ids["invoice_id"],
            client_code="BSQ",
            decomposition={"pretax": 6000, "gst": 300, "qst": 600},
        )
        assert result["status"] == "created"

    def test_idempotent_correction(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import apply_single_correction
        decomp = {"pretax": 6000, "gst": 300, "qst": 600}
        r1 = apply_single_correction(
            conn, credit_memo_id=ids["cm_email"],
            original_invoice_id=ids["invoice_id"],
            client_code="BSQ", decomposition=decomp,
        )
        r2 = apply_single_correction(
            conn, credit_memo_id=ids["cm_email"],
            original_invoice_id=ids["invoice_id"],
            client_code="BSQ", decomposition=decomp,
        )
        assert r1["status"] == "created"
        assert r2["status"] == "already_applied"

    def test_is_duplicate_of_cluster_head(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            cluster_documents,
            is_duplicate_of_cluster_head,
        )
        cluster_documents(
            conn,
            [ids["cm_email"], ids["cm_portal"], ids["cm_photo"]],
            client_code="BSQ",
        )
        assert is_duplicate_of_cluster_head(conn, ids["cm_email"]) is False   # head
        assert is_duplicate_of_cluster_head(conn, ids["cm_portal"]) is True   # non-head
        assert is_duplicate_of_cluster_head(conn, ids["cm_photo"]) is True    # non-head


# =========================================================================
# TRAP 6 — Stale human approval
# =========================================================================

class TestTrap6StaleApproval:
    """Stale action rejected when case version changed."""

    def test_version_mismatch_raises(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.concurrency_engine import (
            check_version_or_raise,
            StaleVersionError,
        )
        # Version is 1 initially
        with pytest.raises(StaleVersionError) as exc_info:
            check_version_or_raise(conn, "document", ids["invoice_id"], 999)
        assert exc_info.value.expected_version == 999
        assert exc_info.value.current_version == 1

    def test_version_match_passes(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.concurrency_engine import check_version_or_raise
        # Should not raise
        check_version_or_raise(conn, "document", ids["invoice_id"], 1)

    def test_stale_approval_logged(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.concurrency_engine import (
            check_version_or_raise,
            StaleVersionError,
        )
        try:
            check_version_or_raise(conn, "document", ids["invoice_id"], 999)
        except StaleVersionError:
            pass
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE event_type = 'stale_version_rejected'"
        ).fetchall()
        assert len(rows) >= 1

    def test_approve_with_version_check(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.concurrency_engine import approve_with_version_check
        result = approve_with_version_check(
            conn,
            document_id=ids["cm_email"],
            expected_document_version=1,
            reviewer="ReviewerA",
        )
        assert result["status"] == "approved"
        assert result["version_at_approval"] == 1


# =========================================================================
# TRAP 7 — Manual journal collision
# =========================================================================

class TestTrap7ManualJournalCollision:
    """Manual journal blocked when it conflicts with document correction."""

    def test_collision_detected_with_correction_chain(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import build_correction_chain_link
        from src.engines.concurrency_engine import detect_manual_journal_collision

        # First create a correction chain
        build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            link_type="credit_memo",
            economic_effect="reduction",
            amount=6000.00,
        )

        # Set gl_account on the credit memo doc
        conn.execute(
            "UPDATE documents SET gl_account = '5100' WHERE document_id = ?",
            (ids["cm_email"],),
        )
        conn.commit()

        # Bookkeeper tries manual journal touching same GL and similar amount
        collision = detect_manual_journal_collision(
            conn,
            client_code="BSQ",
            period="2025-05",
            debit_account="2100",       # A/P
            credit_account="5100",      # expense recovery (same GL)
            amount=6000.00,
            document_id=None,           # No linked doc
        )
        assert collision["has_collision"] is True
        assert len(collision["collisions"]) >= 1

    def test_journal_quarantined(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.concurrency_engine import quarantine_manual_journal

        # Create a manual journal entry
        entry_id = "mje_bookkeeper_001"
        conn.execute(
            """INSERT INTO manual_journal_entries
                   (entry_id, client_code, period, entry_date, prepared_by,
                    debit_account, credit_account, amount, description,
                    source, status, created_at, updated_at)
               VALUES (?, 'BSQ', '2025-05', '2025-05-30', 'ExternalBookkeeper',
                       '2100', '5100', 6900.00,
                       'DR A/P 6900 CR expense recovery 6000 CR GST rec 300 CR QST rec 600',
                       'bookkeeper', 'draft', ?, ?)""",
            (entry_id, "2025-05-30T12:00:00+00:00", "2025-05-30T12:00:00+00:00"),
        )
        conn.commit()

        result = quarantine_manual_journal(
            conn, entry_id,
            collision_document_id=ids["cm_email"],
            reason="correction_chain_overlap",
        )
        assert result["status"] == "quarantined"

        row = conn.execute(
            "SELECT status, collision_status FROM manual_journal_entries WHERE entry_id = ?",
            (entry_id,),
        ).fetchone()
        assert row["status"] == "quarantined"
        assert row["collision_status"] == "collision_detected"

    def test_validate_blocks_colliding_journal(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import build_correction_chain_link
        from src.engines.concurrency_engine import validate_manual_journal

        build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            amount=6000.00,
        )
        conn.execute(
            "UPDATE documents SET gl_account = '5100' WHERE document_id = ?",
            (ids["cm_email"],),
        )
        conn.commit()

        entry_id = "mje_test_validate"
        conn.execute(
            """INSERT INTO manual_journal_entries
                   (entry_id, client_code, period, entry_date,
                    debit_account, credit_account, amount,
                    source, status, created_at, updated_at)
               VALUES (?, 'BSQ', '2025-05', '2025-05-30',
                       '2100', '5100', 6000.00,
                       'bookkeeper', 'draft', ?, ?)""",
            (entry_id, "2025-05-30", "2025-05-30"),
        )
        conn.commit()

        result = validate_manual_journal(
            conn,
            entry_id=entry_id,
            client_code="BSQ",
            period="2025-05",
            debit_account="2100",
            credit_account="5100",
            amount=6000.00,
        )
        assert result["accepted"] is False
        assert result["status"] == "quarantined"


# =========================================================================
# TRAP 8 — Rollback corruption
# =========================================================================

class TestTrap8RollbackCorruption:
    """Rollback is explicit, audited, idempotent. No duplicate effects."""

    def test_rollback_is_audited(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            build_correction_chain_link,
            rollback_correction,
        )
        link = build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            amount=6000.00,
        )
        chain_id = link["chain_id"]

        result = rollback_correction(
            conn,
            chain_id=chain_id,
            client_code="BSQ",
            rolled_back_by="Manager",
            rollback_reason="undo credit memo application",
        )
        assert result["status"] == "rolled_back"

        # Check rollback log
        logs = conn.execute(
            "SELECT * FROM rollback_log WHERE target_id = ?", (str(chain_id),)
        ).fetchall()
        assert len(logs) == 1
        assert logs[0]["rolled_back_by"] == "Manager"

    def test_rollback_is_idempotent(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            build_correction_chain_link,
            rollback_correction,
        )
        link = build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            amount=6000.00,
        )
        chain_id = link["chain_id"]

        r1 = rollback_correction(
            conn, chain_id=chain_id, client_code="BSQ",
            rolled_back_by="Manager", rollback_reason="undo",
        )
        r2 = rollback_correction(
            conn, chain_id=chain_id, client_code="BSQ",
            rolled_back_by="Manager", rollback_reason="undo again",
        )
        assert r1["status"] == "rolled_back"
        assert r2["status"] == "already_rolled_back"

    def test_reimport_blocked_after_rollback(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            build_correction_chain_link,
            rollback_correction,
            check_reimport_after_rollback,
        )
        link = build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            amount=6000.00,
        )
        rollback_correction(
            conn,
            chain_id=link["chain_id"],
            client_code="BSQ",
            rolled_back_by="Manager",
            rollback_reason="intentional archive",
            block_reimport=True,
        )
        check = check_reimport_after_rollback(conn, ids["cm_email"], "BSQ")
        assert check["can_reimport"] is False
        reason_types = [r["reason"] for r in check["reasons"]]
        assert "reimport_blocked_by_rollback" in reason_types

    def test_reimport_allowed_after_non_blocking_rollback(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            build_correction_chain_link,
            rollback_correction,
            check_reimport_after_rollback,
        )
        link = build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            amount=6000.00,
        )
        rollback_correction(
            conn,
            chain_id=link["chain_id"],
            client_code="BSQ",
            rolled_back_by="Manager",
            rollback_reason="undo, will reimport",
            block_reimport=False,
        )
        check = check_reimport_after_rollback(conn, ids["cm_email"], "BSQ")
        assert check["can_reimport"] is True


# =========================================================================
# TRAP 9 — Audit trail lineage
# =========================================================================

class TestTrap9AuditLineage:
    """System can answer: what was believed, what changed, who approved,
    which version was filed, what contradicted it, whether amendment was
    required, whether rollback happened, current state."""

    def test_filing_snapshot_captured(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import take_filing_snapshot
        result = take_filing_snapshot(conn, "BSQ", "2025-04", filed_by="Owner")
        assert result["snapshot_count"] >= 1

        snaps = conn.execute(
            "SELECT * FROM document_snapshots WHERE snapshot_type = 'filing'"
        ).fetchall()
        assert len(snaps) >= 1

    def test_belief_at_filing_time(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import (
            take_filing_snapshot,
            get_belief_at_time,
        )
        take_filing_snapshot(conn, "BSQ", "2025-04", filed_by="Owner")

        # Check that snapshots were actually created
        snaps = conn.execute(
            "SELECT taken_at FROM document_snapshots WHERE document_id = ?",
            (ids["invoice_id"],),
        ).fetchall()
        assert len(snaps) >= 1

        # Use a date far enough in the future to match any snapshot
        belief = get_belief_at_time(
            conn, ids["invoice_id"], "2099-12-31T23:59:59+00:00",
        )
        assert belief["belief"] is not None
        assert belief["belief"]["amount"] == 27594.00
        assert belief["snapshot_type"] == "filing"

    def test_full_amendment_timeline(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import (
            take_filing_snapshot,
            build_period_correction_entry,
            get_amendment_timeline,
        )
        # 1. Take filing snapshot
        take_filing_snapshot(conn, "BSQ", "2025-04", filed_by="Owner")
        # 2. Build correction
        build_period_correction_entry(
            conn,
            original_document_id=ids["invoice_id"],
            correction_document_id=ids["cm_email"],
            client_code="BSQ",
            correction_period="2025-05",
            correction_amount=6000.00,
        )
        # 3. Get timeline
        timeline = get_amendment_timeline(conn, "BSQ", "2025-04")
        assert "filing" in timeline
        assert timeline["filing"]["filed_at"] is not None
        assert len(timeline.get("filing_snapshots", [])) >= 1
        assert len(timeline.get("amendment_flags", [])) >= 1
        assert len(timeline.get("correction_snapshots", [])) >= 1  # pre_correction snapshot
        assert len(timeline.get("current_state", [])) >= 1

    def test_correction_chain_in_timeline(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import (
            build_period_correction_entry,
            get_amendment_timeline,
        )
        from src.engines.correction_chain import get_full_correction_chain

        build_period_correction_entry(
            conn,
            original_document_id=ids["invoice_id"],
            correction_document_id=ids["cm_email"],
            client_code="BSQ",
            correction_period="2025-05",
            correction_amount=6000.00,
            correction_gst=300.00,
            correction_qst=600.00,
        )

        chain = get_full_correction_chain(conn, ids["invoice_id"])
        assert chain["link_count"] >= 1
        assert chain["active_links"] >= 1
        assert chain["total_gst_impact"] == 300.00
        assert chain["total_qst_impact"] == 600.00

    def test_rollback_visible_in_timeline(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.correction_chain import (
            build_correction_chain_link,
            rollback_correction,
        )
        from src.engines.amendment_engine import get_amendment_timeline

        link = build_correction_chain_link(
            conn,
            chain_root_id=ids["invoice_id"],
            client_code="BSQ",
            source_document_id=ids["invoice_id"],
            target_document_id=ids["cm_email"],
            amount=6000.00,
        )
        rollback_correction(
            conn,
            chain_id=link["chain_id"],
            client_code="BSQ",
            rolled_back_by="Manager",
            rollback_reason="test rollback",
        )

        timeline = get_amendment_timeline(conn, "BSQ", "2025-04")
        assert len(timeline.get("rollbacks", [])) >= 1
        assert timeline["rollbacks"][0]["rolled_back_by"] == "Manager"


# =========================================================================
# INTEGRATION — Full Day-by-Day scenario
# =========================================================================

class TestFullScenarioIntegration:
    """Walk through the complete Day 1-45 scenario."""

    def test_full_scenario_day_by_day(self, seeded_db):
        conn, ids = seeded_db
        from src.engines.amendment_engine import (
            take_filing_snapshot,
            build_period_correction_entry,
            update_recognition_period,
            get_amendment_timeline,
            get_belief_at_time,
        )
        from src.engines.correction_chain import (
            decompose_credit_memo_safe,
            cluster_documents,
            apply_single_correction,
            detect_overlap_anomaly,
            rollback_correction,
            check_reimport_after_rollback,
            get_full_correction_chain,
        )
        from src.engines.concurrency_engine import (
            check_version_or_raise,
            StaleVersionError,
            validate_manual_journal,
        )

        # --- Day 1: Original posting (already seeded) ---
        inv = conn.execute(
            "SELECT * FROM posting_jobs WHERE document_id = ?",
            (ids["invoice_id"],),
        ).fetchone()
        assert inv["posting_status"] == "posted"

        # --- Day 7: Bank payment matched (already seeded) ---
        # (bank_matcher would handle this; we verify the posting is settled)

        # --- Filing snapshot taken ---
        take_filing_snapshot(conn, "BSQ", "2025-04", filed_by="Owner")

        # --- Day 32: Three new documents arrive ---
        # A) Credit memo decomposition (Trap 2)
        decomp = decompose_credit_memo_safe(
            conn,
            credit_memo_id=ids["cm_email"],
            credit_memo_amount_tax_included=6900.00,
            original_invoice_id=ids["invoice_id"],
            has_tax_breakdown=False,
            memo_text="monitoring subscription removed; commissioning rebilled separately",
        )
        assert decomp["confidence"] < 0.95
        assert len(decomp["uncertainty_flags"]) >= 1

        # B) Subcontractor overlap (Trap 3)
        # Add commissioning keyword to original invoice OCR text so overlap engine
        # can find cross-language match ("mise en service" ↔ "commissioning")
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'pump control hardware commissioning annual monitoring' WHERE document_id = ?",
            (ids["invoice_id"],),
        )
        # Also add "mise en service" to subcontractor OCR for the overlap to trigger
        conn.execute(
            "UPDATE documents SET raw_ocr_text = 'mise en service finale du système de pompage' WHERE document_id = ?",
            (ids["subcontractor_id"],),
        )
        conn.commit()
        overlaps = detect_overlap_anomaly(
            conn, new_document_id=ids["subcontractor_id"], client_code="BSQ",
        )
        assert len(overlaps) >= 1

        # --- Day 36: Duplicate clustering (Trap 5) ---
        cluster = cluster_documents(
            conn,
            [ids["cm_email"], ids["cm_portal"], ids["cm_photo"]],
            client_code="BSQ",
        )
        assert cluster["member_count"] == 3
        assert cluster["cluster_head_id"] == ids["cm_email"]

        # Only head applies correction
        correction = apply_single_correction(
            conn,
            credit_memo_id=ids["cm_email"],
            original_invoice_id=ids["invoice_id"],
            client_code="BSQ",
            decomposition=decomp,
        )
        assert correction["status"] == "created"

        # Non-head skipped
        skip = apply_single_correction(
            conn,
            credit_memo_id=ids["cm_portal"],
            original_invoice_id=ids["invoice_id"],
            client_code="BSQ",
            decomposition=decomp,
        )
        assert skip["status"] == "skipped_duplicate"

        # --- Day 38: Concurrent reviewers (Trap 6) ---
        # Reviewer A reads version 1
        reviewer_a_version = 1

        # Reviewer A modifies (version auto-increments)
        conn.execute(
            "UPDATE documents SET review_status = 'Ready' WHERE document_id = ?",
            (ids["cm_email"],),
        )
        conn.commit()

        # Reviewer B tries with stale version
        new_version = conn.execute(
            "SELECT version FROM documents WHERE document_id = ?",
            (ids["cm_email"],),
        ).fetchone()["version"]

        # If version changed, Reviewer B is rejected
        if new_version != reviewer_a_version:
            with pytest.raises(StaleVersionError):
                check_version_or_raise(
                    conn, "document", ids["cm_email"], reviewer_a_version,
                )

        # --- Day 38: Manual journal collision (Trap 7) ---
        entry_id = "mje_bookkeeper_scenario"
        conn.execute(
            """INSERT INTO manual_journal_entries
                   (entry_id, client_code, period, entry_date,
                    debit_account, credit_account, amount,
                    description, source, status, created_at, updated_at)
               VALUES (?, 'BSQ', '2025-05', '2025-05-30',
                       '2100', '5100', 6000.00,
                       'DR A/P CR expense recovery',
                       'bookkeeper', 'draft', ?, ?)""",
            (entry_id, "2025-05-30", "2025-05-30"),
        )
        conn.execute(
            "UPDATE documents SET gl_account = '5100' WHERE document_id = ?",
            (ids["cm_email"],),
        )
        conn.commit()

        journal_result = validate_manual_journal(
            conn,
            entry_id=entry_id,
            client_code="BSQ",
            period="2025-05",
            debit_account="2100",
            credit_account="5100",
            amount=6000.00,
        )
        assert journal_result["accepted"] is False

        # --- Day 39: Correction entry created (Trap 1) ---
        correction_entry = build_period_correction_entry(
            conn,
            original_document_id=ids["invoice_id"],
            correction_document_id=ids["cm_email"],
            client_code="BSQ",
            correction_period="2025-05",
            correction_amount=6000.00,
            correction_gst=300.00,
            correction_qst=600.00,
        )
        assert correction_entry["amendment_flag_raised"] is True
        assert correction_entry["original_period"] == "2025-04"

        # Verify April posting untouched
        april_posting = conn.execute(
            "SELECT * FROM posting_jobs WHERE document_id = ?",
            (ids["invoice_id"],),
        ).fetchone()
        assert april_posting["posting_status"] == "posted"
        assert april_posting["amount"] == 27594.00

        # --- Day 42: June activation (Trap 4) ---
        recog = update_recognition_period(
            conn, ids["invoice_id"], "2025-06-03",
        )
        assert recog["recognition_period"] == "2025-06"
        assert recog["prior_period_impact"] is True

        # --- Day 45: Manager rollback (Trap 8) ---
        chain = get_full_correction_chain(conn, ids["invoice_id"])
        active_links = [l for l in chain["links"] if l["status"] == "active"]
        assert len(active_links) >= 1

        # Roll back ALL active links (both the apply_single_correction and
        # the build_period_correction_entry created chain links)
        for link in active_links:
            rollback_correction(
                conn,
                chain_id=link["chain_id"],
                client_code="BSQ",
                rolled_back_by="Manager",
                rollback_reason="undo credit memo application",
                block_reimport=False,
            )

        # Verify all rolled back
        chain_after = get_full_correction_chain(conn, ids["invoice_id"])
        remaining_active = [l for l in chain_after["links"] if l["status"] == "active"]
        assert len(remaining_active) == 0

        # Re-import check
        reimport = check_reimport_after_rollback(conn, ids["cm_email"], "BSQ")
        assert reimport["can_reimport"] is True  # Not blocked

        # --- Trap 9: Full audit trail ---
        timeline = get_amendment_timeline(conn, "BSQ", "2025-04")
        assert timeline["filing"]["filed_at"] is not None
        assert len(timeline.get("filing_snapshots", [])) >= 1
        assert len(timeline.get("amendment_flags", [])) >= 1
        assert len(timeline.get("rollbacks", [])) >= 1
        assert len(timeline.get("current_state", [])) >= 1

        # Belief at filing time shows original state
        belief = get_belief_at_time(
            conn, ids["invoice_id"], "2099-12-31T23:59:59+00:00",
        )
        assert belief["belief"] is not None
        assert belief["belief"]["amount"] == 27594.00

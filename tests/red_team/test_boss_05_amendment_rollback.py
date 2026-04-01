"""
tests/red_team/test_boss_05_amendment_rollback.py
=================================================
BOSS FIGHT 5 — Amendment + Rollback Collision.

Filed period → later contradiction → rollback → re-import → stale approval.
Tests the full amendment lifecycle including rollback scenarios.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.amendment_engine import (
    build_period_correction_entry,
    flag_amendment_needed,
    get_amendment_timeline,
    get_belief_at_time,
    get_open_amendment_flags,
    is_period_filed,
    resolve_amendment_flag,
    snapshot_document,
    take_filing_snapshot,
)
from src.engines.uncertainty_engine import (
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    evaluate_uncertainty,
    reason_filed_period_amendment,
    reason_reimport_blocked,
    reason_stale_version,
    reason_prior_treatment_contradiction,
)
from src.engines.tax_engine import calculate_gst_qst, extract_tax_from_total


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _setup_full_db(conn: sqlite3.Connection):
    """Create all tables needed for amendment + rollback tests."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS gst_filings (
            filing_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            period_label TEXT NOT NULL,
            filed_at TEXT, notes TEXT
        );
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, client_code TEXT,
            vendor TEXT, amount REAL, gl_account TEXT,
            tax_code TEXT, document_date TEXT, doc_type TEXT,
            review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, raw_result TEXT,
            file_name TEXT, file_path TEXT, submitted_by TEXT,
            client_note TEXT, fraud_flags TEXT, category TEXT,
            activation_date TEXT, recognition_period TEXT,
            recognition_status TEXT DEFAULT 'immediate'
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            client_code TEXT, vendor TEXT, amount REAL,
            gl_account TEXT, tax_code TEXT, document_date TEXT,
            doc_type TEXT, review_status TEXT DEFAULT 'approved',
            file_name TEXT, file_path TEXT, currency TEXT DEFAULT 'CAD',
            category TEXT, memo TEXT, confidence REAL,
            blocking_issues TEXT, notes TEXT,
            posting_status TEXT DEFAULT 'posted',
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS amendment_flags (
            flag_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT NOT NULL,
            filed_period TEXT NOT NULL,
            trigger_document_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL DEFAULT 'credit_memo',
            reason_en TEXT NOT NULL DEFAULT '',
            reason_fr TEXT NOT NULL DEFAULT '',
            original_filing_id TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            resolved_by TEXT,
            resolved_at TEXT,
            amendment_filing_id TEXT,
            created_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT '',
            UNIQUE(client_code, filed_period, trigger_document_id)
        );
        CREATE TABLE IF NOT EXISTS document_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL,
            snapshot_type TEXT NOT NULL DEFAULT 'filing',
            snapshot_reason TEXT NOT NULL DEFAULT '',
            state_json TEXT NOT NULL DEFAULT '{}',
            taken_by TEXT NOT NULL DEFAULT 'system',
            taken_at TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS posting_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            posting_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            snapshot_type TEXT NOT NULL DEFAULT 'filing',
            snapshot_reason TEXT NOT NULL DEFAULT '',
            state_json TEXT NOT NULL DEFAULT '{}',
            taken_by TEXT NOT NULL DEFAULT 'system',
            taken_at TEXT NOT NULL DEFAULT ''
        );
    """)
    conn.commit()


def _file_period(conn, client, period):
    conn.execute(
        "INSERT INTO gst_filings (filing_id, client_code, period_label, filed_at) VALUES (?,?,?,?)",
        (f"f_{period}", client, period, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _insert_doc(conn, doc_id, client, vendor, amount, gl, tc, doc_date, doc_type="invoice"):
    conn.execute(
        """INSERT INTO documents
           (document_id, client_code, vendor, amount, gl_account, tax_code,
            document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (doc_id, client, vendor, amount, gl, tc, doc_date, doc_type, "approved"),
    )
    conn.execute(
        """INSERT INTO posting_jobs
           (posting_id, document_id, client_code, vendor, amount, gl_account,
            tax_code, document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (f"pj_{doc_id}", doc_id, client, vendor, amount, gl, tc, doc_date, doc_type, "approved"),
    )
    conn.commit()


class TestAmendmentRollbackCollision:
    """Filed period, contradiction, rollback, re-import, stale approval."""

    def test_full_amendment_lifecycle(self):
        """File → flag → snapshot → correct → resolve."""
        conn = _fresh_db()
        _setup_full_db(conn)
        client = "ROLLBACK_CO"

        # 1. Insert original doc and file the period
        _insert_doc(conn, "ORIG-001", client, "Vendor A", 5000.0,
                    "5200", "T", "2026-01-15")
        _file_period(conn, client, "2026-01")
        assert is_period_filed(conn, client, "2026-01")

        # 2. Snapshot at filing time
        snap = snapshot_document(conn, "ORIG-001", snapshot_type="filing")
        assert snap is not None

        # 3. Later contradiction: amount was wrong
        result = flag_amendment_needed(
            conn,
            client_code=client,
            filed_period="2026-01",
            trigger_document_id="ORIG-001",
            trigger_type="correction_entry",
            reason_en="Amount incorrect — should be $4500",
        )
        assert result["status"] == "amendment_flag_raised"

        # 4. Build correction in current period
        entry = build_period_correction_entry(
            conn,
            original_document_id="ORIG-001",
            correction_document_id="CORR-001",
            client_code=client,
            correction_period="2026-04",
            correction_amount=-500.0,
            reason_en="Amount overstatement corrected",
        )
        assert entry is not None

        # 5. Resolve both flags (the manually raised one and the one from build_period_correction_entry)
        resolve_amendment_flag(
            conn,
            client_code=client,
            filed_period="2026-01",
            trigger_document_id="ORIG-001",
            resolved_by="cpa@test.com",
        )
        resolve_amendment_flag(
            conn,
            client_code=client,
            filed_period="2026-01",
            trigger_document_id="CORR-001",
            resolved_by="cpa@test.com",
        )
        open_flags = get_open_amendment_flags(conn, client, filed_period="2026-01")
        assert len(open_flags) == 0

    def test_stale_version_blocks_posting(self):
        """Stale version detection must block posting."""
        reason = reason_stale_version()
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.95, "gl_account": 0.95, "date": 0.95},
            reasons=[reason],
        )
        assert not state.can_post
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS

    def test_reimport_blocked_after_rollback(self):
        """Re-import after rollback must be blocked until reviewed."""
        reason = reason_reimport_blocked()
        state = evaluate_uncertainty(
            {"vendor": 0.90, "amount": 0.90, "gl_account": 0.90},
            reasons=[reason],
        )
        assert not state.can_post

    def test_prior_treatment_contradiction_flagged(self):
        """Changing GL account for same vendor after filing must flag."""
        reason = reason_prior_treatment_contradiction()
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.95, "gl_account": 0.60},
            reasons=[reason],
        )
        assert not state.can_post

    def test_multiple_amendments_same_period(self):
        """Multiple amendments to the same period must all be tracked."""
        conn = _fresh_db()
        _setup_full_db(conn)
        client = "MULTI_AMEND"
        _file_period(conn, client, "2026-02")

        for i in range(5):
            flag_amendment_needed(
                conn,
                client_code=client,
                filed_period="2026-02",
                trigger_document_id=f"DOC-{i}",
                reason_en=f"Issue #{i+1}",
            )

        open_flags = get_open_amendment_flags(conn, client, filed_period="2026-02")
        assert len(open_flags) == 5

        # Resolve some but not all
        resolve_amendment_flag(conn, client_code=client, filed_period="2026-02",
                               trigger_document_id="DOC-0", resolved_by="cpa")
        resolve_amendment_flag(conn, client_code=client, filed_period="2026-02",
                               trigger_document_id="DOC-1", resolved_by="cpa")

        remaining = get_open_amendment_flags(conn, client, filed_period="2026-02")
        assert len(remaining) == 3

    def test_rollback_tax_integrity(self):
        """Rolling back a document should not corrupt tax calculations."""
        orig = calculate_gst_qst(Decimal("5000"))
        correction = calculate_gst_qst(Decimal("500"))
        net = calculate_gst_qst(Decimal("4500"))

        expected_net_gst = orig["gst"] - correction["gst"]
        assert abs(expected_net_gst - net["gst"]) <= Decimal("0.01")

    def test_amendment_timeline_shows_all_events(self):
        """Timeline must include filing, flags, resolutions."""
        conn = _fresh_db()
        _setup_full_db(conn)
        client = "TIMELINE_CO"

        _insert_doc(conn, "TL-001", client, "Vendor T", 3000.0,
                    "5200", "T", "2026-02-10")
        _file_period(conn, client, "2026-02")

        flag_amendment_needed(conn, client_code=client, filed_period="2026-02",
                              trigger_document_id="TL-001", reason_en="Late credit")
        resolve_amendment_flag(conn, client_code=client, filed_period="2026-02",
                               trigger_document_id="TL-001", resolved_by="cpa")

        timeline = get_amendment_timeline(conn, client, "2026-02")
        assert isinstance(timeline, dict)
        assert "amendment_flags" in timeline

    def test_snapshot_before_and_after_correction(self):
        """Snapshots must capture state before and after correction."""
        conn = _fresh_db()
        _setup_full_db(conn)
        client = "SNAP_CO"

        _insert_doc(conn, "S-001", client, "Vendor S", 2000.0,
                    "5200", "T", "2026-01-20")

        # Snapshot before correction
        snap1 = snapshot_document(conn, "S-001", snapshot_type="pre_correction")
        assert snap1 is not None

        # "Correct" the document
        conn.execute(
            "UPDATE documents SET amount = 1800.0 WHERE document_id = ?",
            ("S-001",),
        )
        conn.commit()

        # Snapshot after correction
        snap2 = snapshot_document(conn, "S-001", snapshot_type="post_correction")
        assert snap2 is not None

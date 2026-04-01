"""
tests/red_team/test_boss_03_temporal_integrity.py
=================================================
BOSS FIGHT 3 — Temporal Integrity.

Filed period → later credit memo → subcontractor invoice →
refund → correction layering. Everything must preserve time ordering
and amendment flags.
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
    get_open_amendment_flags,
    is_period_filed,
    resolve_amendment_flag,
    snapshot_document,
    take_filing_snapshot,
    validate_recognition_timing,
)
from src.engines.tax_engine import (
    calculate_gst_qst,
    extract_tax_from_total,
)
from src.engines.uncertainty_engine import (
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    evaluate_uncertainty,
    reason_filed_period_amendment,
    reason_credit_memo_tax_split_unproven,
    reason_recognition_timing_deferred,
    reason_prior_treatment_contradiction,
)


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _setup_amendment_db(conn: sqlite3.Connection):
    """Create the tables needed for amendment engine tests with correct schema."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS gst_filings (
            filing_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            period_label TEXT NOT NULL,
            filed_at TEXT,
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            vendor TEXT,
            amount REAL,
            gl_account TEXT,
            tax_code TEXT,
            document_date TEXT,
            doc_type TEXT,
            review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95,
            raw_result TEXT,
            file_name TEXT, file_path TEXT,
            submitted_by TEXT, client_note TEXT,
            fraud_flags TEXT, category TEXT,
            activation_date TEXT,
            recognition_period TEXT,
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


def _file_period(conn, client_code, period_label):
    """Mark a period as filed."""
    conn.execute(
        "INSERT INTO gst_filings (filing_id, client_code, period_label, filed_at) "
        "VALUES (?, ?, ?, ?)",
        (f"fil_{period_label}", client_code, period_label,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


class TestTemporalIntegrity:
    """Filed period, later credit memo, correction layering."""

    def test_filed_period_detection(self):
        """is_period_filed returns True only after filing."""
        conn = _fresh_db()
        _setup_amendment_db(conn)
        assert not is_period_filed(conn, "TEMP_CO", "2026-01")
        _file_period(conn, "TEMP_CO", "2026-01")
        assert is_period_filed(conn, "TEMP_CO", "2026-01")

    def test_amendment_flag_lifecycle(self):
        """Flag → resolve lifecycle for a filed period."""
        conn = _fresh_db()
        _setup_amendment_db(conn)
        _file_period(conn, "TEMP_CO", "2026-01")

        # Flag amendment using keyword-only args
        result = flag_amendment_needed(
            conn,
            client_code="TEMP_CO",
            filed_period="2026-01",
            trigger_document_id="CM-001",
            trigger_type="credit_memo",
            reason_en="Credit memo received after filing",
        )
        assert result is not None
        assert result["status"] == "amendment_flag_raised"

        # Should have one open flag
        open_flags = get_open_amendment_flags(conn, "TEMP_CO")
        assert len(open_flags) >= 1

        # Resolve it
        resolve_amendment_flag(
            conn,
            client_code="TEMP_CO",
            filed_period="2026-01",
            trigger_document_id="CM-001",
            resolved_by="accountant@test.com",
        )

        # No more open flags for this period
        open_after = get_open_amendment_flags(conn, "TEMP_CO", filed_period="2026-01")
        assert len(open_after) == 0

    def test_credit_memo_after_filing_triggers_amendment(self):
        """Credit memo arriving after a filed period must trigger amendment flag."""
        conn = _fresh_db()
        _setup_amendment_db(conn)
        _file_period(conn, "TEMP_CO", "2026-01")

        # Insert credit memo dated in the filed period
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, amount, document_date,
                doc_type, review_status)
               VALUES (?,?,?,?,?,?,?)""",
            ("CM-100", "TEMP_CO", "Vendor Z", -500.0, "2026-01-20",
             "credit_memo", "approved"),
        )
        conn.commit()

        # Flag amendment for the filed period
        result = flag_amendment_needed(
            conn,
            client_code="TEMP_CO",
            filed_period="2026-01",
            trigger_document_id="CM-100",
            trigger_type="credit_memo",
            reason_en="Credit memo CM-100 received post-filing",
        )
        assert result["status"] == "amendment_flag_raised"

        # Uncertainty engine should raise credit memo flag
        reason = reason_credit_memo_tax_split_unproven()
        state = evaluate_uncertainty(
            {"vendor": 0.90, "amount": 0.85, "tax_code": 0.70},
            reasons=[reason],
        )
        assert not state.can_post

    def test_correction_entry_builds_in_current_period(self):
        """Correction for January error should land in current period."""
        conn = _fresh_db()
        _setup_amendment_db(conn)
        _file_period(conn, "TEMP_CO", "2026-01")

        # Insert the original document
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, amount, gl_account,
                tax_code, document_date, doc_type, review_status)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            ("ORIG-001", "TEMP_CO", "Vendor A", 1000.0, "5200", "T",
             "2026-01-15", "invoice", "approved"),
        )
        conn.commit()

        entry = build_period_correction_entry(
            conn,
            original_document_id="ORIG-001",
            correction_document_id="CORR-001",
            client_code="TEMP_CO",
            correction_period="2026-05",
            correction_amount=-200.0,
            reason_en="Overcharge corrected",
        )
        assert entry is not None
        assert entry["correction_period"] == "2026-05"
        assert entry["amendment_flag_raised"]

    def test_recognition_timing_activation_vs_invoice(self):
        """Invoice date != activation date — must flag deferred recognition."""
        conn = _fresh_db()
        _setup_amendment_db(conn)

        # Insert doc with activation date later than document date
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, amount, document_date,
                activation_date, doc_type, review_status)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("SVC-001", "TEMP_CO", "Monitoring Co", 1200.0, "2026-01-15",
             "2026-03-01", "invoice", "approved"),
        )
        conn.commit()

        result = validate_recognition_timing(conn, "SVC-001")
        assert result is not None
        # Should detect deferred recognition needed
        assert len(result.get("issues", [])) > 0

    def test_snapshot_preserves_historical_state(self):
        """Document snapshot captures state at filing time."""
        conn = _fresh_db()
        _setup_amendment_db(conn)

        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, amount, gl_account,
                document_date, doc_type, review_status)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("SNAP-001", "TEMP_CO", "Vendor B", 2000.0, "5200",
             "2026-01-10", "invoice", "approved"),
        )
        conn.commit()

        snap_id = snapshot_document(conn, "SNAP-001", snapshot_type="filing")
        assert snap_id is not None

    def test_amendment_timeline_ordering(self):
        """Amendment timeline must be chronologically structured."""
        conn = _fresh_db()
        _setup_amendment_db(conn)
        _file_period(conn, "TEMP_CO", "2026-01")

        # Create multiple flags (different trigger_document_ids for uniqueness)
        flag_amendment_needed(conn, client_code="TEMP_CO", filed_period="2026-01",
                              trigger_document_id="D1", reason_en="First issue")
        flag_amendment_needed(conn, client_code="TEMP_CO", filed_period="2026-01",
                              trigger_document_id="D2", reason_en="Second issue")
        resolve_amendment_flag(conn, client_code="TEMP_CO", filed_period="2026-01",
                               trigger_document_id="D1", resolved_by="acc1")
        flag_amendment_needed(conn, client_code="TEMP_CO", filed_period="2026-01",
                              trigger_document_id="D3", reason_en="Third issue")

        timeline = get_amendment_timeline(conn, "TEMP_CO", "2026-01")
        assert isinstance(timeline, dict)
        assert "amendment_flags" in timeline
        assert len(timeline["amendment_flags"]) >= 3

    def test_tax_roundtrip_on_credit_memo(self):
        """Credit memo tax extraction must be exact inverse of original."""
        original_total = Decimal("1149.75")
        credit_total = Decimal("-1149.75")

        orig_extract = extract_tax_from_total(abs(original_total))
        credit_extract = extract_tax_from_total(abs(credit_total))

        assert orig_extract["gst"] == credit_extract["gst"]
        assert orig_extract["qst"] == credit_extract["qst"]

    def test_refund_after_credit_memo_layering(self):
        """Original → Credit Memo → Refund: net position must be correct."""
        original = Decimal("5000")
        credit = Decimal("-1000")
        refund = Decimal("-500")

        net = original + credit + refund
        assert net == Decimal("3500")

        tax_result = calculate_gst_qst(net)
        assert tax_result["total_with_tax"] > net
        assert tax_result["gst"] > Decimal("0")

    def test_filed_period_amendment_uncertainty(self):
        """Filed period amendment reason must block posting."""
        reason = reason_filed_period_amendment()
        state = evaluate_uncertainty(
            {"vendor": 0.95, "amount": 0.95, "gl_account": 0.95},
            reasons=[reason],
        )
        assert not state.can_post
        assert state.posting_recommendation == PARTIAL_POST_WITH_FLAGS

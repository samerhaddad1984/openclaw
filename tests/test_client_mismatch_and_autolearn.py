"""
tests/test_client_mismatch_and_autolearn.py
=============================================
Tests for:
  - Cross-client mismatch detection (Improvement 1)
  - Auto-approval engine (Improvement 2)
  - Learning feedback loop (Improvement 3)
  - Learning status icons in queue (Improvement 4)
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _create_test_db(tmp_path: Path) -> Path:
    """Create a minimal test database with documents and clients tables."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_code TEXT PRIMARY KEY,
            client_name TEXT NOT NULL,
            contact_email TEXT,
            language TEXT NOT NULL DEFAULT 'fr',
            filing_freq TEXT NOT NULL DEFAULT 'quarterly',
            accountant TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            vendor TEXT,
            invoice_number TEXT,
            document_date TEXT,
            amount REAL,
            currency TEXT DEFAULT 'CAD',
            doc_type TEXT DEFAULT 'invoice',
            category TEXT,
            gl_account TEXT,
            tax_code TEXT,
            memo TEXT,
            raw_ocr_text TEXT,
            raw_result TEXT DEFAULT '{}',
            review_status TEXT DEFAULT 'New',
            confidence REAL DEFAULT 0.85,
            fraud_flags TEXT DEFAULT '[]',
            substance_flags TEXT DEFAULT '{}',
            file_name TEXT,
            file_path TEXT,
            assigned_to TEXT,
            manual_hold_reason TEXT,
            manual_hold_by TEXT,
            manual_hold_at TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            correction_count INTEGER DEFAULT 0,
            has_line_items INTEGER DEFAULT 0,
            lines_reconciled INTEGER DEFAULT 0,
            line_total_sum REAL,
            invoice_total_gap REAL,
            deposit_allocated INTEGER DEFAULT 0,
            suspected_client_mismatch INTEGER DEFAULT 0,
            suggested_client_code TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vendor_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT,
            vendor TEXT NOT NULL,
            vendor_key TEXT NOT NULL DEFAULT '',
            client_code_key TEXT NOT NULL DEFAULT '',
            gl_account TEXT,
            tax_code TEXT,
            doc_type TEXT,
            category TEXT,
            approval_count INTEGER NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0.0,
            last_amount REAL,
            last_document_id TEXT,
            last_source TEXT,
            last_used TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gl_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_id TEXT,
            client_code TEXT,
            vendor TEXT,
            vendor_key TEXT,
            gl_account TEXT,
            decided_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            username TEXT,
            document_id TEXT,
            prompt_snippet TEXT,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS learning_corrections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            correction_id TEXT,
            document_id TEXT,
            field_name TEXT,
            field_name_key TEXT,
            old_value TEXT,
            old_value_key TEXT,
            new_value TEXT,
            new_value_key TEXT,
            vendor_key TEXT,
            client_code_key TEXT,
            doc_type_key TEXT,
            category_key TEXT,
            support_count INTEGER DEFAULT 1,
            reviewer TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def _seed_clients(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    now = _utc_now_iso()
    conn.executemany(
        "INSERT INTO clients (client_code, client_name, contact_email, created_at) VALUES (?, ?, ?, ?)",
        [
            ("BOLDUC", "Bolduc Construction", "info@bolduc.ca", now),
            ("VIEUXPORT", "Restaurant Le Vieux Port", "billing@restaurantlevieuxport.ca", now),
            ("TREMBLAY", "Tremblay & Fils Inc.", "comptable@tremblay.ca", now),
        ],
    )
    conn.commit()
    conn.close()


def _seed_document(db_path: Path, document_id: str, client_code: str,
                   vendor: str = "Bell Canada", amount: float = 250.0,
                   confidence: float = 0.90, fraud_flags: str = "[]",
                   category: str = "operating_expense",
                   gl_account: str = "5420", tax_code: str = "T") -> None:
    conn = sqlite3.connect(str(db_path))
    now = _utc_now_iso()
    conn.execute(
        """INSERT INTO documents (document_id, client_code, vendor, amount,
           confidence, fraud_flags, category, gl_account, tax_code, doc_type,
           review_status, file_name, created_at, updated_at, raw_result, document_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'invoice', 'New', 'test.pdf', ?, ?, '{}', '2026-03-15')""",
        (document_id, client_code, vendor, amount, confidence, fraud_flags,
         category, gl_account, tax_code, now, now),
    )
    conn.commit()
    conn.close()


def _seed_vendor_memory(db_path: Path, vendor: str, client_code: str,
                        approval_count: int = 5, confidence: float = 0.90,
                        gl_account: str = "5420", tax_code: str = "T") -> None:
    conn = sqlite3.connect(str(db_path))
    now = _utc_now_iso()
    vk = " ".join(vendor.lower().split())
    ck = " ".join(client_code.lower().split())
    conn.execute(
        """INSERT INTO vendor_memory (client_code, vendor, vendor_key, client_code_key,
           gl_account, tax_code, doc_type, category, approval_count, confidence,
           created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'invoice', 'operating_expense', ?, ?, ?, ?)""",
        (client_code, vendor, vk, ck, gl_account, tax_code,
         approval_count, confidence, now, now),
    )
    conn.commit()
    conn.close()


# ===========================================================================
# IMPROVEMENT 1: Cross-client mismatch detection
# ===========================================================================

class TestCrossClientMismatch:
    """Tests for detect_client_mismatch()."""

    def test_mismatch_detected_company_name(self, tmp_path):
        """Bill-to company name matches a different client."""
        db = _create_test_db(tmp_path)
        _seed_clients(db)
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row

        from src.engines.client_mismatch_engine import detect_client_mismatch
        result = detect_client_mismatch(
            extracted_data={
                "bill_to": "Restaurant Le Vieux Port",
                "raw_ocr_text": "Facturé à: Restaurant Le Vieux Port\n123 Rue du Port",
            },
            submitted_client_code="BOLDUC",
            conn=conn,
        )
        conn.close()

        assert result["mismatch_detected"] is True
        assert result["suggested_client_code"] == "VIEUXPORT"
        assert len(result["checks"]) >= 1
        assert result["checks"][0]["check"] == "company_name"

    def test_mismatch_detected_email(self, tmp_path):
        """Billing email matches a different client."""
        db = _create_test_db(tmp_path)
        _seed_clients(db)
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row

        from src.engines.client_mismatch_engine import detect_client_mismatch
        result = detect_client_mismatch(
            extracted_data={
                "billing_email": "billing@restaurantlevieuxport.ca",
            },
            submitted_client_code="BOLDUC",
            conn=conn,
        )
        conn.close()

        assert result["mismatch_detected"] is True
        assert result["suggested_client_code"] == "VIEUXPORT"

    def test_no_mismatch_when_correct_client(self, tmp_path):
        """No mismatch when bill-to matches submitted client."""
        db = _create_test_db(tmp_path)
        _seed_clients(db)
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row

        from src.engines.client_mismatch_engine import detect_client_mismatch
        result = detect_client_mismatch(
            extracted_data={
                "bill_to": "Bolduc Construction",
            },
            submitted_client_code="BOLDUC",
            conn=conn,
        )
        conn.close()

        assert result["mismatch_detected"] is False
        assert result["suggested_client_code"] is None

    def test_no_mismatch_when_no_data(self, tmp_path):
        """No mismatch when no identifying data."""
        db = _create_test_db(tmp_path)
        _seed_clients(db)
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row

        from src.engines.client_mismatch_engine import detect_client_mismatch
        result = detect_client_mismatch(
            extracted_data={},
            submitted_client_code="BOLDUC",
            conn=conn,
        )
        conn.close()

        assert result["mismatch_detected"] is False

    def test_mismatch_gst_number(self, tmp_path):
        """GST number on invoice matches a different client."""
        db = _create_test_db(tmp_path)
        _seed_clients(db)
        # Add client_config table with GST number
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT,
                key TEXT,
                value TEXT
            )
        """)
        conn.execute(
            "INSERT INTO client_config (client_code, key, value) VALUES (?, ?, ?)",
            ("VIEUXPORT", "gst_number", "123456789"),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row

        from src.engines.client_mismatch_engine import detect_client_mismatch
        result = detect_client_mismatch(
            extracted_data={
                "raw_ocr_text": "TPS/GST: 123456789 RT 0001\nTotal: $500.00",
            },
            submitted_client_code="BOLDUC",
            conn=conn,
        )
        conn.close()

        assert result["mismatch_detected"] is True
        assert result["suggested_client_code"] == "VIEUXPORT"
        assert result["checks"][0]["check"] == "gst_number"

    def test_ensure_mismatch_columns(self, tmp_path):
        """Mismatch columns are added to documents table."""
        db = _create_test_db(tmp_path)
        conn = sqlite3.connect(str(db))
        from src.engines.client_mismatch_engine import ensure_mismatch_columns
        ensure_mismatch_columns(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        conn.close()
        assert "suspected_client_mismatch" in cols
        assert "suggested_client_code" in cols


# ===========================================================================
# IMPROVEMENT 2: Auto-approval engine
# ===========================================================================

class TestAutoApproval:
    """Tests for can_auto_approve()."""

    def test_can_auto_approve_after_5_approvals(self, tmp_path):
        """Returns True when vendor has 5+ approvals, high confidence, no fraud."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-001", "BOLDUC", "Bell Canada",
                       amount=250.0, confidence=0.92)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=6, confidence=0.92)

        from src.engines.client_mismatch_engine import can_auto_approve
        result = can_auto_approve("doc-001", db_path=db)

        assert result["can_auto"] is True
        assert result["confidence"] >= 0.85
        assert result["suggested_gl"] == "5420"
        assert result["suggested_tax"] == "T"
        assert result["approval_count"] >= 5

    def test_cannot_auto_approve_insufficient_approvals(self, tmp_path):
        """Returns False when vendor has < 5 approvals."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-002", "BOLDUC", "New Vendor Co")
        _seed_vendor_memory(db, "New Vendor Co", "BOLDUC",
                            approval_count=2, confidence=0.60)

        from src.engines.client_mismatch_engine import can_auto_approve
        result = can_auto_approve("doc-002", db_path=db)

        assert result["can_auto"] is False
        assert "approbation" in result["reason"] or "approval" in result["reason_en"]

    def test_cannot_auto_approve_fraud_flag(self, tmp_path):
        """Returns False when document has HIGH/CRITICAL fraud flags."""
        db = _create_test_db(tmp_path)
        fraud = json.dumps([{"rule": "vendor_amount_anomaly", "severity": "HIGH"}])
        _seed_document(db, "doc-003", "BOLDUC", "Bell Canada",
                       fraud_flags=fraud)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=10, confidence=0.95)

        from src.engines.client_mismatch_engine import can_auto_approve
        result = can_auto_approve("doc-003", db_path=db)

        assert result["can_auto"] is False
        assert "fraude" in result["reason"].lower() or "fraud" in result["reason_en"].lower()

    def test_cannot_auto_approve_capex(self, tmp_path):
        """Returns False for CapEx category."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-004", "BOLDUC", "Bell Canada",
                       category="capex")
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=10, confidence=0.95)

        from src.engines.client_mismatch_engine import can_auto_approve
        result = can_auto_approve("doc-004", db_path=db)

        assert result["can_auto"] is False
        assert "routini" in result["reason"].lower() or "routine" in result["reason_en"].lower()

    def test_cannot_auto_approve_low_confidence(self, tmp_path):
        """Returns False when document confidence < threshold."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-005", "BOLDUC", "Bell Canada",
                       confidence=0.50)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=10, confidence=0.95)

        from src.engines.client_mismatch_engine import can_auto_approve
        result = can_auto_approve("doc-005", db_path=db)

        assert result["can_auto"] is False

    def test_cannot_auto_approve_over_max_amount(self, tmp_path):
        """Returns False when amount exceeds max threshold."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-006", "BOLDUC", "Bell Canada",
                       amount=10000.0, confidence=0.92)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=10, confidence=0.95)

        from src.engines.client_mismatch_engine import can_auto_approve
        result = can_auto_approve("doc-006", db_path=db)

        assert result["can_auto"] is False
        assert "montant" in result["reason"].lower() or "amount" in result["reason_en"].lower()

    def test_auto_approve_logs_to_audit(self, tmp_path):
        """auto_approve_document logs to audit trail."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-007", "BOLDUC", "Bell Canada",
                       amount=200.0, confidence=0.92)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=8, confidence=0.92)

        # Enable auto-approve in config
        import src.engines.client_mismatch_engine as engine
        original_fn = engine._get_learning_config

        def _mock_config():
            return {
                "auto_approve_after_n_approvals": 5,
                "auto_approve_confidence_threshold": 0.85,
                "auto_approve_max_amount": 5000,
                "auto_approve_enabled": True,
            }
        engine._get_learning_config = _mock_config
        try:
            result = engine.auto_approve_document("doc-007", db_path=db)
        finally:
            engine._get_learning_config = original_fn

        assert result["ok"] is True
        assert result["confidence"] >= 0.85

        # Check audit log
        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT * FROM audit_log WHERE event_type = 'auto_approved' AND document_id = 'doc-007'"
        ).fetchone()
        conn.close()
        assert row is not None

    def test_auto_approve_disabled_by_default(self, tmp_path):
        """auto_approve_document returns False when disabled."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-008", "BOLDUC", "Bell Canada")
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=10, confidence=0.95)

        from src.engines.client_mismatch_engine import auto_approve_document
        result = auto_approve_document("doc-008", db_path=db)

        assert result["ok"] is False
        assert "disabled" in result["reason"]


# ===========================================================================
# IMPROVEMENT 3: Learning feedback loop
# ===========================================================================

class TestLearningFeedback:
    """Tests for learning feedback connections."""

    def test_record_client_correction(self, tmp_path):
        """Client correction is recorded in client_corrections table."""
        db = _create_test_db(tmp_path)

        from src.engines.client_mismatch_engine import record_client_correction
        result = record_client_correction(
            vendor="Bell Canada",
            old_client_code="BOLDUC",
            new_client_code="VIEUXPORT",
            document_id="doc-100",
            db_path=db,
        )

        assert result["ok"] is True

        conn = sqlite3.connect(str(db))
        row = conn.execute("SELECT * FROM client_corrections WHERE document_id = 'doc-100'").fetchone()
        conn.close()
        assert row is not None

    def test_auto_approve_feedback_accepted(self, tmp_path):
        """Accepted auto-approve feedback boosts vendor memory."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-200", "BOLDUC", "Bell Canada",
                       gl_account="5420", tax_code="T")

        from src.engines.client_mismatch_engine import record_auto_approve_feedback
        result = record_auto_approve_feedback("doc-200", accepted=True, db_path=db)

        assert result["ok"] is True
        assert result["accepted"] is True

    def test_auto_approve_feedback_rejected(self, tmp_path):
        """Rejected auto-approve feedback downgrades vendor memory."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-201", "BOLDUC", "Bell Canada",
                       gl_account="5420", tax_code="T")

        from src.engines.client_mismatch_engine import record_auto_approve_feedback
        result = record_auto_approve_feedback("doc-201", accepted=False, db_path=db)

        assert result["ok"] is True
        assert result["accepted"] is False


# ===========================================================================
# IMPROVEMENT 4: Learning status icons
# ===========================================================================

class TestLearningStatusIcons:
    """Tests for get_learning_status_icon()."""

    def test_fraud_icon_shown(self, tmp_path):
        """Fraud flag results in siren icon."""
        from src.engines.client_mismatch_engine import get_learning_status_icon
        row = {
            "vendor": "Suspect Co",
            "client_code": "BOLDUC",
            "fraud_flags": json.dumps([{"rule": "duplicate_exact", "severity": "HIGH"}]),
        }
        result = get_learning_status_icon(row)
        assert result["status"] == "fraud"
        assert result["icon"] == "\U0001f6a8"

    def test_auto_approvable_icon(self, tmp_path):
        """Auto-approvable vendor gets robot icon."""
        db = _create_test_db(tmp_path)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=10, confidence=0.95)

        from src.engines.client_mismatch_engine import get_learning_status_icon
        row = {
            "vendor": "Bell Canada",
            "client_code": "BOLDUC",
            "fraud_flags": "[]",
        }
        result = get_learning_status_icon(row, db_path=db)
        assert result["status"] == "auto_approvable"
        assert result["icon"] == "\U0001f916"

    def test_learning_icon_shown(self, tmp_path):
        """Vendor with some approvals shows learning icon."""
        db = _create_test_db(tmp_path)
        _seed_vendor_memory(db, "New Vendor", "BOLDUC",
                            approval_count=2, confidence=0.50)

        from src.engines.client_mismatch_engine import get_learning_status_icon
        row = {
            "vendor": "New Vendor",
            "client_code": "BOLDUC",
            "fraud_flags": "[]",
        }
        result = get_learning_status_icon(row, db_path=db)
        assert result["status"] == "learning"
        assert result["icon"] == "\U0001f4da"
        assert result["approval_count"] == 2

    def test_needs_review_icon_default(self, tmp_path):
        """Unknown vendor shows needs-review icon."""
        db = _create_test_db(tmp_path)

        from src.engines.client_mismatch_engine import get_learning_status_icon
        row = {
            "vendor": "Unknown Co",
            "client_code": "BOLDUC",
            "fraud_flags": "[]",
        }
        result = get_learning_status_icon(row, db_path=db)
        assert result["status"] == "needs_review"
        assert result["icon"] == "\u26a0\ufe0f"


# ===========================================================================
# Learning stats
# ===========================================================================

class TestLearningStats:
    """Tests for get_learning_stats()."""

    def test_learning_stats_returns_structure(self, tmp_path):
        """Stats return all required keys."""
        db = _create_test_db(tmp_path)
        _seed_document(db, "doc-300", "BOLDUC", "Bell Canada")

        from src.engines.client_mismatch_engine import get_learning_stats
        stats = get_learning_stats(db_path=db)

        assert "total" in stats
        assert "auto_approved" in stats
        assert "suggested_correctly" in stats
        assert "needed_correction" in stats
        assert "fraud_caught" in stats
        assert "time_saved_hours" in stats
        assert stats["total"] >= 1

    def test_vendor_learning_detail(self, tmp_path):
        """Vendor learning detail returns approval count and confidence."""
        db = _create_test_db(tmp_path)
        _seed_vendor_memory(db, "Bell Canada", "BOLDUC",
                            approval_count=8, confidence=0.94)

        from src.engines.client_mismatch_engine import get_vendor_learning_detail
        detail = get_vendor_learning_detail("Bell Canada", "BOLDUC", db_path=db)

        assert detail["approval_count"] == 8
        assert detail["gl_confidence"] >= 0.90
        assert detail["can_auto_next"] is True

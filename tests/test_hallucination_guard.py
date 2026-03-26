"""
tests/test_hallucination_guard.py
==================================
pytest tests for src/agents/core/hallucination_guard.py

Covers
------
1. verify_numeric_totals — match, mismatch, skipped, GST/QST from taxes array,
   fallback to tax_total, tolerance edge cases
2. verify_ai_output — vendor checks, amount checks, date checks,
   gl_account pattern, tax_code validation, confidence threshold
3. record_math_mismatch — DB write, idempotency (duplicate replacement)
4. set_hallucination_suspected — DB write
5. track_correction_count — threshold logic, audit_log entry, return value
6. migrate_db declares the new columns
7. review_dashboard exposes hallucination_warning i18n key in EN and FR
8. ocr_engine returns hallucination_suspected in process_file result
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    """In-memory documents + audit_log tables matching the real schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE documents (
            document_id            TEXT PRIMARY KEY,
            file_name              TEXT,
            file_path              TEXT,
            client_code            TEXT,
            vendor                 TEXT,
            doc_type               TEXT,
            amount                 REAL,
            document_date          TEXT,
            review_status          TEXT DEFAULT 'NeedsReview',
            confidence             REAL DEFAULT 0.5,
            raw_result             TEXT,
            created_at             TEXT,
            updated_at             TEXT,
            fraud_flags            TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            correction_count        INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE audit_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type     TEXT    NOT NULL DEFAULT 'ai_call',
            username       TEXT,
            document_id    TEXT,
            provider       TEXT,
            task_type      TEXT,
            prompt_snippet TEXT,
            latency_ms     INTEGER,
            created_at     TEXT    NOT NULL DEFAULT ''
        )
    """)
    conn.commit()
    return conn


def _insert_doc(conn: sqlite3.Connection, doc_id: str, **kwargs: Any) -> None:
    defaults: dict[str, Any] = {
        "file_name": "test.pdf",
        "file_path": "/tmp/test.pdf",
        "client_code": "TEST",
        "vendor": "ACME Inc",
        "doc_type": "invoice",
        "amount": 100.0,
        "document_date": "2025-01-15",
        "review_status": "New",
        "confidence": 0.9,
        "raw_result": "{}",
        "created_at": "2025-01-01T00:00:00+00:00",
        "updated_at": "2025-01-01T00:00:00+00:00",
        "fraud_flags": None,
        "hallucination_suspected": 0,
        "correction_count": 0,
    }
    defaults.update(kwargs)
    defaults["document_id"] = doc_id
    conn.execute(
        """INSERT INTO documents
           (document_id, file_name, file_path, client_code, vendor, doc_type,
            amount, document_date, review_status, confidence, raw_result,
            created_at, updated_at, fraud_flags, hallucination_suspected, correction_count)
           VALUES
           (:document_id, :file_name, :file_path, :client_code, :vendor, :doc_type,
            :amount, :document_date, :review_status, :confidence, :raw_result,
            :created_at, :updated_at, :fraud_flags, :hallucination_suspected, :correction_count)""",
        defaults,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# 1. verify_numeric_totals
# ---------------------------------------------------------------------------

from src.agents.core.hallucination_guard import (
    verify_numeric_totals,
    verify_ai_output,
    record_math_mismatch,
    set_hallucination_suspected,
    track_correction_count,
    MATH_TOLERANCE,
    CONFIDENCE_THRESHOLD,
)


class TestVerifyNumericTotals:
    def test_match_with_gst_qst_taxes_array(self):
        result = {
            "subtotal": 100.00,
            "total":    105.00,
            "taxes": [
                {"type": "GST", "amount": 5.00},
            ],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True
        assert out["skipped"] is False
        assert out["delta"] == pytest.approx(0.0, abs=1e-4)

    def test_match_gst_and_qst(self):
        result = {
            "subtotal": 100.00,
            "total":    114.975,
            "taxes": [
                {"type": "GST", "amount": 5.00},
                {"type": "QST", "amount": 9.975},
            ],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True

    def test_mismatch_exceeds_tolerance(self):
        result = {
            "subtotal": 100.00,
            "total":    120.00,
            "taxes": [
                {"type": "GST", "amount": 5.00},
            ],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is False
        assert out["delta"] > MATH_TOLERANCE

    def test_within_tolerance(self):
        # delta = 0.01 which is <= MATH_TOLERANCE (0.02)
        result = {
            "subtotal": 100.00,
            "total":    105.01,
            "taxes": [{"type": "GST", "amount": 5.00}],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True
        assert out["delta"] == pytest.approx(0.01, abs=1e-4)

    def test_exactly_at_tolerance_boundary(self):
        # delta = 0.02 which is == MATH_TOLERANCE → ok
        result = {
            "subtotal": 100.00,
            "total":    105.02,
            "taxes": [{"type": "GST", "amount": 5.00}],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True

    def test_just_above_tolerance(self):
        # delta = 0.021 → not ok
        result = {
            "subtotal": 100.00,
            "total":    105.021,
            "taxes": [{"type": "GST", "amount": 5.00}],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is False

    def test_fallback_to_tax_total_when_no_gst_qst(self):
        # taxes array has PST only → falls back to tax_total
        result = {
            "subtotal":  100.00,
            "total":     107.00,
            "tax_total": 7.00,
            "taxes": [{"type": "PST", "amount": 7.00}],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True
        assert out["skipped"] is False

    def test_fallback_to_tax_total_empty_taxes(self):
        result = {
            "subtotal":  200.00,
            "total":     210.00,
            "tax_total": 10.00,
            "taxes": [],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True

    def test_skipped_when_subtotal_missing(self):
        result = {"total": 100.00, "taxes": []}
        out = verify_numeric_totals(result)
        assert out["skipped"] is True
        assert out["ok"] is True

    def test_skipped_when_total_missing(self):
        result = {"subtotal": 100.00, "taxes": []}
        out = verify_numeric_totals(result)
        assert out["skipped"] is True

    def test_skipped_when_tax_total_also_missing(self):
        result = {"subtotal": 100.00, "total": 110.00, "taxes": []}
        out = verify_numeric_totals(result)
        assert out["skipped"] is True

    def test_skipped_on_invalid_subtotal(self):
        result = {"subtotal": "bad", "total": 100.00}
        out = verify_numeric_totals(result)
        assert out["skipped"] is True

    def test_hst_ignored_falls_back_to_tax_total(self):
        result = {
            "subtotal":  100.00,
            "total":     113.00,
            "tax_total": 13.00,
            "taxes": [{"type": "HST", "amount": 13.00}],
        }
        out = verify_numeric_totals(result)
        assert out["ok"] is True


# ---------------------------------------------------------------------------
# 2. verify_ai_output
# ---------------------------------------------------------------------------

class TestVerifyAIOutput:

    def _today(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _make_good(self, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "vendor_name":   "Staples Canada",
            "total":         100.00,
            "document_date": self._today(),
            "confidence":    0.9,
        }
        base.update(overrides)
        return base

    def test_clean_result_no_failures(self):
        out = verify_ai_output(self._make_good())
        assert out["hallucination_suspected"] is False
        assert out["review_status"] is None
        assert out["failures"] == []

    # --- vendor ---
    def test_empty_vendor_fails(self):
        out = verify_ai_output(self._make_good(vendor_name=""))
        assert out["hallucination_suspected"] is True
        assert any("empty" in f for f in out["failures"])

    def test_vendor_too_short_fails(self):
        out = verify_ai_output(self._make_good(vendor_name="X"))
        assert out["hallucination_suspected"] is True

    def test_vendor_too_long_fails(self):
        out = verify_ai_output(self._make_good(vendor_name="A" * 101))
        assert out["hallucination_suspected"] is True

    def test_vendor_random_chars_fails(self):
        out = verify_ai_output(self._make_good(vendor_name="XZXZXZXZX"))
        assert out["hallucination_suspected"] is True

    def test_vendor_with_spaces_passes(self):
        out = verify_ai_output(self._make_good(vendor_name="Bell Canada Inc"))
        assert out["hallucination_suspected"] is False

    # --- amount ---
    def test_amount_too_small_fails(self):
        out = verify_ai_output(self._make_good(total=0.005))
        assert out["hallucination_suspected"] is True

    def test_amount_zero_fails(self):
        out = verify_ai_output(self._make_good(total=0.0))
        assert out["hallucination_suspected"] is True

    def test_amount_negative_fails(self):
        out = verify_ai_output(self._make_good(total=-10.00))
        assert out["hallucination_suspected"] is True

    def test_amount_too_large_fails(self):
        out = verify_ai_output(self._make_good(total=500_000.01))
        assert out["hallucination_suspected"] is True

    def test_amount_max_minus_one_passes(self):
        out = verify_ai_output(self._make_good(total=499_999.99))
        assert out["hallucination_suspected"] is False

    def test_amount_missing_passes(self):
        r = self._make_good()
        del r["total"]
        out = verify_ai_output(r)
        # Only vendor + date + confidence provided; amount absent → no amount failure
        assert not any("amount" in f for f in out["failures"])

    def test_amount_not_a_number_fails(self):
        out = verify_ai_output(self._make_good(total="abc"))
        assert out["hallucination_suspected"] is True

    # --- date ---
    def test_date_too_far_past_fails(self):
        old_date = (datetime.now(timezone.utc).date() - timedelta(days=365 * 6)).isoformat()
        out = verify_ai_output(self._make_good(document_date=old_date))
        assert out["hallucination_suspected"] is True

    def test_date_too_far_future_fails(self):
        future_date = (datetime.now(timezone.utc).date() + timedelta(days=8)).isoformat()
        out = verify_ai_output(self._make_good(document_date=future_date))
        assert out["hallucination_suspected"] is True

    def test_date_7_days_future_passes(self):
        future_date = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
        out = verify_ai_output(self._make_good(document_date=future_date))
        assert not any("future" in f for f in out["failures"])

    def test_date_invalid_format_fails(self):
        out = verify_ai_output(self._make_good(document_date="15-01-2025"))
        assert out["hallucination_suspected"] is True

    def test_date_missing_passes(self):
        r = self._make_good()
        del r["document_date"]
        out = verify_ai_output(r)
        assert not any("date" in f for f in out["failures"])

    # --- gl_account ---
    def test_gl_account_valid_passes(self):
        out = verify_ai_output(self._make_good(gl_account="5200-EXP"))
        assert not any("gl_account" in f for f in out["failures"])

    def test_gl_account_invalid_chars_fails(self):
        out = verify_ai_output(self._make_good(gl_account="5200 EXP!"))
        assert out["hallucination_suspected"] is True

    def test_gl_account_empty_passes(self):
        out = verify_ai_output(self._make_good(gl_account=""))
        assert not any("gl_account" in f for f in out["failures"])

    # --- tax_code ---
    def test_valid_tax_codes(self):
        for code in ("T", "Z", "E", "M", "I"):
            out = verify_ai_output(self._make_good(tax_code=code))
            assert not any("tax_code" in f for f in out["failures"]), code

    def test_invalid_tax_code_fails(self):
        out = verify_ai_output(self._make_good(tax_code="X"))
        assert out["hallucination_suspected"] is True

    def test_empty_tax_code_passes(self):
        out = verify_ai_output(self._make_good(tax_code=""))
        assert not any("tax_code" in f for f in out["failures"])

    # --- confidence ---
    def test_low_confidence_fails(self):
        out = verify_ai_output(self._make_good(confidence=0.65))
        assert out["hallucination_suspected"] is True
        assert any("confidence" in f for f in out["failures"])

    def test_confidence_exactly_threshold_passes(self):
        out = verify_ai_output(self._make_good(confidence=CONFIDENCE_THRESHOLD))
        assert not any("confidence" in f for f in out["failures"])

    def test_confidence_just_below_fails(self):
        out = verify_ai_output(self._make_good(confidence=CONFIDENCE_THRESHOLD - 0.001))
        assert out["hallucination_suspected"] is True

    def test_review_status_set_on_failure(self):
        out = verify_ai_output(self._make_good(vendor_name=""))
        assert out["review_status"] == "NeedsReview"

    def test_review_status_none_on_pass(self):
        out = verify_ai_output(self._make_good())
        assert out["review_status"] is None

    def test_multiple_failures_all_reported(self):
        out = verify_ai_output({"vendor_name": "", "total": -1.0, "document_date": "bad"})
        assert len(out["failures"]) >= 2


# ---------------------------------------------------------------------------
# 3. record_math_mismatch
# ---------------------------------------------------------------------------

class TestRecordMathMismatch:

    def test_adds_fraud_flag_to_db(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = _make_db()
        # Use tmp_path for a real file-based DB
        real_conn = sqlite3.connect(str(db_path))
        real_conn.row_factory = sqlite3.Row
        real_conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                fraud_flags TEXT,
                review_status TEXT DEFAULT 'New',
                updated_at TEXT
            )
        """)
        real_conn.execute(
            "INSERT INTO documents (document_id, fraud_flags, review_status) VALUES (?, ?, ?)",
            ("doc_abc", None, "New"),
        )
        real_conn.commit()
        real_conn.close()

        record_math_mismatch("doc_abc", 0.05, 105.05, 105.10, db_path=db_path)

        conn2 = sqlite3.connect(str(db_path))
        conn2.row_factory = sqlite3.Row
        row = conn2.execute("SELECT fraud_flags, review_status FROM documents WHERE document_id='doc_abc'").fetchone()
        conn2.close()

        flags = json.loads(row["fraud_flags"])
        assert len(flags) == 1
        assert flags[0]["rule"] == "math_mismatch"
        assert flags[0]["severity"] == "high"
        assert row["review_status"] == "NeedsReview"

    def test_idempotent_replaces_existing_flag(self, tmp_path):
        db_path = tmp_path / "test2.db"
        existing_flags = json.dumps([{"rule": "math_mismatch", "severity": "high", "note": "old"}])
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                fraud_flags TEXT,
                review_status TEXT DEFAULT 'New',
                updated_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO documents (document_id, fraud_flags, review_status) VALUES (?, ?, ?)",
            ("doc_xyz", existing_flags, "New"),
        )
        conn.commit()
        conn.close()

        record_math_mismatch("doc_xyz", 0.10, 110.10, 110.20, db_path=db_path)

        conn2 = sqlite3.connect(str(db_path))
        conn2.row_factory = sqlite3.Row
        row = conn2.execute("SELECT fraud_flags FROM documents WHERE document_id='doc_xyz'").fetchone()
        conn2.close()
        flags = json.loads(row["fraud_flags"])
        # Only one math_mismatch flag should exist
        mm_flags = [f for f in flags if f["rule"] == "math_mismatch"]
        assert len(mm_flags) == 1

    def test_preserves_other_flags(self, tmp_path):
        db_path = tmp_path / "test3.db"
        existing = [{"rule": "duplicate_exact", "severity": "high", "note": "dupe"}]
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                fraud_flags TEXT,
                review_status TEXT DEFAULT 'New',
                updated_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO documents (document_id, fraud_flags, review_status) VALUES (?, ?, ?)",
            ("doc_p", json.dumps(existing), "New"),
        )
        conn.commit()
        conn.close()

        record_math_mismatch("doc_p", 0.05, 105.05, 105.10, db_path=db_path)

        conn2 = sqlite3.connect(str(db_path))
        conn2.row_factory = sqlite3.Row
        row = conn2.execute("SELECT fraud_flags FROM documents WHERE document_id='doc_p'").fetchone()
        conn2.close()
        flags = json.loads(row["fraud_flags"])
        rules = {f["rule"] for f in flags}
        assert "duplicate_exact" in rules
        assert "math_mismatch" in rules

    def test_noop_on_missing_document(self, tmp_path):
        db_path = tmp_path / "test4.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                fraud_flags TEXT,
                review_status TEXT DEFAULT 'New',
                updated_at TEXT
            )
        """)
        conn.commit()
        conn.close()
        # Should not raise
        record_math_mismatch("nonexistent", 0.05, 0.0, 0.05, db_path=db_path)


# ---------------------------------------------------------------------------
# 4. set_hallucination_suspected
# ---------------------------------------------------------------------------

class TestSetHallucinationSuspected:

    def test_sets_flag_and_status(self, tmp_path):
        db_path = tmp_path / "h.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                hallucination_suspected INTEGER DEFAULT 0,
                review_status TEXT DEFAULT 'New',
                updated_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO documents (document_id, review_status) VALUES (?, ?)",
            ("doc_h", "New"),
        )
        conn.commit()
        conn.close()

        set_hallucination_suspected("doc_h", ["vendor_name is empty"], db_path=db_path)

        conn2 = sqlite3.connect(str(db_path))
        conn2.row_factory = sqlite3.Row
        row = conn2.execute("SELECT hallucination_suspected, review_status FROM documents WHERE document_id='doc_h'").fetchone()
        conn2.close()
        assert row["hallucination_suspected"] == 1
        assert row["review_status"] == "NeedsReview"

    def test_noop_on_missing_document(self, tmp_path):
        db_path = tmp_path / "h2.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                hallucination_suspected INTEGER DEFAULT 0,
                review_status TEXT DEFAULT 'New',
                updated_at TEXT
            )
        """)
        conn.commit()
        conn.close()
        set_hallucination_suspected("nonexistent", [], db_path=db_path)


# ---------------------------------------------------------------------------
# 5. track_correction_count
# ---------------------------------------------------------------------------

class TestTrackCorrectionCount:

    def _make_file_db(self, tmp_path: Path) -> Path:
        db_path = tmp_path / "cc.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id   TEXT PRIMARY KEY,
                vendor        TEXT,
                client_code   TEXT,
                doc_type      TEXT,
                amount        REAL,
                document_date TEXT,
                gl_account    TEXT,
                tax_code      TEXT,
                category      TEXT,
                review_status TEXT,
                correction_count INTEGER DEFAULT 0,
                updated_at    TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE audit_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type     TEXT,
                document_id    TEXT,
                task_type      TEXT,
                prompt_snippet TEXT,
                created_at     TEXT
            )
        """)
        conn.execute(
            """INSERT INTO documents
               (document_id, vendor, client_code, doc_type, amount,
                document_date, gl_account, tax_code, category, review_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("doc_cc", "ACME", "CLIENT1", "invoice", 100.0,
             "2025-01-15", "5200", "T", "office", "New"),
        )
        conn.commit()
        conn.close()
        return db_path

    def _make_before_row(self, **kwargs: Any) -> dict[str, Any]:
        base = {
            "vendor": "ACME", "client_code": "CLIENT1", "doc_type": "invoice",
            "amount": 100.0, "document_date": "2025-01-15", "gl_account": "5200",
            "tax_code": "T", "category": "office", "review_status": "New",
        }
        base.update(kwargs)
        return base

    def test_returns_zero_when_nothing_changed(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()
        submitted = dict(before)
        n = track_correction_count("doc_cc", before, submitted, db_path=db_path)
        assert n == 0

    def test_returns_correct_count(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()
        submitted = dict(before)
        submitted["vendor"] = "NewVendor"
        submitted["amount"] = "200"
        submitted["category"] = "travel"
        n = track_correction_count("doc_cc", before, submitted, db_path=db_path)
        assert n == 3

    def test_does_not_increment_when_at_or_below_threshold(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()
        submitted = dict(before)
        submitted["vendor"] = "NewVendor"
        submitted["amount"] = "200"  # exactly 2 changes = CORRECTION_THRESHOLD, not "more than"
        n = track_correction_count("doc_cc", before, submitted, db_path=db_path)
        assert n == 2
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT correction_count FROM documents WHERE document_id='doc_cc'").fetchone()
        conn.close()
        assert row["correction_count"] == 0  # not incremented

    def test_increments_correction_count_when_above_threshold(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()
        submitted = dict(before)
        submitted["vendor"] = "NewVendor"
        submitted["amount"] = "200"
        submitted["category"] = "travel"
        track_correction_count("doc_cc", before, submitted, db_path=db_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT correction_count FROM documents WHERE document_id='doc_cc'").fetchone()
        conn.close()
        assert row["correction_count"] == 1

    def test_writes_audit_log_entry(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()
        submitted = dict(before)
        submitted["vendor"] = "NewVendor"
        submitted["amount"] = "200"
        submitted["category"] = "travel"
        track_correction_count("doc_cc", before, submitted, db_path=db_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE document_id='doc_cc' AND event_type='multi_field_correction'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1
        assert "vendor" in rows[0]["prompt_snippet"]

    def test_multiple_saves_accumulate(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()

        for _ in range(3):
            submitted = dict(before)
            submitted["vendor"] = "NewVendor"
            submitted["amount"] = "200"
            submitted["category"] = "travel"
            track_correction_count("doc_cc", before, submitted, db_path=db_path)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT correction_count FROM documents WHERE document_id='doc_cc'").fetchone()
        conn.close()
        assert row["correction_count"] == 3

    def test_ignores_non_tracked_fields(self, tmp_path):
        db_path = self._make_file_db(tmp_path)
        before = self._make_before_row()
        submitted = {"file_name": "new.pdf", "raw_result": "{}"}  # non-tracked
        n = track_correction_count("doc_cc", before, submitted, db_path=db_path)
        assert n == 0


# ---------------------------------------------------------------------------
# 6. migrate_db declares the new columns
# ---------------------------------------------------------------------------

class TestMigrateDBColumns:
    def test_documents_columns_declared(self):
        """migrate_db.py must add the new columns to documents."""
        from scripts.migrate_db import add_missing
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Create a minimal documents table without the new columns
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT,
                review_status TEXT,
                confidence REAL
            )
        """)
        added = add_missing(conn, "documents", [
            ("raw_ocr_text",           "TEXT"),
            ("hallucination_suspected", "INTEGER NOT NULL DEFAULT 0"),
            ("correction_count",        "INTEGER NOT NULL DEFAULT 0"),
        ])
        assert "raw_ocr_text" in added
        assert "hallucination_suspected" in added
        assert "correction_count" in added

    def test_confidence_column_migration(self):
        """confidence column must be added when missing."""
        from scripts.migrate_db import add_missing
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT
            )
        """)
        added = add_missing(conn, "documents", [("confidence", "REAL")])
        assert "confidence" in added


# ---------------------------------------------------------------------------
# 7. i18n keys
# ---------------------------------------------------------------------------

class TestI18nKeys:
    def _load(self, lang: str) -> dict:
        path = ROOT / "src" / "i18n" / f"{lang}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_en_hallucination_warning(self):
        en = self._load("en")
        assert "hallucination_warning" in en
        assert len(en["hallucination_warning"]) > 10

    def test_fr_hallucination_warning(self):
        fr = self._load("fr")
        assert "hallucination_warning" in fr
        assert len(fr["hallucination_warning"]) > 10

    def test_en_section_raw_ocr(self):
        en = self._load("en")
        assert "section_raw_ocr" in en

    def test_fr_section_raw_ocr(self):
        fr = self._load("fr")
        assert "section_raw_ocr" in fr

    def test_en_err_math_mismatch(self):
        en = self._load("en")
        assert "err_math_mismatch" in en

    def test_fr_err_math_mismatch(self):
        fr = self._load("fr")
        assert "err_math_mismatch" in fr

    def test_warning_messages_differ_between_languages(self):
        en = self._load("en")
        fr = self._load("fr")
        assert en["hallucination_warning"] != fr["hallucination_warning"]


# ---------------------------------------------------------------------------
# 8. ocr_engine.process_file returns hallucination_suspected
# ---------------------------------------------------------------------------

class TestOCREngineHallucinationHook:
    """
    Smoke-test that process_file() exposes the hallucination_suspected key.
    We patch the extraction step to return a known bad result.
    """

    def test_process_file_returns_hallucination_suspected_key(self, tmp_path, monkeypatch):
        """process_file must include hallucination_suspected in its return dict."""
        from src.engines import ocr_engine

        # Patch call_vision to return a result that will fail field validation
        bad_extraction = {
            "doc_type":      "invoice",
            "vendor_name":   "",           # empty vendor → hallucination_suspected=True
            "document_date": None,
            "invoice_number": None,
            "currency":      "CAD",
            "subtotal":      None,
            "tax_total":     None,
            "total":         None,
            "taxes":         [],
            "confidence":    0.95,
            "notes":         "",
        }
        monkeypatch.setattr(ocr_engine, "call_vision", lambda *_a, **_kw: bad_extraction)

        db_path = tmp_path / "test.db"
        upload_dir = tmp_path / "uploads"
        # Minimal PNG bytes (1×1 white pixel)
        png_bytes = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00"
            b"\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18"
            b"\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        # Create the DB with minimum schema
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT, file_path TEXT, client_code TEXT,
                vendor TEXT, doc_type TEXT, amount REAL,
                document_date TEXT, review_status TEXT,
                confidence REAL, raw_result TEXT,
                created_at TEXT, updated_at TEXT,
                submitted_by TEXT, client_note TEXT,
                currency TEXT, subtotal REAL, tax_total REAL,
                extraction_method TEXT, ingest_source TEXT,
                raw_ocr_text TEXT, hallucination_suspected INTEGER DEFAULT 0,
                correction_count INTEGER DEFAULT 0, fraud_flags TEXT
            )
        """)
        conn.commit()
        conn.close()

        result = ocr_engine.process_file(
            png_bytes, "test.png",
            client_code="TEST",
            db_path=db_path,
            upload_dir=upload_dir,
        )
        assert "hallucination_suspected" in result
        assert result["hallucination_suspected"] is True

    def test_process_file_not_suspected_with_good_extraction(self, tmp_path, monkeypatch):
        from src.engines import ocr_engine

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        good_extraction = {
            "doc_type":      "invoice",
            "vendor_name":   "Staples Canada",
            "document_date": today,
            "invoice_number": "INV-001",
            "currency":      "CAD",
            "subtotal":      100.00,
            "tax_total":     5.00,
            "total":         105.00,
            "taxes":         [{"type": "GST", "amount": 5.00}],
            "confidence":    0.92,
            "notes":         "",
        }
        monkeypatch.setattr(ocr_engine, "call_vision", lambda *_a, **_kw: good_extraction)
        monkeypatch.setattr(ocr_engine, "detect_handwriting", lambda *_a, **_kw: 0.0)

        db_path = tmp_path / "test2.db"
        upload_dir = tmp_path / "uploads2"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT, file_path TEXT, client_code TEXT,
                vendor TEXT, doc_type TEXT, amount REAL,
                document_date TEXT, review_status TEXT,
                confidence REAL, raw_result TEXT,
                created_at TEXT, updated_at TEXT,
                submitted_by TEXT, client_note TEXT,
                currency TEXT, subtotal REAL, tax_total REAL,
                extraction_method TEXT, ingest_source TEXT,
                raw_ocr_text TEXT, hallucination_suspected INTEGER DEFAULT 0,
                correction_count INTEGER DEFAULT 0, fraud_flags TEXT
            )
        """)
        conn.commit()
        conn.close()

        png_bytes = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00"
            b"\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18"
            b"\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        result = ocr_engine.process_file(
            png_bytes, "test.png",
            client_code="TEST",
            db_path=db_path,
            upload_dir=upload_dir,
        )
        assert "hallucination_suspected" in result
        assert result["hallucination_suspected"] is False


# ---------------------------------------------------------------------------
# 9. review_dashboard exposes hallucination functions (smoke)
# ---------------------------------------------------------------------------

class TestReviewDashboardSmoke:
    def test_review_dashboard_imports_without_error(self):
        """review_dashboard.py must be importable."""
        import importlib
        import sys
        # Guard: avoid re-running global side effects
        if "scripts.review_dashboard" not in sys.modules:
            spec = importlib.util.spec_from_file_location(
                "scripts.review_dashboard",
                ROOT / "scripts" / "review_dashboard.py",
            )
            # We only check the file is parse-valid via compile
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        compile(src, "review_dashboard.py", "exec")  # syntax check

    def test_render_document_signature_unchanged(self):
        """render_document function must still exist (not renamed)."""
        import ast
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "render_document" in func_names

    def test_hallucination_guard_referenced_in_dashboard(self):
        """review_dashboard.py must reference hallucination_guard."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "hallucination_guard" in src

    def test_math_mismatch_check_in_qbo_approve(self):
        """The /qbo/approve handler must contain a numeric totals check."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "verify_numeric_totals" in src

    def test_correction_count_tracked_in_document_update(self):
        """The /document/update handler must call track_correction_count."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "track_correction_count" in src

    def test_hallucination_warning_rendered(self):
        """render_document must reference hallucination_warning i18n key."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "hallucination_warning" in src

    def test_raw_ocr_section_rendered(self):
        """render_document must reference section_raw_ocr i18n key."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "section_raw_ocr" in src

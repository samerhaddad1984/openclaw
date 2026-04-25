"""Layer 2: ingest must not write silent default GL/category.

The OCR engine used to set gl_account='5440' / category='operating_expense'
whenever it couldn't classify (rcpt_16.png hit this path because the
document had no readable vendor at all). Layer 1 covered display; this
file pins the underlying data: when OCR can't classify, the columns
are NULL and needs_categorization=1.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap_documents(db_path: Path) -> None:
    """Minimal documents table; ocr_engine._ensure_columns will add
    the rest. We must NOT call rd.bootstrap_schema here because it
    pulls in clients/firms tables that this test doesn't care about."""
    c = sqlite3.connect(str(db_path))
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT, confidence REAL,
            raw_result TEXT, created_at TEXT, updated_at TEXT,
            submitted_by TEXT, client_note TEXT
        );
        """
    )
    c.commit(); c.close()


def test_ocr_no_signal_leaves_gl_null(tmp_path):
    """When the vendor classification cascade falls through to the
    'else' branch, gl_account must be empty (was '5440')."""
    from src.engines.ocr_engine import upsert_document, _ensure_columns
    db = tmp_path / 'ocr.db'
    _bootstrap_documents(db)

    record = {
        "document_id": "doc_FAIL",
        "file_name": "rcpt_16.png",
        "file_path": "/tmp/rcpt_16.png",
        "client_code": "CLI1",
        "vendor": None,
        "doc_type": None,
        "amount": None,
        "document_date": None,
        # The OCR cascade has finished and left gl_account empty —
        # simulate that by explicitly omitting / nulling it. The new
        # behaviour must NOT replace it with '5440'.
        "gl_account": None,
        "tax_code": None,
        "category": None,
        "review_status": "NeedsReview",
        "confidence": 0.0,
        "raw_result": "{}",
        "created_at": "2026-04-25T00:00:00",
        "updated_at": "2026-04-25T00:00:00",
        "submitted_by": "ocr",
        "client_note": "",
        "currency": None,
        "subtotal": None,
        "tax_total": None,
        "extraction_method": None,
        "ingest_source": "ocr",
    }
    upsert_document(record, db_path=db)

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT gl_account, category, needs_categorization "
            "FROM documents WHERE document_id='doc_FAIL'"
        ).fetchone()
    assert row[0] is None, f"gl_account should be NULL, got {row[0]!r}"
    assert row[1] is None, f"category should be NULL, got {row[1]!r}"
    assert row[2] == 1, (
        f"needs_categorization should be 1 for failed-OCR doc, got {row[2]!r}"
    )


def test_ocr_signal_present_keeps_gl(tmp_path):
    """When the OCR cascade DID classify, the assigned GL is kept and
    needs_categorization stays 0."""
    from src.engines.ocr_engine import upsert_document
    db = tmp_path / 'ocr_ok.db'
    _bootstrap_documents(db)
    record = {
        "document_id": "doc_OK",
        "file_name": "bell_invoice.pdf",
        "file_path": "/tmp/bell.pdf",
        "client_code": "CLI1",
        "vendor": "Bell Canada",
        "doc_type": "invoice",
        "amount": 95.42,
        "document_date": "2026-04-01",
        "gl_account": "5400",  # telecom
        "tax_code": "T",
        "category": "telecom",
        "review_status": "Ready",
        "confidence": 0.93,
        "raw_result": "{}",
        "created_at": "2026-04-25T00:00:00",
        "updated_at": "2026-04-25T00:00:00",
        "submitted_by": "ocr",
        "client_note": "",
        "currency": "CAD",
        "subtotal": 86.74,
        "tax_total": 8.68,
        "extraction_method": "ai",
        "ingest_source": "ocr",
    }
    upsert_document(record, db_path=db)

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT gl_account, category, needs_categorization "
            "FROM documents WHERE document_id='doc_OK'"
        ).fetchone()
    assert row[0] == "5400"
    assert row[1] == "telecom"
    assert row[2] == 0


def test_ocr_engine_no_5440_fallback_in_classifier_cascade():
    """Read-the-source check: the unconditional `else: result['gl_account'] = '5440'`
    branch must be gone. We grep the source rather than trying to
    drive the full OCR pipeline."""
    src = (ROOT / 'src/engines/ocr_engine.py').read_text()
    # The exact silent-default line must not exist.
    assert "result[\"gl_account\"] = \"5440\"" not in src, (
        "ocr_engine.py still has the silent '5440' default — this is the rcpt_16 bug"
    )
    # Belt + suspenders: the dotted-string variant doesn't exist either.
    assert "result['gl_account'] = '5440'" not in src


def test_ai_validator_invalid_gl_clears_instead_of_defaulting(tmp_path):
    """ai_validator.validate_document_extraction used to rewrite an
    invalid gl_account to '5440'. Now it clears the value and flags
    needs_categorization."""
    from src.engines.ai_validator import validate_document_extraction
    result = {
        "vendor": "Acme",
        "amount": 42.00,
        "gl_account": "9999",  # invalid
        "tax_code": "T",
    }
    cleaned, errors = validate_document_extraction(result)
    assert cleaned["gl_account"] == "", (
        f"expected gl_account cleared, got {cleaned['gl_account']!r}"
    )
    assert cleaned.get("needs_categorization") is True
    assert any("Invalid GL account" in e for e in errors)


def test_ai_validator_valid_gl_preserved():
    from src.engines.ai_validator import validate_document_extraction
    result = {
        "vendor": "Acme",
        "amount": 100.0,
        "gl_account": "5400",
        "tax_code": "T",
    }
    cleaned, errors = validate_document_extraction(result)
    assert cleaned["gl_account"] == "5400"
    assert "needs_categorization" not in cleaned or not cleaned["needs_categorization"]


# ---------------------------------------------------------------------------
# Backfill flagging script — flags only, doesn't modify
# ---------------------------------------------------------------------------


def _bootstrap_full(db_path: Path) -> None:
    """Same as _bootstrap_documents but also adds needs_categorization
    so the backfill script doesn't bail."""
    c = sqlite3.connect(str(db_path))
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT, confidence REAL,
            raw_result TEXT, created_at TEXT, updated_at TEXT,
            submitted_by TEXT, client_note TEXT,
            has_line_items INTEGER DEFAULT 0,
            needs_categorization INTEGER DEFAULT 0
        );
        """
    )
    c.commit(); c.close()


def test_backfill_flags_silently_defaulted_doc(tmp_path):
    """Doc with the silent-default fingerprint gets flagged."""
    from scripts.maintenance.flag_silently_defaulted_documents import (
        find_suspect_docs, flag_docs,
    )
    db = tmp_path / "bf.db"
    _bootstrap_full(db)
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, file_name, vendor, "
            "amount, gl_account, category, has_line_items, "
            "needs_categorization, created_at) "
            "VALUES ('SILENT', 'rcpt.png', NULL, NULL, '5440', "
            "'operating_expense', 0, 0, datetime('now'))"
        )
        conn.commit()
        suspects = find_suspect_docs(conn, limit=100)
        assert len(suspects) == 1
        assert suspects[0]["document_id"] == "SILENT"
        # Flag and verify
        n = flag_docs(conn, ["SILENT"])
        assert n == 1
        row = conn.execute(
            "SELECT needs_categorization, gl_account, category "
            "FROM documents WHERE document_id='SILENT'"
        ).fetchone()
    assert row[0] == 1, "needs_categorization not set"
    # Critical: gl_account / category were NOT modified.
    assert row[1] == '5440', "backfill must not modify gl_account"
    assert row[2] == 'operating_expense', "backfill must not modify category"


def test_backfill_skips_real_data(tmp_path):
    """A doc with vendor + amount is not a silent-default match,
    even if gl_account happens to be 5440."""
    from scripts.maintenance.flag_silently_defaulted_documents import (
        find_suspect_docs,
    )
    db = tmp_path / "bf2.db"
    _bootstrap_full(db)
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, file_name, vendor, "
            "amount, gl_account, category, has_line_items, created_at) "
            "VALUES ('REAL', 'invoice.pdf', 'Acme Office Supplies', "
            "42.50, '5440', 'operating_expense', 0, datetime('now'))"
        )
        conn.commit()
        suspects = find_suspect_docs(conn, limit=100)
    # Real data with vendor+amount — must NOT be flagged.
    assert all(s["document_id"] != "REAL" for s in suspects), (
        "backfill flagged a doc that has real vendor+amount"
    )


def test_backfill_skips_multi_line_doc(tmp_path):
    """has_line_items=1 means lines are the source of truth and
    doc-level 5440 is irrelevant; don't flag."""
    from scripts.maintenance.flag_silently_defaulted_documents import (
        find_suspect_docs,
    )
    db = tmp_path / "bf3.db"
    _bootstrap_full(db)
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, file_name, vendor, "
            "amount, gl_account, category, has_line_items, created_at) "
            "VALUES ('LINES', 'inv.pdf', NULL, NULL, '5440', "
            "'operating_expense', 1, datetime('now'))"
        )
        conn.commit()
        suspects = find_suspect_docs(conn, limit=100)
    assert all(s["document_id"] != "LINES" for s in suspects)

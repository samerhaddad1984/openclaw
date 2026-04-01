"""
Shared fixtures and helpers for the A-Z adversarial campaign.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CENT = Decimal("0.01")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def fresh_db() -> sqlite3.Connection:
    """In-memory SQLite DB with row_factory + WAL + FK."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def doc_id() -> str:
    return f"doc-{uuid.uuid4().hex[:8]}"


def insert_document(conn: sqlite3.Connection, **kwargs) -> dict[str, Any]:
    """Insert into documents table, return row dict."""
    defaults = {
        "document_id": doc_id(),
        "file_name": "test.pdf",
        "file_path": "/tmp/test.pdf",
        "client_code": "TEST01",
        "vendor": "Test Vendor",
        "vendor_name": "Test Vendor",
        "invoice_number": f"INV-{uuid.uuid4().hex[:6]}",
        "doc_type": "invoice",
        "amount": 1000.00,
        "subtotal": 869.57,
        "tax_total": 130.43,
        "document_date": "2025-06-15",
        "gl_account": "5000",
        "tax_code": "T",
        "category": "expense",
        "review_status": "Ready",
        "confidence": 0.95,
        "currency": "CAD",
        "raw_result": "{}",
        "substance_flags": "[]",
        "fraud_flags": "[]",
        "hallucination_suspected": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
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


def ensure_documents_table(conn: sqlite3.Connection) -> None:
    """Create documents table for testing."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT DEFAULT '',
            file_path TEXT DEFAULT '',
            client_code TEXT,
            vendor TEXT,
            vendor_name TEXT,
            invoice_number TEXT,
            doc_type TEXT,
            amount REAL,
            subtotal REAL,
            tax_total REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            review_status TEXT,
            confidence REAL,
            currency TEXT DEFAULT 'CAD',
            raw_result TEXT DEFAULT '{}',
            substance_flags TEXT DEFAULT '[]',
            fraud_flags TEXT DEFAULT '[]',
            fraud_override_reason TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            version INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT DEFAULT 'ai_call',
            username TEXT,
            document_id TEXT,
            provider TEXT,
            task_type TEXT,
            prompt_snippet TEXT,
            latency_ms INTEGER,
            created_at TEXT
        );

        -- FIX 2: Audit trail tamper-proofing — immutable rows
        CREATE TRIGGER IF NOT EXISTS trg_audit_log_no_delete
        BEFORE DELETE ON audit_log
        BEGIN
            SELECT RAISE(ABORT, 'Audit log entries are permanent and cannot be deleted');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_audit_log_no_update
        BEFORE UPDATE ON audit_log
        BEGIN
            SELECT RAISE(ABORT, 'Audit log entries are immutable and cannot be modified');
        END;

        -- FIX 12: Auto-create audit log on document insertion
        CREATE TRIGGER IF NOT EXISTS trg_document_insert_audit
        AFTER INSERT ON documents
        BEGIN
            INSERT INTO audit_log (event_type, document_id, username, created_at)
            VALUES ('document_created', NEW.document_id, 'system', datetime('now'));
        END;

        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            file_name TEXT,
            client_code TEXT,
            vendor TEXT,
            document_date TEXT,
            amount REAL,
            currency TEXT DEFAULT 'CAD',
            doc_type TEXT,
            gl_account TEXT,
            tax_code TEXT,
            review_status TEXT,
            payload_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Scoreboard collector
# ---------------------------------------------------------------------------

_SCOREBOARD: list[dict] = []


def record_defect(
    test_id: str,
    letter: str,
    subsystem: str,
    severity: str,
    title: str,
    description: str,
    repro: str,
    implicated_files: list[str],
    business_consequence: str,
):
    """Record a confirmed defect for the final report."""
    _SCOREBOARD.append({
        "test_id": test_id,
        "letter": letter,
        "subsystem": subsystem,
        "severity": severity,
        "title": title,
        "description": description,
        "repro": repro,
        "implicated_files": implicated_files,
        "business_consequence": business_consequence,
    })


def get_scoreboard() -> list[dict]:
    return list(_SCOREBOARD)

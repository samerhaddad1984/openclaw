from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from src.agents.tools.posting_builder import sync_posting_payload


def test_sync_posting_payload_keeps_row_and_payload_consistent(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    conn.execute(
        """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            document_date TEXT,
            amount REAL,
            doc_type TEXT,
            category TEXT,
            gl_account TEXT,
            tax_code TEXT,
            review_status TEXT,
            confidence REAL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            target_system TEXT,
            entry_kind TEXT,
            posting_status TEXT,
            approval_state TEXT,
            reviewer TEXT,
            external_id TEXT,
            payload_json TEXT,
            error_text TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    conn.execute(
        """
        INSERT INTO documents (
            document_id, file_name, file_path, client_code, vendor,
            document_date, amount, doc_type, category, gl_account,
            tax_code, review_status, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "doc_1",
            "invoice.pdf",
            r"D:\Agents\OtoCPAAi\tests\documents_real\invoice.pdf",
            "SOUSSOL Quebec",
            "CompanyCam",
            "2025-10-22",
            11.72,
            "invoice",
            "Software",
            "Software Expense",
            "NONE",
            "Ready",
            0.97,
        ),
    )

    stale_payload = {
        "posting_id": "post_1",
        "document_id": "doc_1",
        "target_system": "qbo",
        "entry_kind": "expense",
        "posting_status": "draft",
        "approval_state": "approved_for_posting",
        "reviewer": "ExceptionRouter",
        "external_id": None,
        "updated_at": "2026-03-10T00:00:00+00:00",
    }

    conn.execute(
        """
        INSERT INTO posting_jobs (
            posting_id, document_id, target_system, entry_kind,
            posting_status, approval_state, reviewer, external_id,
            payload_json, error_text, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "post_1",
            "doc_1",
            "qbo",
            "expense",
            "posted",
            "approved_for_posting",
            "OpenClawCaseOrchestrator",
            "145",
            json.dumps(stale_payload),
            None,
            "2026-03-10T00:00:00+00:00",
            "2026-03-10T00:00:00+00:00",
        ),
    )
    conn.commit()

    result = sync_posting_payload(conn, document_id="doc_1")

    row = conn.execute(
        """
        SELECT posting_status, approval_state, reviewer, external_id, payload_json, updated_at
        FROM posting_jobs
        WHERE document_id = ?
        """,
        ("doc_1",),
    ).fetchone()

    assert row is not None
    payload = json.loads(row["payload_json"])

    assert result["posting_status"] == "posted"
    assert row["posting_status"] == "posted"
    assert payload["posting_status"] == "posted"

    assert result["approval_state"] == "approved_for_posting"
    assert row["approval_state"] == "approved_for_posting"
    assert payload["approval_state"] == "approved_for_posting"

    assert result["reviewer"] == "OpenClawCaseOrchestrator"
    assert row["reviewer"] == "OpenClawCaseOrchestrator"
    assert payload["reviewer"] == "OpenClawCaseOrchestrator"

    assert result["external_id"] == "145"
    assert row["external_id"] == "145"
    assert payload["external_id"] == "145"

    assert payload["updated_at"] == row["updated_at"]

    conn.close()
from __future__ import annotations

import gc
import json
import sqlite3
from pathlib import Path

import pytest

from src.agents.core.openclaw_review_queue import OpenClawReviewQueue


TEST_DB = Path(__file__).resolve().parent / "test_review_queue.db"


def setup_db():
    if TEST_DB.exists():
        TEST_DB.unlink()

    conn = sqlite3.connect(TEST_DB)
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            review_status TEXT,
            confidence REAL,
            raw_result TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE posting_jobs (
            posting_id TEXT,
            document_id TEXT,
            posting_status TEXT,
            approval_state TEXT,
            reviewer TEXT,
            external_id TEXT,
            payload_json TEXT,
            error_text TEXT
        )
    """)

    conn.commit()
    return conn


def insert_document(conn, *, document_id, raw_result=None, review_status=""):
    conn.execute("""
        INSERT INTO documents (
            document_id, file_name, file_path, client_code, vendor,
            doc_type, amount, document_date, gl_account, tax_code,
            category, review_status, confidence, raw_result,
            created_at, updated_at
        ) VALUES (?, '', '', '', '', '', 0, '', '', '', '', ?, 0, ?, '', '')
    """, (
        document_id,
        review_status,
        json.dumps(raw_result or {}),
    ))


def insert_posting(conn, *, document_id, status="", approval="", reviewer=""):
    conn.execute("""
        INSERT INTO posting_jobs (
            posting_id, document_id, posting_status,
            approval_state, reviewer, external_id,
            payload_json, error_text
        ) VALUES (?, ?, ?, ?, ?, '', '{}', '')
    """, (
        f"pj_{document_id}",
        document_id,
        status,
        approval,
        reviewer,
    ))


@pytest.fixture()
def queue():
    conn = setup_db()
    yield OpenClawReviewQueue(db_path=TEST_DB)
    conn.close()
    gc.collect()
    if TEST_DB.exists():
        TEST_DB.unlink()


def test_posted_exception_router_not_escalated(queue):
    conn = sqlite3.connect(TEST_DB)

    insert_document(conn, document_id="doc1")
    insert_posting(
        conn,
        document_id="doc1",
        status="posted",
        approval="approved_for_posting",
        reviewer="ExceptionRouter",
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(escalated_only=True)
    assert result["summary"]["escalated_count"] == 0


def test_pending_human_approval_is_escalated(queue):
    conn = sqlite3.connect(TEST_DB)

    insert_document(conn, document_id="doc2")
    insert_posting(
        conn,
        document_id="doc2",
        status="draft",
        approval="pending_human_approval",
        reviewer="ExceptionRouter",
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(escalated_only=True)
    assert result["summary"]["escalated_count"] == 1


def test_raw_result_escalation(queue):
    conn = sqlite3.connect(TEST_DB)

    insert_document(
        conn,
        document_id="doc3",
        raw_result={"openclaw_escalation_result": {"should_escalate": True}},
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(escalated_only=True)
    assert result["summary"]["escalated_count"] == 1


def test_duplicate_medium_is_review(queue):
    conn = sqlite3.connect(TEST_DB)

    insert_document(
        conn,
        document_id="doc4",
        raw_result={"duplicate_result": {"risk_level": "medium"}},
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(review_only=True)
    assert result["summary"]["needs_review_count"] == 1


def test_vendor_flagged_is_review(queue):
    conn = sqlite3.connect(TEST_DB)

    insert_document(
        conn,
        document_id="doc5",
        raw_result={"vendor_memory_result": {"flagged_for_review": True}},
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(review_only=True)
    assert result["summary"]["needs_review_count"] == 1


def test_escalated_only_filters_correctly(queue):
    conn = sqlite3.connect(TEST_DB)

    # NOT escalated
    insert_document(conn, document_id="doc6")
    insert_posting(
        conn,
        document_id="doc6",
        status="posted",
        approval="approved_for_posting",
        reviewer="ExceptionRouter",
    )

    # escalated
    insert_document(conn, document_id="doc7")
    insert_posting(
        conn,
        document_id="doc7",
        status="draft",
        approval="pending_human_approval",
        reviewer="ExceptionRouter",
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(escalated_only=True)

    assert result["summary"]["total_items"] == 1
    assert result["items"][0]["document_id"] == "doc7"


def test_review_only_includes_escalation(queue):
    conn = sqlite3.connect(TEST_DB)

    insert_document(conn, document_id="doc8")
    insert_posting(
        conn,
        document_id="doc8",
        status="draft",
        approval="pending_human_approval",
        reviewer="ExceptionRouter",
    )

    conn.commit()
    conn.close()

    result = queue.list_queue(review_only=True)
    assert result["summary"]["needs_review_count"] == 1
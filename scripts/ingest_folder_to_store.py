from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.local_document_processor import process_document
from src.agents.tools.fingerprint_utils import (
    build_fingerprint_bundle,
    compute_file_sha256,
)

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
INGEST_FOLDER = ROOT_DIR / "data" / "incoming_documents"


def utc_now():
    return datetime.utcnow().isoformat(timespec="seconds")


def open_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def column_exists(conn, table, column):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    return column in cols


def ensure_schema(conn):

    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            file_path TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            client_code TEXT,
            confidence REAL,
            review_status TEXT,
            raw_result TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    conn.commit()

    if not column_exists(conn, "documents", "physical_id"):
        cur.execute("ALTER TABLE documents ADD COLUMN physical_id TEXT")

    if not column_exists(conn, "documents", "logical_fingerprint"):
        cur.execute("ALTER TABLE documents ADD COLUMN logical_fingerprint TEXT")

    conn.commit()

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_documents_logical
        ON documents(logical_fingerprint)
        """
    )

    conn.commit()


def document_exists(conn, logical_fingerprint):

    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1 FROM documents
        WHERE logical_fingerprint = ?
        LIMIT 1
        """,
        (logical_fingerprint,),
    )

    return cur.fetchone() is not None


def insert_document(conn, bundle, file_path, extracted_data):

    cur = conn.cursor()

    now = utc_now()

    cur.execute(
        """
        INSERT INTO documents (
            physical_id,
            logical_fingerprint,
            file_name,
            file_path,
            vendor,
            doc_type,
            amount,
            document_date,
            client_code,
            confidence,
            review_status,
            raw_result,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            bundle.physical_id,
            bundle.logical_fingerprint,
            file_path.name,
            str(file_path),
            extracted_data.get("vendor"),
            extracted_data.get("doc_type"),
            extracted_data.get("amount"),
            extracted_data.get("date"),
            extracted_data.get("client_code"),
            extracted_data.get("confidence", 0.0),
            "Pending",
            json.dumps(extracted_data),
            now,
            now,
        ),
    )

    conn.commit()


def ingest_file(conn, file_path):

    print(f"Processing {file_path.name}")

    extracted_data = process_document(file_path)

    if not extracted_data:
        print("Extraction failed.")
        return

    file_hash = compute_file_sha256(file_path)

    bundle = build_fingerprint_bundle(
        extracted_data,
        file_path.name,
        file_hash,
    )

    if document_exists(conn, bundle.logical_fingerprint):
        print("Duplicate logical document skipped.")
        return

    insert_document(conn, bundle, file_path, extracted_data)

    print("Document stored.")


def run():

    conn = open_db()

    ensure_schema(conn)

    if not INGEST_FOLDER.exists():
        print("Incoming folder missing:", INGEST_FOLDER)
        return

    files = list(INGEST_FOLDER.glob("*"))

    if not files:
        print("No files found.")
        return

    for file_path in files:

        if not file_path.is_file():
            continue

        ingest_file(conn, file_path)

    conn.close()


if __name__ == "__main__":
    run()
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def get_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def ensure_review_status_column(conn: sqlite3.Connection) -> None:
    cols = get_columns(conn, "documents")
    if "review_status" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN review_status TEXT")
        conn.commit()


def ensure_updated_at_column(conn: sqlite3.Connection) -> None:
    cols = get_columns(conn, "documents")
    if "updated_at" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN updated_at TEXT")
        conn.commit()


def ensure_cleanup_note_column(conn: sqlite3.Connection) -> None:
    cols = get_columns(conn, "documents")
    if "cleanup_note" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN cleanup_note TEXT")
        conn.commit()


def detect_document_id_column(conn: sqlite3.Connection) -> str:
    cols = get_columns(conn, "documents")
    for candidate in ["document_id", "id", "doc_id"]:
        if candidate in cols:
            return candidate
    raise RuntimeError("documents table has no usable id column")


def load_documents(conn: sqlite3.Connection, id_col: str) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            {id_col} AS document_id,
            file_name,
            file_path,
            vendor,
            client_code,
            doc_type,
            amount,
            document_date,
            gl_account,
            tax_code,
            category,
            review_status,
            raw_result
        FROM documents
        ORDER BY {id_col}
        """
    )
    return cur.fetchall()


def is_missing(value: Any) -> bool:
    return normalize_text(value) == ""


def bad_reasons(row: sqlite3.Row) -> list[str]:
    reasons: list[str] = []

    if is_missing(row["document_id"]):
        reasons.append("missing_document_id")

    critical_fields = [
        "vendor",
        "amount",
        "document_date",
        "gl_account",
        "tax_code",
        "category",
        "client_code",
    ]

    missing_count = 0
    for field in critical_fields:
        if is_missing(row[field]):
            missing_count += 1

    if missing_count == len(critical_fields):
        reasons.append("all_critical_fields_missing")
    elif missing_count >= 5:
        reasons.append(f"too_many_missing_fields:{missing_count}")

    file_name = normalize_text(row["file_name"]).lower()
    file_path = normalize_text(row["file_path"]).lower()

    if not file_name and not file_path:
        reasons.append("missing_file_identity")

    return reasons


def mark_ignored(conn: sqlite3.Connection, id_col: str, document_id: Any, note: str) -> None:
    conn.execute(
        f"""
        UPDATE documents
        SET
            review_status = ?,
            cleanup_note = ?,
            updated_at = ?
        WHERE {id_col} IS ?
        """,
        (
            "Ignored",
            note,
            utc_now_iso(),
            document_id,
        ),
    )


def main() -> int:
    with open_db() as conn:
        if not table_exists(conn, "documents"):
            raise RuntimeError("documents table not found")

        ensure_review_status_column(conn)
        ensure_updated_at_column(conn)
        ensure_cleanup_note_column(conn)

        id_col = detect_document_id_column(conn)
        rows = load_documents(conn, id_col)

        scanned = 0
        cleaned = 0

        for row in rows:
            scanned += 1
            reasons = bad_reasons(row)

            if not reasons:
                continue

            note = ";".join(reasons)
            mark_ignored(conn, id_col, row["document_id"], note)
            cleaned += 1

            print(
                f"[CLEANED] document_id={normalize_text(row['document_id']) or 'None'} "
                f"file={normalize_text(row['file_name']) or '-'} "
                f"reasons={note}"
            )

        conn.commit()

    print()
    print("Cleanup complete")
    print("Documents scanned:", scanned)
    print("Documents cleaned:", cleaned)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
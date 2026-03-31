from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.learning_memory_store import LearningMemoryStore

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
DEFAULT_REVIEWER = "backfill"


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


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


def resolve_column(columns: list[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def parse_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_amount_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        return f"{round(float(text.replace(',', '')), 2):.2f}"
    except Exception:
        return text


def ensure_minimum_tables(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "documents"):
        raise RuntimeError("documents table not found")
    if not table_exists(conn, "learning_memory"):
        # schema is created by LearningMemoryStore on init
        LearningMemoryStore(DB_PATH)


def load_documents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    cols = get_columns(conn, "documents")

    id_col = resolve_column(cols, "document_id", "id", "doc_id")
    if not id_col:
        raise RuntimeError("documents table has no usable primary id column")

    file_name_col = resolve_column(cols, "file_name", "filename", "name")
    client_code_col = resolve_column(cols, "client_code", "client")
    vendor_col = resolve_column(cols, "vendor", "vendor_name", "supplier", "merchant")
    doc_type_col = resolve_column(cols, "doc_type", "document_type", "type")
    amount_col = resolve_column(cols, "amount", "total_amount", "gross_amount")
    date_col = resolve_column(cols, "document_date", "date", "invoice_date", "txn_date")
    gl_col = resolve_column(cols, "gl_account")
    tax_col = resolve_column(cols, "tax_code")
    category_col = resolve_column(cols, "category")
    review_status_col = resolve_column(cols, "review_status", "status")
    raw_col = resolve_column(cols, "raw_result", "raw_json", "payload_json")

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            {id_col} AS document_id,
            {file_name_col or 'NULL'} AS file_name,
            {client_code_col or 'NULL'} AS client_code,
            {vendor_col or 'NULL'} AS vendor,
            {doc_type_col or 'NULL'} AS doc_type,
            {amount_col or 'NULL'} AS amount,
            {date_col or 'NULL'} AS document_date,
            {gl_col or 'NULL'} AS gl_account,
            {tax_col or 'NULL'} AS tax_code,
            {category_col or 'NULL'} AS category,
            {review_status_col or 'NULL'} AS review_status,
            {raw_col or 'NULL'} AS raw_result
        FROM documents
        """
    )
    return cur.fetchall()


def current_learning_keys(conn: sqlite3.Connection) -> set[tuple[str, str, str]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT document_id, field_name, new_value
        FROM learning_memory
        """
    )
    rows = cur.fetchall()
    return {
        (
            normalize_text(r["document_id"]),
            normalize_text(r["field_name"]),
            normalize_text(r["new_value"]),
        )
        for r in rows
    }


def candidate_from_raw(raw_result: dict[str, Any], field_name: str) -> Any:
    # Tries multiple locations in raw_result for historical extracted values
    if not raw_result:
        return None

    direct = raw_result.get(field_name)
    if direct not in (None, ""):
        return direct

    nested_keys = [
        "raw_rules_output",
        "raw_vendor_output",
        "raw_ai_output",
        "metadata",
    ]

    for key in nested_keys:
        nested = raw_result.get(key)
        if isinstance(nested, dict):
            value = nested.get(field_name)
            if value not in (None, ""):
                return value

    aliases: dict[str, list[str]] = {
        "client_code": ["client", "client_name"],
        "vendor": ["vendor_name", "supplier", "merchant"],
        "doc_type": ["document_type", "type"],
        "amount": ["total_amount", "gross_amount"],
        "document_date": ["date", "invoice_date", "txn_date"],
        "gl_account": ["account", "expense_account"],
        "tax_code": ["tax", "sales_tax_code"],
        "category": ["expense_category"],
        "review_status": ["status"],
    }

    for alias in aliases.get(field_name, []):
        direct = raw_result.get(alias)
        if direct not in (None, ""):
            return direct

        for key in nested_keys:
            nested = raw_result.get(key)
            if isinstance(nested, dict):
                value = nested.get(alias)
                if value not in (None, ""):
                    return value

    return None


def values_different(field_name: str, old_value: Any, new_value: Any) -> bool:
    if field_name == "amount":
        return normalize_amount_text(old_value) != normalize_amount_text(new_value)
    return normalize_text(old_value) != normalize_text(new_value)


def backfill_from_documents(conn: sqlite3.Connection, store: LearningMemoryStore) -> int:
    docs = load_documents(conn)
    existing = current_learning_keys(conn)

    tracked_fields = [
        "client_code",
        "vendor",
        "doc_type",
        "amount",
        "document_date",
        "gl_account",
        "tax_code",
        "category",
        "review_status",
    ]

    inserted = 0

    for row in docs:
        document_id = normalize_text(row["document_id"])
        if not document_id:
            continue

        raw_result = parse_json(row["raw_result"])

        for field_name in tracked_fields:
            final_value = row[field_name]
            historical_value = candidate_from_raw(raw_result, field_name)

            if final_value in (None, ""):
                continue

            if historical_value in (None, ""):
                # no source value to compare against, skip to avoid inventing corrections
                continue

            if not values_different(field_name, historical_value, final_value):
                continue

            key = (
                document_id,
                field_name,
                normalize_text(final_value) if field_name != "amount" else normalize_amount_text(final_value),
            )
            if key in existing:
                continue

            store.record_correction(
                document_id=document_id,
                field_name=field_name,
                old_value=historical_value,
                new_value=final_value,
                reviewer=DEFAULT_REVIEWER,
                client_code=row["client_code"],
                vendor=row["vendor"],
                doc_type=row["doc_type"],
                correction_context={
                    "source": "backfill_from_documents",
                    "file_name": row["file_name"],
                },
            )
            existing.add(key)
            inserted += 1

    return inserted


def backfill_from_posting_jobs(conn: sqlite3.Connection, store: LearningMemoryStore) -> int:
    if not table_exists(conn, "posting_jobs"):
        return 0

    cols = get_columns(conn, "posting_jobs")
    payload_col = resolve_column(cols, "payload_json")
    document_id_col = resolve_column(cols, "document_id")
    if not payload_col or not document_id_col:
        return 0

    existing = current_learning_keys(conn)

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            {document_id_col} AS document_id,
            {payload_col} AS payload_json
        FROM posting_jobs
        """
    )
    rows = cur.fetchall()

    inserted = 0

    for row in rows:
        document_id = normalize_text(row["document_id"])
        if not document_id:
            continue

        payload = parse_json(row["payload_json"])
        if not payload:
            continue

        client_code = payload.get("client_code")
        vendor = payload.get("vendor")
        doc_type = payload.get("doc_type")

        mappings = {
            "gl_account": payload.get("gl_account"),
            "tax_code": payload.get("tax_code"),
            "category": payload.get("category"),
        }

        for field_name, final_value in mappings.items():
            if final_value in (None, ""):
                continue

            key = (
                document_id,
                field_name,
                normalize_text(final_value),
            )
            if key in existing:
                continue

            # posting payload is approved/derived final output; no reliable old value here
            # so skip if we cannot anchor a correction. To still backfill some value,
            # use a placeholder source marker only when no entry exists for that doc+field.
            cur2 = conn.cursor()
            cur2.execute(
                """
                SELECT 1
                FROM learning_memory
                WHERE document_id = ?
                  AND field_name = ?
                LIMIT 1
                """,
                (document_id, field_name),
            )
            if cur2.fetchone():
                continue

            store.record_correction(
                document_id=document_id,
                field_name=field_name,
                old_value="",
                new_value=final_value,
                reviewer=DEFAULT_REVIEWER,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                correction_context={
                    "source": "backfill_from_posting_jobs",
                    "note": "seeded from posting payload",
                },
            )
            existing.add(key)
            inserted += 1

    return inserted


def show_summary(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM learning_memory")
    total_learning = int(cur.fetchone()["c"])

    patterns = []
    if table_exists(conn, "learning_memory"):
        cur.execute(
            """
            SELECT
                vendor,
                field_name,
                new_value,
                COUNT(*) AS support
            FROM learning_memory
            GROUP BY vendor, field_name, new_value
            ORDER BY support DESC, vendor ASC
            LIMIT 20
            """
        )
        patterns = cur.fetchall()

    print()
    print("learning_memory rows:", total_learning)
    print()
    print("Top patterns:")
    if not patterns:
        print("  None")
        return

    for row in patterns:
        print(
            f"  vendor={normalize_text(row['vendor']) or '-'} | "
            f"field={row['field_name']} | "
            f"value={normalize_text(row['new_value'])} | "
            f"support={int(row['support'])}"
        )


def main() -> int:
    store = LearningMemoryStore(DB_PATH)

    with open_db() as conn:
        ensure_minimum_tables(conn)

        inserted_from_docs = backfill_from_documents(conn, store)
        inserted_from_posting = backfill_from_posting_jobs(conn, store)

        print("Backfill complete.")
        print("Inserted from documents:", inserted_from_docs)
        print("Inserted from posting_jobs:", inserted_from_posting)

        show_summary(conn)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
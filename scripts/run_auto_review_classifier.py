from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def safe_json_loads(v: Any) -> dict[str, Any]:
    if not v:
        return {}
    try:
        loaded = json.loads(v)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def fields_missing(row: sqlite3.Row) -> list[str]:
    required = [
        "vendor",
        "amount",
        "document_date",
        "gl_account",
        "tax_code",
        "category",
        "client_code",
    ]

    missing = []

    for field in required:
        val = normalize_text(row[field])
        if not val:
            missing.append(field)

    return missing


def determine_status(row: sqlite3.Row) -> tuple[str, str]:
    current_status = normalize_text(row["review_status"])

    # Never override cleanup decisions
    if current_status == "Ignored":
        return "Ignored", "preserve_ignored"

    raw = safe_json_loads(row["raw_result"])
    autofill_count = int(raw.get("learning_autofill_applied_count", 0) or 0)

    missing = fields_missing(row)

    if missing:
        return "NeedsReview", f"missing_fields:{','.join(missing)}"

    if autofill_count > 0:
        return "Ready", "learning_autofill"

    return "Ready", "complete_fields"


def update_document(conn: sqlite3.Connection, row: sqlite3.Row, new_status: str, reason: str) -> None:
    conn.execute(
        """
        UPDATE documents
        SET review_status = ?, updated_at = ?
        WHERE document_id IS ?
        """,
        (
            new_status,
            utc_now_iso(),
            row["document_id"],
        ),
    )

    print(f"[STATUS] {row['document_id']} -> {new_status} ({reason})")


def main() -> int:
    with open_db() as conn:
        rows = conn.execute(
            """
            SELECT
                document_id,
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
            ORDER BY document_id
            """
        ).fetchall()

        scanned = 0
        updated = 0

        for row in rows:
            scanned += 1

            new_status, reason = determine_status(row)
            current_status = normalize_text(row["review_status"])

            if new_status != current_status:
                update_document(conn, row, new_status, reason)
                updated += 1

        conn.commit()

    print()
    print("Auto review complete")
    print("Documents scanned:", scanned)
    print("Statuses updated:", updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.vendor_memory_store import VendorMemoryStore

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def safe_json_loads(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def main() -> int:
    store = VendorMemoryStore(DB_PATH)

    with open_db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE target_system = 'qbo'
              AND approval_state = 'approved_for_posting'
            ORDER BY created_at ASC
            """
        ).fetchall()

    scanned = 0
    imported = 0

    for row in rows:
        scanned += 1
        payload = safe_json_loads(row["payload_json"])

        client_code = payload.get("client_code")
        vendor = payload.get("vendor")
        gl_account = payload.get("gl_account")
        tax_code = payload.get("tax_code")
        doc_type = payload.get("doc_type")
        category = payload.get("category")

        if not client_code or not vendor:
            continue

        store.record_approval(
            {
                "client_code": client_code,
                "vendor": vendor,
                "gl_account": gl_account,
                "tax_code": tax_code,
                "doc_type": doc_type,
                "category": category,
            }
        )
        imported += 1

    print(
        json.dumps(
            {
                "posting_jobs_scanned": scanned,
                "vendor_memory_records_applied": imported,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
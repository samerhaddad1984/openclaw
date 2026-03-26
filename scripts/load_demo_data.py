#!/usr/bin/env python3
"""
scripts/load_demo_data.py

Select 50 pre-curated documents from the synthetic data (10 per client)
that best showcase all features, and mark them with demo=1 flag.

Per client (10 docs):
  - 2 normal approved documents
  - 1 fraud flagged document (duplicate or weekend)
  - 1 AI warning document (low confidence)
  - 1 meal receipt (tax code M)
  - 1 bank statement match example
  - 4 other interesting documents

Also adds "demo_mode": false to ledgerlink.config.json.

Usage:
    python scripts/load_demo_data.py
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
CONFIG_PATH = ROOT_DIR / "ledgerlink.config.json"
CLIENTS = ["MARCEL", "BOLDUC", "DENTAIRE", "BOUTIQUE", "TECHLAVAL"]


def _open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_demo_column(conn: sqlite3.Connection) -> bool:
    """Add demo column if missing.  Returns True if column was added."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if "demo" not in cols:
        conn.execute(
            "ALTER TABLE documents ADD COLUMN demo INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()
        return True
    return False


def _exclude_clause(doc_ids: list[str]) -> tuple[str, tuple[str, ...]]:
    """Build a SQL NOT IN clause for already-selected doc IDs."""
    if not doc_ids:
        return "", ()
    placeholders = ",".join("?" * len(doc_ids))
    return f"AND document_id NOT IN ({placeholders})", tuple(doc_ids)


def _pick_one(
    conn: sqlite3.Connection,
    client: str,
    doc_ids: list[str],
    where: str,
    params: tuple[Any, ...] = (),
) -> str | None:
    """Pick one document matching extra WHERE clause, excluding already-selected."""
    excl, excl_params = _exclude_clause(doc_ids)
    sql = (
        f"SELECT document_id FROM documents "
        f"WHERE client_code = ? {where} {excl} LIMIT 1"
    )
    row = conn.execute(sql, (client, *params, *excl_params)).fetchone()
    return row["document_id"] if row else None


def _pick_demo_docs(conn: sqlite3.Connection, client: str) -> list[str]:
    """Pick 10 representative documents for one client."""
    doc_ids: list[str] = []

    # ── 1. Two normal approved documents ─────────────────────────────────
    excl, excl_p = _exclude_clause(doc_ids)
    rows = conn.execute(
        f"""SELECT document_id FROM documents
            WHERE client_code = ? AND ingest_source = 'test:normal'
              AND review_status IN ('ReadyToPost', 'Posted')
              {excl}
            ORDER BY confidence DESC LIMIT 2""",
        (client, *excl_p),
    ).fetchall()
    doc_ids.extend(r["document_id"] for r in rows)

    # ── 2. One fraud flagged document (duplicate or weekend) ─────────────
    did = _pick_one(
        conn, client, doc_ids,
        "AND ingest_source IN ('test:duplicate', 'test:weekend') "
        "AND fraud_flags IS NOT NULL AND fraud_flags != '[]'",
    )
    if did:
        doc_ids.append(did)

    # ── 3. One AI warning document (low confidence) ──────────────────────
    did = _pick_one(conn, client, doc_ids, "AND ingest_source = 'test:low_confidence'")
    if did:
        doc_ids.append(did)

    # ── 4. One meal receipt (tax code M) ─────────────────────────────────
    did = _pick_one(conn, client, doc_ids, "AND ingest_source = 'test:meal'")
    if did:
        doc_ids.append(did)

    # ── 5. One bank statement match example ──────────────────────────────
    did = None
    try:
        excl, excl_p = _exclude_clause(doc_ids)
        row = conn.execute(
            f"""SELECT d.document_id FROM documents d
                JOIN bank_transactions bt ON bt.matched_document_id = d.document_id
                WHERE d.client_code = ? {excl}
                LIMIT 1""",
            (client, *excl_p),
        ).fetchone()
        if row:
            did = row["document_id"]
    except Exception:
        pass  # bank_transactions table may not exist
    if not did:
        # Fallback: pick a normal posted doc
        did = _pick_one(
            conn, client, doc_ids,
            "AND ingest_source = 'test:normal' AND review_status = 'Posted'",
        )
    if did:
        doc_ids.append(did)

    # ── 6. Four other interesting documents ──────────────────────────────
    for source in ["test:new_vendor", "test:round_number", "test:insurance", "test:math_mismatch"]:
        if len(doc_ids) >= 10:
            break
        did = _pick_one(conn, client, doc_ids, "AND ingest_source = ?", (source,))
        if did:
            doc_ids.append(did)

    # ── Fill remaining with any test docs ────────────────────────────────
    while len(doc_ids) < 10:
        did = _pick_one(conn, client, doc_ids, "AND ingest_source LIKE 'test:%'")
        if did:
            doc_ids.append(did)
        else:
            break

    return doc_ids[:10]


def _update_config() -> None:
    """Add demo_mode: false to ledgerlink.config.json if not already present."""
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        config = {}

    if "demo_mode" not in config:
        config["demo_mode"] = False
        CONFIG_PATH.write_text(
            json.dumps(config, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"  Added 'demo_mode': false to {CONFIG_PATH.name}")
    else:
        print(f"  'demo_mode' already in config: {config['demo_mode']}")


def main() -> int:
    print("load_demo_data.py")
    print(f"  DB: {DB_PATH}")

    conn = _open_db()

    added = _ensure_demo_column(conn)
    if added:
        print("  Added 'demo' column to documents table")

    # Reset all demo flags
    conn.execute("UPDATE documents SET demo = 0 WHERE demo = 1")
    conn.commit()

    total_marked = 0
    for client in CLIENTS:
        doc_ids = _pick_demo_docs(conn, client)
        if doc_ids:
            placeholders = ",".join("?" * len(doc_ids))
            conn.execute(
                f"UPDATE documents SET demo = 1 WHERE document_id IN ({placeholders})",
                doc_ids,
            )
            conn.commit()

        total_marked += len(doc_ids)
        print(f"\n  {client}: {len(doc_ids)} demo documents")
        for did in doc_ids:
            r = conn.execute(
                "SELECT ingest_source, vendor, amount, tax_code FROM documents "
                "WHERE document_id = ?",
                (did,),
            ).fetchone()
            if r:
                src = (r["ingest_source"] or "").replace("test:", "")
                print(
                    f"    {did}  [{src:<16}]  "
                    f"{(r['vendor'] or ''):<35}  "
                    f"${r['amount'] or 0:>10,.2f}  {r['tax_code'] or ''}"
                )

    conn.close()

    print()
    _update_config()
    print(f"\nTotal demo documents marked: {total_marked}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

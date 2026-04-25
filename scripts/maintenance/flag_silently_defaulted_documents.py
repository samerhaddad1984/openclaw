#!/usr/bin/env python3
"""Flag historical documents that look like silent OCR defaults.

Background: prior to the Layer-2 ingest fix, the OCR engine wrote
gl_account='5440' / category='operating_expense' on any document it
couldn't classify. That hides under "real" data, polluting reports.

This script does NOT modify the gl_account / category columns —
they may legitimately be 5440 / operating_expense for some docs and
we don't want to blow away CPA-confirmed work. It only sets the
``needs_categorization`` flag on documents that match the silent-
default fingerprint:

    gl_account = '5440'
    AND category = 'operating_expense'
    AND has_line_items = 0 (or NULL)
    AND (vendor IS NULL OR vendor = '' OR amount IS NULL OR amount = 0)

Run with --dry-run first to see how many docs would be flagged.

Usage:
    python3 scripts/maintenance/flag_silently_defaulted_documents.py \\
        [--db data/otocpa_agent.db] [--dry-run] [--limit 500]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


_DEFAULT_DB = Path(__file__).resolve().parents[2] / "data" / "otocpa_agent.db"


_FINGERPRINT_SQL = """
    SELECT document_id, file_name, vendor, amount, confidence
    FROM documents
    WHERE COALESCE(gl_account, '') = '5440'
      AND COALESCE(category, '') = 'operating_expense'
      AND COALESCE(has_line_items, 0) = 0
      AND (
            COALESCE(vendor, '') = ''
         OR amount IS NULL
         OR amount = 0
      )
      AND COALESCE(needs_categorization, 0) = 0
    ORDER BY created_at DESC
    LIMIT ?
"""


def find_suspect_docs(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(_FINGERPRINT_SQL, (limit,)).fetchall()


def flag_docs(conn: sqlite3.Connection, doc_ids: list[str]) -> int:
    if not doc_ids:
        return 0
    placeholders = ",".join("?" for _ in doc_ids)
    cur = conn.execute(
        f"UPDATE documents SET needs_categorization = 1 "
        f"WHERE document_id IN ({placeholders})",
        tuple(doc_ids),
    )
    conn.commit()
    return cur.rowcount


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=str(_DEFAULT_DB),
                   help="path to otocpa SQLite db")
    p.add_argument("--dry-run", action="store_true",
                   help="report counts only; do not write")
    p.add_argument("--limit", type=int, default=10000,
                   help="max docs to inspect in one run")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 2

    with sqlite3.connect(str(db_path)) as conn:
        # Make sure the column exists. We DON'T add it ourselves —
        # the dashboard's bootstrap_schema handles that. If missing,
        # bail with an explicit error so the operator runs the
        # dashboard once first.
        existing = {r[1] for r in
                    conn.execute("PRAGMA table_info(documents)").fetchall()}
        if "needs_categorization" not in existing:
            print(
                "documents.needs_categorization is missing. Run the "
                "dashboard once (which calls bootstrap_schema) before "
                "this script.",
                file=sys.stderr,
            )
            return 2

        suspects = find_suspect_docs(conn, args.limit)
        print(f"Found {len(suspects)} document(s) matching the silent-"
              f"default fingerprint:")
        print("  gl_account=5440 AND category=operating_expense AND")
        print("  has_line_items=0 AND (no vendor OR no amount)")
        print()
        for s in suspects[:20]:
            v = s["vendor"] or "<NULL>"
            a = s["amount"] if s["amount"] is not None else "<NULL>"
            print(f"  {s['document_id']}  {s['file_name']!r}  "
                  f"vendor={v!r} amount={a} conf={s['confidence']!r}")
        if len(suspects) > 20:
            print(f"  ... and {len(suspects) - 20} more")
        print()

        if args.dry_run:
            print("(dry-run — no changes written)")
            return 0

        if not suspects:
            return 0

        n = flag_docs(conn, [s["document_id"] for s in suspects])
        print(f"Flagged {n} document(s) with needs_categorization=1.")
        print("CPA dashboard will surface these for human review.")
        print("gl_account / category columns were NOT modified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

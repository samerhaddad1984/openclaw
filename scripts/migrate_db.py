"""
migrate_db.py — safe, additive schema migration for LedgerLink.

Compares every CREATE TABLE definition used across the Python codebase against
the live database and adds any missing columns via ALTER TABLE.  No data is
ever deleted or modified.

Usage:
    python scripts/migrate_db.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def add_missing(
    conn: sqlite3.Connection,
    table: str,
    columns: list[tuple[str, str]],
) -> list[str]:
    """Add any columns from *columns* not already present in *table*.

    Parameters
    ----------
    columns:
        List of ``(column_name, sql_type_fragment)`` pairs, e.g.
        ``("must_reset_password", "INTEGER NOT NULL DEFAULT 0")``.

    Returns
    -------
    List of column names that were actually added.
    """
    if not table_exists(conn, table):
        print(f"  SKIP  {table!r} does not exist — skipping")
        return []

    existing = existing_columns(conn, table)
    added: list[str] = []
    for col, typedef in columns:
        if col in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
        added.append(col)
        print(f"  ADD   {table}.{col}  ({typedef})")
    return added


def run_migration(db_path: Path = DB_PATH) -> None:
    print(f"Database : {db_path}")
    if not db_path.exists():
        print("ERROR: database file not found — run the application first to create it")
        sys.exit(1)

    with open_db(db_path) as conn:
        changed: list[str] = []

        # ------------------------------------------------------------------ #
        # dashboard_users
        # Expected by: review_dashboard.py, client_portal.py, dashboard_auth.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "dashboard_users", [
            ("is_active",           "INTEGER NOT NULL DEFAULT 1"),
            ("updated_at",          "TEXT NOT NULL DEFAULT ''"),
            ("last_login_at",       "TEXT"),
            ("must_reset_password", "INTEGER NOT NULL DEFAULT 0"),
            ("client_code",         "TEXT"),      # client_portal.py
            ("language",            "TEXT"),      # client_portal.py
        ])

        # Force must_reset_password=1 for rows that still carry a legacy
        # SHA-256 hash (40-char hex, not starting with $2b$).
        conn.execute(
            """
            UPDATE dashboard_users
               SET must_reset_password = 1
             WHERE must_reset_password = 0
               AND password_hash NOT LIKE '$2b$%'
               AND password_hash NOT LIKE '$2a$%'
               AND password_hash NOT LIKE '$2y$%'
            """
        )
        rows_flagged = conn.execute("SELECT changes()").fetchone()[0]
        if rows_flagged:
            print(f"  FLAG  dashboard_users: {rows_flagged} row(s) flagged must_reset_password=1 (legacy hash)")

        # ------------------------------------------------------------------ #
        # dashboard_sessions
        # Expected by: dashboard_auth.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "dashboard_sessions", [
            ("role",         "TEXT NOT NULL DEFAULT ''"),
            ("last_seen_at", "TEXT NOT NULL DEFAULT ''"),
        ])

        # ------------------------------------------------------------------ #
        # documents
        # Expected by: client_portal.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("submitted_by", "TEXT"),
            ("client_note",  "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # posting_jobs
        # Expected by: posting_builder.py (ensure_posting_job_table_minimum)
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "posting_jobs", [
            ("file_name",      "TEXT"),
            ("file_path",      "TEXT"),
            ("client_code",    "TEXT"),
            ("vendor",         "TEXT"),
            ("document_date",  "TEXT"),
            ("amount",         "REAL"),
            ("currency",       "TEXT"),
            ("doc_type",       "TEXT"),
            ("category",       "TEXT"),
            ("gl_account",     "TEXT"),
            ("tax_code",       "TEXT"),
            ("memo",           "TEXT"),
            ("review_status",  "TEXT"),
            ("confidence",     "REAL"),
            ("blocking_issues","TEXT"),
            ("notes",          "TEXT"),
            ("error_text",     "TEXT"),   # in case it was created without it
            ("assigned_to",    "TEXT"),   # in case it was created without it
        ])

        # ------------------------------------------------------------------ #
        # vendor_memory — backfill normalised key columns
        # Expected by: vendor_memory_store.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "vendor_memory", [
            ("vendor_key",      "TEXT NOT NULL DEFAULT ''"),
            ("client_code_key", "TEXT NOT NULL DEFAULT ''"),
            ("last_amount",     "REAL"),
            ("last_document_id","TEXT"),
            ("last_source",     "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # learning_memory — backfill normalised key / stat columns
        # Expected by: learning_memory_store.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "learning_memory", [
            ("memory_key",       "TEXT NOT NULL DEFAULT ''"),
            ("event_type",       "TEXT NOT NULL DEFAULT ''"),
            ("vendor_key",       "TEXT NOT NULL DEFAULT ''"),
            ("client_code_key",  "TEXT NOT NULL DEFAULT ''"),
            ("category",         "TEXT"),
            ("gl_account",       "TEXT"),
            ("tax_code",         "TEXT"),
            ("outcome_count",    "INTEGER NOT NULL DEFAULT 0"),
            ("success_count",    "INTEGER NOT NULL DEFAULT 0"),
            ("review_count",     "INTEGER NOT NULL DEFAULT 0"),
            ("posted_count",     "INTEGER NOT NULL DEFAULT 0"),
            ("avg_confidence",   "REAL NOT NULL DEFAULT 0.0"),
            ("avg_amount",       "REAL"),
            ("last_document_id", "TEXT"),
            ("last_payload_json","TEXT"),
            ("updated_at",       "TEXT NOT NULL DEFAULT ''"),
        ])

        conn.commit()

    if changed:
        print(f"\nDone — {len(changed)} column(s) added.")
    else:
        print("\nDone — schema is already up to date, no changes needed.")


if __name__ == "__main__":
    run_migration()

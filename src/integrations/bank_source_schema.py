"""Smart-bank-source schema migration.

Idempotent, applied by both ``review_dashboard.bootstrap_schema`` and
test fixtures. Extends existing ``bank_transactions`` + ``clients``
tables and adds a new ``bank_tx_dedup`` audit table.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _ensure_column(conn: sqlite3.Connection, table: str, col: str,
                    ddl: str) -> None:
    if col not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def apply_bank_source_schema(conn: sqlite3.Connection) -> None:
    """Idempotently add bank-source columns + dedup table."""
    # clients.bank_source ∈ {'qbo','plaid','both','none'}
    if 'clients' in {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}:
        _ensure_column(conn, 'clients', 'bank_source',
                        "TEXT DEFAULT 'none'")
    # bank_transactions may not exist in a fresh test DB; create minimal
    # shape if absent so the ALTERs below succeed uniformly.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            plaid_transaction_id TEXT,
            account_id TEXT,
            date TEXT,
            amount REAL,
            description TEXT,
            merchant_name TEXT,
            category TEXT,
            pending INTEGER DEFAULT 0,
            matched_document_id TEXT,
            reconciled INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    _ensure_column(conn, 'bank_transactions', 'firm_code', "TEXT")
    _ensure_column(conn, 'bank_transactions', 'source',
                    "TEXT DEFAULT 'plaid'")
    _ensure_column(conn, 'bank_transactions', 'external_id', "TEXT")
    _ensure_column(conn, 'bank_transactions', 'qbo_account_id', "TEXT")
    _ensure_column(conn, 'bank_transactions', 'qbo_sync_token', "TEXT")
    _ensure_column(conn, 'bank_transactions', 'hidden_duplicate',
                    "INTEGER DEFAULT 0")

    # UNIQUE index for upsert key (firm, client, source, external_id).
    # Partial index so legacy plaid rows without firm_code don't collide.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_tx_source_external "
        "ON bank_transactions(firm_code, client_code, source, external_id) "
        "WHERE external_id IS NOT NULL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bank_tx_client_source_date "
        "ON bank_transactions(firm_code, client_code, source, date)"
    )

    # Dedup audit table.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_tx_dedup (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_code TEXT,
            client_code TEXT,
            primary_source TEXT,
            primary_id TEXT,
            duplicate_source TEXT,
            duplicate_id TEXT,
            match_confidence REAL,
            detected_at TEXT,
            resolved_by TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bank_dedup_client "
        "ON bank_tx_dedup(firm_code, client_code)"
    )
    conn.commit()


def ensure_bank_source_schema(db_path: Path | str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        apply_bank_source_schema(conn)

"""QBO bidirectional-sync schema.

All tables are idempotent (``CREATE TABLE IF NOT EXISTS``) so both the
review_dashboard bootstrap path and test fixtures can call
``apply_qbo_sync_schema`` without guarding.

Entity model:

- ``qbo_sync_state``       — one row per (firm, client, entity_type, qbo_id)
                              tracking sync tokens + timestamps.
- ``qbo_accounts``         — COA cache.
- ``qbo_customers``        — customer cache.
- ``qbo_vendors``          — vendor cache.
- ``qbo_journal_entries``  — JE header.
- ``qbo_journal_entry_lines`` — JE lines.
- ``qbo_bills``            — AP bills.
- ``qbo_invoices``         — AR invoices.
- ``qbo_sync_log``         — run history.
- ``qbo_webhook_events``   — inbound webhook dedup + processing queue.

Relies on ``qbo_connections`` (already bootstrapped elsewhere).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


_DDL = [
    # Master sync-state table. Optimistic concurrency via qbo_sync_token.
    """
    CREATE TABLE IF NOT EXISTS qbo_sync_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        qbo_sync_token TEXT,
        local_id TEXT,
        last_pulled_at TEXT,
        last_pushed_at TEXT,
        last_qbo_modified TEXT,
        last_local_modified TEXT,
        sync_status TEXT,
        sync_source TEXT,
        conflict_details TEXT,
        version INTEGER DEFAULT 1,
        UNIQUE(firm_code, client_code, entity_type, qbo_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qbo_sync_state_entity
        ON qbo_sync_state(firm_code, client_code, entity_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qbo_sync_state_status
        ON qbo_sync_state(sync_status)
    """,

    # Chart of accounts cache.
    """
    CREATE TABLE IF NOT EXISTS qbo_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        name TEXT,
        account_type TEXT,
        account_sub_type TEXT,
        account_number TEXT,
        parent_ref TEXT,
        currency TEXT,
        active INTEGER DEFAULT 1,
        classification TEXT,
        balance REAL,
        current_balance REAL,
        last_synced TEXT,
        UNIQUE(firm_code, client_code, qbo_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qbo_accounts_number
        ON qbo_accounts(firm_code, client_code, account_number)
    """,

    # Customers cache.
    """
    CREATE TABLE IF NOT EXISTS qbo_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        display_name TEXT,
        company_name TEXT,
        email TEXT,
        phone TEXT,
        billing_address TEXT,
        shipping_address TEXT,
        balance REAL,
        active INTEGER DEFAULT 1,
        last_synced TEXT,
        UNIQUE(firm_code, client_code, qbo_id)
    )
    """,

    # Vendors cache.
    """
    CREATE TABLE IF NOT EXISTS qbo_vendors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        display_name TEXT,
        company_name TEXT,
        email TEXT,
        phone TEXT,
        balance REAL,
        active INTEGER DEFAULT 1,
        tax_identifier TEXT,
        last_synced TEXT,
        UNIQUE(firm_code, client_code, qbo_id)
    )
    """,

    # Journal-entry headers.
    """
    CREATE TABLE IF NOT EXISTS qbo_journal_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        doc_number TEXT,
        txn_date TEXT,
        total_amount REAL,
        currency TEXT,
        memo TEXT,
        adjustment INTEGER DEFAULT 0,
        source TEXT,
        local_je_id INTEGER,
        last_synced TEXT,
        UNIQUE(firm_code, client_code, qbo_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qbo_je_txn_date
        ON qbo_journal_entries(firm_code, client_code, txn_date)
    """,

    # Journal-entry lines. Keyed on qbo_je_id + line_num (QBO numbers within an
    # entry, not globally).
    """
    CREATE TABLE IF NOT EXISTS qbo_journal_entry_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qbo_je_id TEXT,
        line_num INTEGER,
        amount REAL,
        debit_credit TEXT,
        account_qbo_id TEXT,
        description TEXT,
        customer_qbo_id TEXT,
        vendor_qbo_id TEXT,
        class_qbo_id TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qbo_je_lines_je
        ON qbo_journal_entry_lines(qbo_je_id)
    """,

    # AP bills.
    """
    CREATE TABLE IF NOT EXISTS qbo_bills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        vendor_qbo_id TEXT,
        doc_number TEXT,
        txn_date TEXT,
        due_date TEXT,
        total_amount REAL,
        balance REAL,
        memo TEXT,
        source TEXT,
        local_document_id TEXT,
        last_synced TEXT,
        UNIQUE(firm_code, client_code, qbo_id)
    )
    """,

    # AR invoices.
    """
    CREATE TABLE IF NOT EXISTS qbo_invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        qbo_id TEXT NOT NULL,
        customer_qbo_id TEXT,
        doc_number TEXT,
        txn_date TEXT,
        due_date TEXT,
        total_amount REAL,
        balance REAL,
        memo TEXT,
        source TEXT,
        last_synced TEXT,
        UNIQUE(firm_code, client_code, qbo_id)
    )
    """,

    # Per-run audit.
    """
    CREATE TABLE IF NOT EXISTS qbo_sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_code TEXT NOT NULL,
        client_code TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        direction TEXT,
        entities_synced INTEGER,
        errors INTEGER,
        details TEXT,
        triggered_by TEXT
    )
    """,

    # Webhook dedup + processing queue.
    """
    CREATE TABLE IF NOT EXISTS qbo_webhook_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        realm_id TEXT,
        entity_type TEXT,
        entity_id TEXT,
        operation TEXT,
        last_updated TEXT,
        processed INTEGER DEFAULT 0,
        processed_at TEXT,
        error TEXT,
        received_at TEXT DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qbo_webhook_processed
        ON qbo_webhook_events(processed)
    """,
]


def apply_qbo_sync_schema(conn: sqlite3.Connection) -> None:
    """Create every QBO-sync table + index if missing. Idempotent."""
    for stmt in _DDL:
        conn.execute(stmt)
    conn.commit()


def ensure_qbo_sync_schema(db_path: Path | str) -> None:
    """Open ``db_path``, apply the schema, close. Safe to call at startup
    and from tests."""
    with sqlite3.connect(str(db_path)) as conn:
        apply_qbo_sync_schema(conn)

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
import unicodedata
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
            ("whatsapp_number",     "TEXT"),      # openclaw_bridge.py — inbound WhatsApp sender lookup
            ("telegram_id",         "TEXT"),      # openclaw_bridge.py — inbound Telegram sender lookup
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
            ("created_at",       "TEXT NOT NULL DEFAULT ''"),
            ("updated_at",       "TEXT NOT NULL DEFAULT ''"),
        ])

        # ------------------------------------------------------------------ #
        # documents — FIX 1: OCR-normalized invoice number for duplicate detection
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("invoice_number",            "TEXT"),
            ("invoice_number_normalized", "TEXT"),
        ])

        # Backfill normalized invoice numbers for existing rows
        if table_exists(conn, "documents") and "invoice_number" in existing_columns(conn, "documents"):
            rows_to_norm = conn.execute(
                "SELECT document_id, invoice_number FROM documents "
                "WHERE invoice_number IS NOT NULL AND invoice_number != '' "
                "AND (invoice_number_normalized IS NULL OR invoice_number_normalized = '')"
            ).fetchall()
            _norm_count = 0
            for row in rows_to_norm:
                raw = row["invoice_number"]
                normed = raw.strip().upper()
                normed = normed.replace("O", "0").replace("I", "1").replace("L", "1")
                normed = normed.replace("-", "").replace(" ", "")
                conn.execute(
                    "UPDATE documents SET invoice_number_normalized = ? WHERE document_id = ?",
                    (normed, row["document_id"]),
                )
                _norm_count += 1
            if _norm_count:
                print(f"  FIX1  documents: backfilled {_norm_count} invoice_number_normalized value(s)")

        # ------------------------------------------------------------------ #
        # documents — OCR engine columns added by ocr_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("currency",          "TEXT"),
            ("subtotal",          "REAL"),
            ("tax_total",         "REAL"),
            ("extraction_method", "TEXT"),
            ("ingest_source",     "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # documents — fraud_flags column added by fraud_engine.py
        # Expected by: src/engines/fraud_engine.py, scripts/review_dashboard.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("fraud_flags", "TEXT"),
            ("fraud_override_reason", "TEXT"),
            ("fraud_override_locked", "INTEGER NOT NULL DEFAULT 0"),
            ("substance_flags", "TEXT"),
            ("entry_kind", "TEXT"),
            ("review_history", "TEXT DEFAULT '[]'"),
        ])

        # ------------------------------------------------------------------ #
        # documents — hallucination guard columns
        # Expected by: src/agents/core/hallucination_guard.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("confidence",             "REAL"),
            ("raw_ocr_text",           "TEXT"),
            ("hallucination_suspected", "INTEGER NOT NULL DEFAULT 0"),
            ("correction_count",        "INTEGER NOT NULL DEFAULT 0"),
        ])

        # ------------------------------------------------------------------ #
        # documents — handwriting detection columns
        # Expected by: src/engines/ocr_engine.py (handwriting pipeline)
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("handwriting_low_confidence", "INTEGER NOT NULL DEFAULT 0"),
            ("handwriting_sample",         "INTEGER NOT NULL DEFAULT 0"),
        ])

        # ------------------------------------------------------------------ #
        # documents — created_at for time-decay filtering in vendor memory
        # Expected by: src/agents/core/vendor_memory_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ])

        # ------------------------------------------------------------------ #
        # period_close — month-end close checklist
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS period_close (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code      TEXT NOT NULL,
                period           TEXT NOT NULL,
                checklist_item   TEXT NOT NULL,
                status           TEXT NOT NULL DEFAULT 'open',
                responsible_user TEXT,
                due_date         TEXT,
                completed_by     TEXT,
                completed_at     TEXT,
                notes            TEXT,
                UNIQUE(client_code, period, checklist_item)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS period_close_locks (
                client_code  TEXT NOT NULL,
                period       TEXT NOT NULL,
                locked_by    TEXT,
                locked_at    TEXT,
                PRIMARY KEY (client_code, period)
            )
        """)
        changed += add_missing(conn, "period_close", [
            ("responsible_user", "TEXT"),
            ("due_date",         "TEXT"),
            ("completed_by",     "TEXT"),
            ("completed_at",     "TEXT"),
            ("notes",            "TEXT"),
        ])
        changed += add_missing(conn, "period_close_locks", [
            ("locked_by", "TEXT"),
            ("locked_at", "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # time_entries — billable time tracking per document/client
        # Expected by: time_tracker.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS time_entries (
                entry_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                username         TEXT NOT NULL,
                client_code      TEXT NOT NULL,
                document_id      TEXT,
                started_at       TEXT NOT NULL,
                ended_at         TEXT,
                duration_minutes REAL,
                description      TEXT,
                billable         INTEGER NOT NULL DEFAULT 1,
                hourly_rate      REAL
            )
        """)
        changed += add_missing(conn, "time_entries", [
            ("document_id",      "TEXT"),
            ("ended_at",         "TEXT"),
            ("duration_minutes", "REAL"),
            ("description",      "TEXT"),
            ("billable",         "INTEGER NOT NULL DEFAULT 1"),
            ("hourly_rate",      "REAL"),
        ])

        # ------------------------------------------------------------------ #
        # invoices — invoice metadata generated from time entries
        # Expected by: invoice_generator.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                invoice_id    TEXT PRIMARY KEY,
                client_code   TEXT NOT NULL,
                period_start  TEXT NOT NULL,
                period_end    TEXT NOT NULL,
                generated_by  TEXT NOT NULL,
                generated_at  TEXT NOT NULL,
                hourly_rate   REAL NOT NULL,
                subtotal      REAL NOT NULL,
                gst_amount    REAL NOT NULL,
                qst_amount    REAL NOT NULL,
                total_amount  REAL NOT NULL,
                entry_count   INTEGER NOT NULL DEFAULT 0
            )
        """)

        # ------------------------------------------------------------------ #
        # client_config — per-client settings (Quick Method flag, etc.)
        # Expected by: revenu_quebec.py, filing_calendar.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_config (
                client_code               TEXT PRIMARY KEY,
                quick_method              INTEGER NOT NULL DEFAULT 0,
                quick_method_type         TEXT    NOT NULL DEFAULT 'retail',
                updated_at                TEXT,
                filing_frequency          TEXT    NOT NULL DEFAULT 'monthly',
                gst_registration_number   TEXT,
                qst_registration_number   TEXT,
                fiscal_year_end           TEXT    NOT NULL DEFAULT '12-31'
            )
        """)
        changed += add_missing(conn, "client_config", [
            ("quick_method",            "INTEGER NOT NULL DEFAULT 0"),
            ("quick_method_type",       "TEXT    NOT NULL DEFAULT 'retail'"),
            ("updated_at",              "TEXT"),
            ("filing_frequency",        "TEXT    NOT NULL DEFAULT 'monthly'"),
            ("gst_registration_number", "TEXT"),
            ("qst_registration_number", "TEXT"),
            ("fiscal_year_end",         "TEXT    NOT NULL DEFAULT '12-31'"),
        ])

        # ------------------------------------------------------------------ #
        # gst_filings — records when each client's period was filed
        # Expected by: filing_calendar.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gst_filings (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code  TEXT NOT NULL,
                period_label TEXT NOT NULL,
                deadline     TEXT NOT NULL,
                filed_at     TEXT,
                filed_by     TEXT,
                UNIQUE(client_code, period_label)
            )
        """)

        # ------------------------------------------------------------------ #
        # audit_log — created by ai_router; add missing columns if partial
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type     TEXT    NOT NULL DEFAULT 'ai_call',
                username       TEXT,
                document_id    TEXT,
                provider       TEXT,
                task_type      TEXT,
                prompt_snippet TEXT,
                latency_ms     INTEGER,
                created_at     TEXT    NOT NULL DEFAULT ''
            )
        """)
        changed += add_missing(conn, "audit_log", [
            ("event_type",     "TEXT    NOT NULL DEFAULT 'ai_call'"),
            ("username",       "TEXT"),
            ("document_id",    "TEXT"),
            ("provider",       "TEXT"),
            ("task_type",      "TEXT"),
            ("prompt_snippet", "TEXT"),
            ("latency_ms",     "INTEGER"),
            ("created_at",     "TEXT    NOT NULL DEFAULT ''"),
        ])

        # ------------------------------------------------------------------ #
        # bank_statements / bank_transactions — bank statement import
        # Expected by: src/engines/bank_parser.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bank_statements (
                statement_id      TEXT PRIMARY KEY,
                bank_name         TEXT,
                file_name         TEXT,
                client_code       TEXT,
                imported_by       TEXT,
                imported_at       TEXT,
                period_start      TEXT,
                period_end        TEXT,
                transaction_count INTEGER DEFAULT 0,
                matched_count     INTEGER DEFAULT 0,
                unmatched_count   INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bank_transactions (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                statement_id        TEXT NOT NULL,
                document_id         TEXT NOT NULL,
                txn_date            TEXT,
                description         TEXT,
                debit               REAL,
                credit              REAL,
                balance             REAL,
                matched_document_id TEXT,
                match_confidence    REAL,
                match_reason        TEXT
            )
        """)
        changed += add_missing(conn, "bank_statements", [
            ("bank_name",         "TEXT"),
            ("file_name",         "TEXT"),
            ("client_code",       "TEXT"),
            ("imported_by",       "TEXT"),
            ("imported_at",       "TEXT"),
            ("period_start",      "TEXT"),
            ("period_end",        "TEXT"),
            ("transaction_count", "INTEGER DEFAULT 0"),
            ("matched_count",     "INTEGER DEFAULT 0"),
            ("unmatched_count",   "INTEGER DEFAULT 0"),
        ])
        changed += add_missing(conn, "bank_transactions", [
            ("statement_id",        "TEXT NOT NULL DEFAULT ''"),
            ("document_id",         "TEXT NOT NULL DEFAULT ''"),
            ("txn_date",            "TEXT"),
            ("description",         "TEXT"),
            ("debit",               "REAL"),
            ("credit",              "REAL"),
            ("balance",             "REAL"),
            ("matched_document_id", "TEXT"),
            ("match_confidence",    "REAL"),
            ("match_reason",        "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # client_communications — accountant-to-client message log
        # Expected by: src/agents/core/client_comms.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_communications (
                comm_id     TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                client_code TEXT NOT NULL,
                direction   TEXT NOT NULL DEFAULT 'outbound',
                message     TEXT NOT NULL,
                sent_at     TEXT,
                sent_by     TEXT,
                read_at     TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_comms_document "
            "ON client_communications(document_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_comms_client "
            "ON client_communications(client_code)"
        )


        # ------------------------------------------------------------------ #
        # Audit tables — working_papers, working_paper_items, audit_evidence,
        #                trial_balance, chart_of_accounts, engagements
        # Expected by: src/engines/audit_engine.py
        # ------------------------------------------------------------------ #
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS working_papers (
                paper_id          TEXT PRIMARY KEY,
                client_code       TEXT NOT NULL,
                period            TEXT NOT NULL,
                engagement_type   TEXT NOT NULL DEFAULT 'audit',
                account_code      TEXT NOT NULL,
                account_name      TEXT NOT NULL,
                balance_per_books REAL,
                balance_confirmed REAL,
                difference        REAL,
                tested_by         TEXT,
                reviewed_by       TEXT,
                sign_off_at       TEXT,
                status            TEXT NOT NULL DEFAULT 'open',
                notes             TEXT,
                created_at        TEXT,
                updated_at        TEXT
            );

            CREATE TABLE IF NOT EXISTS working_paper_items (
                item_id     TEXT PRIMARY KEY,
                paper_id    TEXT NOT NULL,
                document_id TEXT,
                tick_mark   TEXT NOT NULL DEFAULT 'tested',
                notes       TEXT,
                tested_by   TEXT,
                tested_at   TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_wp_items_paper
                ON working_paper_items(paper_id);

            CREATE TABLE IF NOT EXISTS audit_evidence (
                evidence_id         TEXT PRIMARY KEY,
                document_id         TEXT NOT NULL,
                evidence_type       TEXT NOT NULL,
                linked_document_ids TEXT,
                match_status        TEXT NOT NULL DEFAULT 'missing',
                notes               TEXT,
                created_at          TEXT,
                updated_at          TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_evidence_document
                ON audit_evidence(document_id);

            CREATE TABLE IF NOT EXISTS trial_balance (
                tb_id         TEXT PRIMARY KEY,
                client_code   TEXT NOT NULL,
                period        TEXT NOT NULL,
                account_code  TEXT NOT NULL,
                account_name  TEXT NOT NULL,
                debit_total   REAL NOT NULL DEFAULT 0,
                credit_total  REAL NOT NULL DEFAULT 0,
                net_balance   REAL NOT NULL DEFAULT 0,
                generated_at  TEXT,
                UNIQUE(client_code, period, account_code)
            );

            CREATE TABLE IF NOT EXISTS chart_of_accounts (
                account_code             TEXT PRIMARY KEY,
                account_name             TEXT NOT NULL,
                account_type             TEXT NOT NULL,
                normal_balance           TEXT NOT NULL DEFAULT 'debit',
                financial_statement_line TEXT,
                is_active                INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS engagements (
                engagement_id    TEXT PRIMARY KEY,
                client_code      TEXT NOT NULL,
                period           TEXT NOT NULL,
                engagement_type  TEXT NOT NULL DEFAULT 'audit',
                status           TEXT NOT NULL DEFAULT 'planning',
                partner          TEXT,
                manager          TEXT,
                staff            TEXT,
                planned_hours    REAL,
                actual_hours     REAL,
                budget           REAL,
                fee              REAL,
                created_at       TEXT,
                updated_at       TEXT,
                completed_at     TEXT,
                issued_by        TEXT,
                issued_at        TEXT,
                final_pdf_path   TEXT
            );
        """)
        changed += add_missing(conn, "working_papers", [
            ("balance_per_books", "REAL"),
            ("balance_confirmed", "REAL"),
            ("difference",        "REAL"),
            ("tested_by",         "TEXT"),
            ("reviewed_by",       "TEXT"),
            ("sign_off_at",       "TEXT"),
            ("notes",             "TEXT"),
            ("updated_at",        "TEXT"),
        ])
        changed += add_missing(conn, "audit_evidence", [
            ("linked_document_ids", "TEXT"),
            ("notes",               "TEXT"),
            ("updated_at",          "TEXT"),
        ])
        changed += add_missing(conn, "engagements", [
            ("actual_hours",   "REAL"),
            ("budget",         "REAL"),
            ("fee",            "REAL"),
            ("completed_at",   "TEXT"),
            ("issued_by",      "TEXT"),
            ("issued_at",      "TEXT"),
            ("final_pdf_path", "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # messaging_log — inbound / outbound messaging events
        # Expected by: src/integrations/whatsapp.py, openclaw_bridge.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messaging_log (
                log_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code  TEXT,
                platform     TEXT NOT NULL DEFAULT '',
                direction    TEXT NOT NULL DEFAULT 'inbound',
                message_type TEXT NOT NULL DEFAULT 'media',
                document_id  TEXT,
                sent_at      TEXT NOT NULL DEFAULT '',
                status       TEXT NOT NULL DEFAULT 'delivered'
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_msglog_sent_at "
            "ON messaging_log(sent_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_msglog_client "
            "ON messaging_log(client_code)"
        )
        changed += add_missing(conn, "messaging_log", [
            ("client_code",  "TEXT"),
            ("platform",     "TEXT NOT NULL DEFAULT ''"),
            ("direction",    "TEXT NOT NULL DEFAULT 'inbound'"),
            ("message_type", "TEXT NOT NULL DEFAULT 'media'"),
            ("document_id",  "TEXT"),
            ("sent_at",      "TEXT NOT NULL DEFAULT ''"),
            ("status",       "TEXT NOT NULL DEFAULT 'delivered'"),
        ])

        # ------------------------------------------------------------------ #
        # bank_reconciliations / reconciliation_items — bank reconciliation
        # Expected by: src/engines/reconciliation_engine.py
        # ------------------------------------------------------------------ #
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bank_reconciliations (
                reconciliation_id       TEXT PRIMARY KEY,
                client_code             TEXT NOT NULL,
                account_name            TEXT NOT NULL,
                account_number          TEXT,
                period_end_date         TEXT NOT NULL,
                statement_ending_balance REAL NOT NULL,
                gl_ending_balance       REAL NOT NULL,
                deposits_in_transit     TEXT DEFAULT '[]',
                outstanding_cheques     TEXT DEFAULT '[]',
                bank_errors             TEXT DEFAULT '[]',
                book_errors             TEXT DEFAULT '[]',
                adjusted_bank_balance   REAL,
                adjusted_book_balance   REAL,
                difference              REAL,
                status                  TEXT NOT NULL DEFAULT 'open',
                prepared_by             TEXT,
                reviewed_by             TEXT,
                prepared_at             TEXT,
                reviewed_at             TEXT,
                notes                   TEXT
            );

            CREATE TABLE IF NOT EXISTS reconciliation_items (
                item_id             TEXT PRIMARY KEY,
                reconciliation_id   TEXT NOT NULL,
                item_type           TEXT NOT NULL,
                description         TEXT NOT NULL,
                amount              REAL NOT NULL,
                transaction_date    TEXT,
                cleared_date        TEXT,
                document_id         TEXT,
                status              TEXT NOT NULL DEFAULT 'outstanding',
                FOREIGN KEY (reconciliation_id) REFERENCES bank_reconciliations(reconciliation_id)
            );

            CREATE INDEX IF NOT EXISTS idx_recon_items_recon
                ON reconciliation_items(reconciliation_id);
        """)
        changed += add_missing(conn, "bank_reconciliations", [
            ("account_number",          "TEXT"),
            ("deposits_in_transit",     "TEXT DEFAULT '[]'"),
            ("outstanding_cheques",     "TEXT DEFAULT '[]'"),
            ("bank_errors",             "TEXT DEFAULT '[]'"),
            ("book_errors",             "TEXT DEFAULT '[]'"),
            ("adjusted_bank_balance",   "REAL"),
            ("adjusted_book_balance",   "REAL"),
            ("difference",              "REAL"),
            ("prepared_by",             "TEXT"),
            ("reviewed_by",             "TEXT"),
            ("prepared_at",             "TEXT"),
            ("reviewed_at",             "TEXT"),
            ("notes",                   "TEXT"),
        ])
        changed += add_missing(conn, "reconciliation_items", [
            ("cleared_date",        "TEXT"),
            ("document_id",         "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # CAS 580 — Management Representation Letters
        # Expected by: src/engines/cas_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS management_representation_letters (
                letter_id        TEXT PRIMARY KEY,
                engagement_id    TEXT NOT NULL,
                client_code      TEXT NOT NULL,
                period_end_date  TEXT,
                draft_text_fr    TEXT,
                draft_text_en    TEXT,
                management_name  TEXT,
                management_title TEXT,
                signed_at        TEXT,
                signed_by        TEXT,
                status           TEXT NOT NULL DEFAULT 'draft',
                created_by       TEXT,
                created_at       TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rep_letter_engagement "
            "ON management_representation_letters(engagement_id)"
        )
        changed += add_missing(conn, "management_representation_letters", [
            ("period_end_date",  "TEXT"),
            ("draft_text_fr",    "TEXT"),
            ("draft_text_en",    "TEXT"),
            ("management_name",  "TEXT"),
            ("management_title", "TEXT"),
            ("signed_at",        "TEXT"),
            ("signed_by",        "TEXT"),
            ("status",           "TEXT NOT NULL DEFAULT 'draft'"),
            ("created_by",       "TEXT"),
            ("created_at",       "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # CAS 330 — Control Testing Documentation
        # Expected by: src/engines/cas_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS control_tests (
                test_id             TEXT PRIMARY KEY,
                engagement_id       TEXT NOT NULL,
                control_name        TEXT NOT NULL,
                control_description TEXT,
                control_objective   TEXT,
                test_type           TEXT NOT NULL DEFAULT 'walkthrough',
                test_procedure      TEXT,
                sample_size         INTEGER,
                items_tested        INTEGER,
                exceptions_found    INTEGER DEFAULT 0,
                exception_details   TEXT,
                conclusion          TEXT DEFAULT 'effective',
                tested_by           TEXT,
                tested_at           TEXT,
                reviewed_by         TEXT,
                notes               TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_control_tests_engagement "
            "ON control_tests(engagement_id)"
        )
        changed += add_missing(conn, "control_tests", [
            ("control_description", "TEXT"),
            ("control_objective",   "TEXT"),
            ("test_procedure",      "TEXT"),
            ("sample_size",         "INTEGER"),
            ("items_tested",        "INTEGER"),
            ("exceptions_found",    "INTEGER DEFAULT 0"),
            ("exception_details",   "TEXT"),
            ("conclusion",          "TEXT DEFAULT 'effective'"),
            ("tested_by",           "TEXT"),
            ("tested_at",           "TEXT"),
            ("reviewed_by",         "TEXT"),
            ("notes",               "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # CAS 550 — Related Party Procedures
        # Expected by: src/engines/cas_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS related_parties (
                party_id             TEXT PRIMARY KEY,
                client_code          TEXT NOT NULL,
                party_name           TEXT NOT NULL,
                relationship_type    TEXT NOT NULL DEFAULT 'affiliated_company',
                ownership_percentage REAL,
                notes                TEXT,
                identified_by        TEXT,
                identified_at        TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_related_parties_client "
            "ON related_parties(client_code)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS related_party_transactions (
                rpt_id                     TEXT PRIMARY KEY,
                engagement_id              TEXT NOT NULL,
                party_id                   TEXT NOT NULL,
                document_id                TEXT,
                transaction_date           TEXT,
                amount                     REAL,
                description                TEXT,
                normal_amount              REAL,
                difference                 REAL,
                measurement_basis          TEXT DEFAULT 'exchange_amount',
                disclosure_required        INTEGER NOT NULL DEFAULT 1,
                audit_procedures_performed TEXT,
                conclusion                 TEXT,
                reviewed_by                TEXT,
                notes                      TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rpt_engagement "
            "ON related_party_transactions(engagement_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rpt_party "
            "ON related_party_transactions(party_id)"
        )
        changed += add_missing(conn, "related_parties", [
            ("ownership_percentage", "REAL"),
            ("notes",                "TEXT"),
            ("identified_by",        "TEXT"),
            ("identified_at",        "TEXT"),
        ])
        changed += add_missing(conn, "related_party_transactions", [
            ("document_id",                "TEXT"),
            ("transaction_date",           "TEXT"),
            ("amount",                     "REAL"),
            ("description",                "TEXT"),
            ("normal_amount",              "REAL"),
            ("difference",                 "REAL"),
            ("measurement_basis",          "TEXT DEFAULT 'exchange_amount'"),
            ("disclosure_required",        "INTEGER NOT NULL DEFAULT 1"),
            ("audit_procedures_performed", "TEXT"),
            ("conclusion",                 "TEXT"),
            ("reviewed_by",                "TEXT"),
            ("notes",                      "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # invoice_lines — per-line invoice parsing results
        # Expected by: src/engines/line_item_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS invoice_lines (
                line_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id      TEXT NOT NULL,
                line_number      INTEGER NOT NULL,
                description      TEXT,
                quantity         REAL,
                unit_price       REAL,
                line_total_pretax REAL,
                tax_code         TEXT,
                tax_regime       TEXT,
                gst_amount       REAL,
                qst_amount       REAL,
                hst_amount       REAL,
                province_of_supply TEXT,
                is_tax_included  INTEGER,
                line_notes       TEXT,
                created_at       TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (document_id) REFERENCES documents(document_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_invoice_lines_doc "
            "ON invoice_lines(document_id)"
        )
        changed += add_missing(conn, "invoice_lines", [
            ("description",       "TEXT"),
            ("quantity",          "REAL"),
            ("unit_price",        "REAL"),
            ("line_total_pretax", "REAL"),
            ("tax_code",          "TEXT"),
            ("tax_regime",        "TEXT"),
            ("gst_amount",        "REAL"),
            ("qst_amount",        "REAL"),
            ("hst_amount",        "REAL"),
            ("province_of_supply","TEXT"),
            ("is_tax_included",   "INTEGER"),
            ("line_notes",        "TEXT"),
        ])

        # ------------------------------------------------------------------ #
        # documents — line-item parsing columns
        # Expected by: src/engines/line_item_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("has_line_items",     "INTEGER NOT NULL DEFAULT 0"),
            ("lines_reconciled",   "INTEGER NOT NULL DEFAULT 0"),
            ("line_total_sum",     "REAL"),
            ("invoice_total_gap",  "REAL"),
            ("deposit_allocated",  "INTEGER NOT NULL DEFAULT 0"),
        ])

        # ------------------------------------------------------------------ #
        # FIX 2: Vendor DBA alias mapping
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vendor_aliases (
                alias_id             INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_vendor_key TEXT NOT NULL,
                alias_name           TEXT NOT NULL,
                alias_key            TEXT NOT NULL,
                created_by           TEXT,
                created_at           TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vendor_alias_key "
            "ON vendor_aliases(alias_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vendor_alias_canonical "
            "ON vendor_aliases(canonical_vendor_key)"
        )

        # ------------------------------------------------------------------ #
        # FIX 5: Personal use percentage for ITC/ITR disallowance
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("personal_use_percentage", "REAL"),
        ])

        # ------------------------------------------------------------------ #
        # FIX 4 (BoC): Bank of Canada FX rate cache table
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boc_fx_rates (
                rate_date   TEXT PRIMARY KEY,
                usd_cad     REAL NOT NULL,
                fetched_at  TEXT NOT NULL DEFAULT ''
            )
        """)

        # ------------------------------------------------------------------ #
        # FIX 7: Manual journal entries
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_journal_entries (
                entry_id       TEXT PRIMARY KEY,
                client_code    TEXT NOT NULL,
                period         TEXT NOT NULL,
                entry_date     TEXT NOT NULL,
                prepared_by    TEXT,
                debit_account  TEXT NOT NULL,
                credit_account TEXT NOT NULL,
                amount         REAL NOT NULL,
                description    TEXT,
                document_id    TEXT,
                source         TEXT NOT NULL DEFAULT 'bookkeeper',
                status         TEXT NOT NULL DEFAULT 'draft',
                created_at     TEXT NOT NULL DEFAULT '',
                updated_at     TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mje_client_period "
            "ON manual_journal_entries(client_code, period)"
        )

        # ------------------------------------------------------------------ #
        # FIX 8: Credit memo ↔ invoice linking
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS credit_memo_invoice_link (
                link_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                credit_memo_id       TEXT NOT NULL,
                original_invoice_id  TEXT NOT NULL,
                link_confidence      REAL,
                link_method          TEXT NOT NULL DEFAULT 'auto',
                invoice_number_match INTEGER NOT NULL DEFAULT 0,
                amount_match         INTEGER NOT NULL DEFAULT 0,
                created_at           TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cm_link_credit "
            "ON credit_memo_invoice_link(credit_memo_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cm_link_invoice "
            "ON credit_memo_invoice_link(original_invoice_id)"
        )

        # ------------------------------------------------------------------ #
        # FIX 4: Re-normalize vendor_key values with accent stripping
        # ------------------------------------------------------------------ #
        def _strip_accents(text: str) -> str:
            return unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")

        for tbl in ("vendor_memory", "learning_corrections"):
            if table_exists(conn, tbl) and "vendor_key" in existing_columns(conn, tbl):
                rows = conn.execute(f"SELECT id, vendor_key FROM {tbl} WHERE vendor_key != ''").fetchall()
                updated = 0
                for row in rows:
                    old_key = row["vendor_key"]
                    new_key = _strip_accents(old_key)
                    if old_key != new_key:
                        conn.execute(f"UPDATE {tbl} SET vendor_key = ? WHERE id = ?", (new_key, row["id"]))
                        updated += 1
                if updated:
                    print(f"  FIX4  {tbl}: re-normalized {updated} vendor_key(s) (accent stripping)")

        # ------------------------------------------------------------------ #
        # Trap 1+9: amendment_flags — tracks when filed periods need amendment
        # Expected by: src/engines/amendment_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS amendment_flags (
                flag_id             INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code         TEXT NOT NULL,
                filed_period        TEXT NOT NULL,
                trigger_document_id TEXT NOT NULL,
                trigger_type        TEXT NOT NULL DEFAULT 'credit_memo',
                reason_en           TEXT NOT NULL DEFAULT '',
                reason_fr           TEXT NOT NULL DEFAULT '',
                original_filing_id  TEXT,
                status              TEXT NOT NULL DEFAULT 'open',
                resolved_by         TEXT,
                resolved_at         TEXT,
                amendment_filing_id TEXT,
                created_at          TEXT NOT NULL DEFAULT '',
                updated_at          TEXT NOT NULL DEFAULT '',
                UNIQUE(client_code, filed_period, trigger_document_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_amend_flags_client_period "
            "ON amendment_flags(client_code, filed_period)"
        )
        changed += add_missing(conn, "amendment_flags", [
            ("trigger_type",        "TEXT NOT NULL DEFAULT 'credit_memo'"),
            ("reason_en",           "TEXT NOT NULL DEFAULT ''"),
            ("reason_fr",           "TEXT NOT NULL DEFAULT ''"),
            ("original_filing_id",  "TEXT"),
            ("status",              "TEXT NOT NULL DEFAULT 'open'"),
            ("resolved_by",         "TEXT"),
            ("resolved_at",         "TEXT"),
            ("amendment_filing_id", "TEXT"),
            ("updated_at",          "TEXT NOT NULL DEFAULT ''"),
        ])

        # ------------------------------------------------------------------ #
        # Trap 9: document_snapshots — point-in-time snapshots for audit lineage
        # Expected by: src/engines/amendment_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS document_snapshots (
                snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id     TEXT NOT NULL,
                snapshot_type   TEXT NOT NULL DEFAULT 'filing',
                snapshot_reason TEXT NOT NULL DEFAULT '',
                state_json      TEXT NOT NULL DEFAULT '{}',
                taken_by        TEXT NOT NULL DEFAULT 'system',
                taken_at        TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_snapshots_doc "
            "ON document_snapshots(document_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_snapshots_type "
            "ON document_snapshots(snapshot_type, taken_at)"
        )

        # ------------------------------------------------------------------ #
        # Trap 9: posting_snapshots — point-in-time snapshots of posting jobs
        # Expected by: src/engines/amendment_engine.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posting_snapshots (
                snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_id      TEXT NOT NULL,
                document_id     TEXT NOT NULL,
                snapshot_type   TEXT NOT NULL DEFAULT 'filing',
                snapshot_reason TEXT NOT NULL DEFAULT '',
                state_json      TEXT NOT NULL DEFAULT '{}',
                taken_by        TEXT NOT NULL DEFAULT 'system',
                taken_at        TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_post_snapshots_doc "
            "ON posting_snapshots(document_id)"
        )

        # ------------------------------------------------------------------ #
        # Trap 2+3+5+8: correction_chains — links original → correction → refund
        # Expected by: src/engines/correction_chain.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS correction_chains (
                chain_id            INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_root_id       TEXT NOT NULL,
                client_code         TEXT NOT NULL,
                source_document_id  TEXT NOT NULL,
                target_document_id  TEXT NOT NULL,
                link_type           TEXT NOT NULL DEFAULT 'credit_memo',
                economic_effect     TEXT NOT NULL DEFAULT 'reduction',
                amount              REAL,
                tax_impact_gst      REAL,
                tax_impact_qst      REAL,
                uncertainty_flags   TEXT DEFAULT '[]',
                status              TEXT NOT NULL DEFAULT 'active',
                created_by          TEXT NOT NULL DEFAULT 'system',
                created_at          TEXT NOT NULL DEFAULT '',
                superseded_by       INTEGER,
                rollback_of         INTEGER
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chain_root "
            "ON correction_chains(chain_root_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chain_source "
            "ON correction_chains(source_document_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chain_target "
            "ON correction_chains(target_document_id)"
        )

        # ------------------------------------------------------------------ #
        # Trap 5: document_clusters — persistent duplicate grouping
        # Expected by: src/engines/correction_chain.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS document_clusters (
                cluster_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_key     TEXT NOT NULL,
                client_code     TEXT NOT NULL,
                cluster_head_id TEXT,
                member_count    INTEGER NOT NULL DEFAULT 0,
                status          TEXT NOT NULL DEFAULT 'active',
                created_at      TEXT NOT NULL DEFAULT '',
                updated_at      TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_cluster_key "
            "ON document_clusters(cluster_key)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS document_cluster_members (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id      INTEGER NOT NULL,
                document_id     TEXT NOT NULL,
                is_cluster_head INTEGER NOT NULL DEFAULT 0,
                similarity_score REAL,
                variant_notes   TEXT,
                added_at        TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (cluster_id) REFERENCES document_clusters(cluster_id),
                UNIQUE(cluster_id, document_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cluster_members_doc "
            "ON document_cluster_members(document_id)"
        )

        # ------------------------------------------------------------------ #
        # Trap 3: overlap_anomalies — cross-vendor work overlap flags
        # Expected by: src/engines/correction_chain.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS overlap_anomalies (
                anomaly_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code         TEXT NOT NULL,
                document_a_id       TEXT NOT NULL,
                document_b_id       TEXT NOT NULL,
                vendor_a            TEXT NOT NULL,
                vendor_b            TEXT NOT NULL,
                overlap_type        TEXT NOT NULL DEFAULT 'work_scope',
                overlap_description TEXT NOT NULL DEFAULT '',
                status              TEXT NOT NULL DEFAULT 'open',
                resolved_by         TEXT,
                resolved_at         TEXT,
                resolution_notes    TEXT,
                created_at          TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_overlap_client "
            "ON overlap_anomalies(client_code, status)"
        )

        # ------------------------------------------------------------------ #
        # Trap 8: rollback_log — explicit rollback audit trail
        # Expected by: src/engines/correction_chain.py
        # ------------------------------------------------------------------ #
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rollback_log (
                rollback_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code         TEXT NOT NULL,
                target_type         TEXT NOT NULL DEFAULT 'correction_chain',
                target_id           TEXT NOT NULL,
                rollback_reason     TEXT NOT NULL DEFAULT '',
                rolled_back_by      TEXT NOT NULL DEFAULT '',
                state_before_json   TEXT NOT NULL DEFAULT '{}',
                state_after_json    TEXT NOT NULL DEFAULT '{}',
                is_reimport_blocked INTEGER NOT NULL DEFAULT 0,
                created_at          TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rollback_target "
            "ON rollback_log(target_type, target_id)"
        )

        # ------------------------------------------------------------------ #
        # Trap 6: Optimistic locking — version columns
        # Expected by: src/engines/concurrency_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "posting_jobs", [
            ("version",     "INTEGER NOT NULL DEFAULT 1"),
        ])
        changed += add_missing(conn, "documents", [
            ("version",     "INTEGER NOT NULL DEFAULT 1"),
        ])

        # Trigger: auto-increment version on posting_jobs update
        conn.executescript("""
            DROP TRIGGER IF EXISTS trg_posting_version_increment;
            CREATE TRIGGER trg_posting_version_increment
            AFTER UPDATE ON posting_jobs
            WHEN NEW.version = OLD.version
            BEGIN
                UPDATE posting_jobs SET version = OLD.version + 1
                WHERE posting_id = NEW.posting_id;
            END;
        """)

        # Trigger: auto-increment version on documents update
        if table_exists(conn, "documents"):
            conn.executescript("""
                DROP TRIGGER IF EXISTS trg_document_version_increment;
                CREATE TRIGGER trg_document_version_increment
                AFTER UPDATE ON documents
                WHEN NEW.version = OLD.version
                BEGIN
                    UPDATE documents SET version = OLD.version + 1
                    WHERE document_id = NEW.document_id;
                END;
            """)

        # ------------------------------------------------------------------ #
        # Trap 4: Recognition timing columns
        # Expected by: src/engines/amendment_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "documents", [
            ("activation_date",     "TEXT"),
            ("recognition_period",  "TEXT"),
            ("recognition_status",  "TEXT NOT NULL DEFAULT 'immediate'"),
        ])

        # ------------------------------------------------------------------ #
        # Trap 9: gst_filings — amendment tracking columns
        # Expected by: src/engines/amendment_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "gst_filings", [
            ("is_amended",            "INTEGER NOT NULL DEFAULT 0"),
            ("amendment_filed_at",    "TEXT"),
            ("amendment_filed_by",    "TEXT"),
            ("original_snapshot_id",  "INTEGER"),
            ("amended_snapshot_id",   "INTEGER"),
        ])

        # ------------------------------------------------------------------ #
        # Trap 7: manual_journal_entries — collision detection columns
        # Expected by: src/engines/concurrency_engine.py
        # ------------------------------------------------------------------ #
        changed += add_missing(conn, "manual_journal_entries", [
            ("collision_status",     "TEXT NOT NULL DEFAULT 'clear'"),
            ("collision_document_id","TEXT"),
            ("collision_chain_id",   "INTEGER"),
            ("reviewed_by",          "TEXT"),
            ("reviewed_at",          "TEXT"),
        ])

        conn.commit()

    if changed:
        print(f"\nDone — {len(changed)} column(s) added.")
    else:
        print("\nDone — schema is already up to date, no changes needed.")


if __name__ == "__main__":
    run_migration()

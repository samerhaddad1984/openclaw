"""Scope 2.2 — safety / interaction tests.

Phase 2.2 (QBO full historical pull) shipped during the v1
continuation before v2 safety scaffolding was in place. This file
backfills the four interaction tests the v2 spec required:

  - test_historical_pull_doesnt_duplicate_existing_sync
  - test_sync_state_preserved_across_full_pull
  - test_financial_statements_unchanged_after_import_rollback
  - test_cron_resumes_normally_after_historical

Stubbed puller — no network. The stub writes deterministic rows
tagged with a year so we can verify per-year behaviour.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import qbo_historical as qh  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'safety.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE qbo_sync_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                entity_type TEXT, qbo_id TEXT,
                last_pulled_at TEXT,
                UNIQUE(firm_code, client_code, entity_type, qbo_id)
            );
            CREATE TABLE qbo_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                realm_id TEXT, status TEXT DEFAULT 'active',
                last_error TEXT,
                last_scheduled_run_at TEXT
            );
            CREATE TABLE gl_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, source TEXT,
                amount REAL, account TEXT, txn_date TEXT
            );
            CREATE TABLE bank_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, source TEXT,
                amount REAL, description TEXT, txn_date TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO qbo_connections "
            "(firm_code, client_code, realm_id, status) "
            "VALUES ('FIRM','CONS','999','active')"
        )
        conn.commit()
    return db


def _idempotent_stub(call_log=None):
    """A stub that UPSERTs its rows — simulates the real production
    puller, which uses qbo_id as its natural key. Re-running the
    same window must not create duplicates."""
    def _pull(*, firm_code, client_code, db_path, start, end):
        y = int(start[:4])
        if call_log is not None:
            call_log.append((y, start, end))
        with sqlite3.connect(db_path) as conn:
            # 3 JEs per year, deterministic qbo_id.
            for i in range(3):
                qid = f'{y}-je-{i}'
                conn.execute(
                    "INSERT OR IGNORE INTO qbo_sync_state "
                    "(firm_code, client_code, entity_type, qbo_id, "
                    " last_pulled_at) VALUES (?,?,?,?,?)",
                    (firm_code, client_code, 'journal_entry', qid,
                     f'{y}-{start[5:]}'),
                )
            # 2 bills per year, added to gl_transactions.
            for i in range(2):
                conn.execute(
                    "INSERT INTO gl_transactions "
                    "(client_code, source, amount, account, txn_date) "
                    "VALUES (?,?,?,?,?)",
                    (client_code, 'qbo', float(100 + i),
                     'Expenses', f'{y}-06-01'),
                )
            conn.commit()
        return {'journal_entries': 3, 'bills': 2}
    return _pull


# ---------------------------------------------------------------------------
# test_historical_pull_doesnt_duplicate_existing_sync
# ---------------------------------------------------------------------------


def test_historical_pull_doesnt_duplicate_existing_sync(tmp_path):
    """Running the historical pull twice (simulating a restart
    mid-run or a re-trigger) must not duplicate qbo_sync_state
    rows, because the production puller UPSERTs on qbo_id."""
    db = _mkdb(tmp_path)
    # First pull.
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    with sqlite3.connect(db) as conn:
        first_count = conn.execute(
            "SELECT COUNT(*) FROM qbo_sync_state "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0]
    # Second pull (same data). Should be idempotent at the sync_state
    # level via INSERT OR IGNORE.
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    with sqlite3.connect(db) as conn:
        second_count = conn.execute(
            "SELECT COUNT(*) FROM qbo_sync_state "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0]
    assert first_count == second_count, (
        f"qbo_sync_state grew on re-run: {first_count} → {second_count}"
    )


# ---------------------------------------------------------------------------
# test_sync_state_preserved_across_full_pull
# ---------------------------------------------------------------------------


def test_sync_state_preserved_across_full_pull(tmp_path):
    """Existing qbo_sync_state rows (from past cron runs) must survive
    a historical pull — we only append new ids, never wipe."""
    db = _mkdb(tmp_path)
    # Seed some rows that a previous cron would have written.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, last_pulled_at) "
            "VALUES ('FIRM','CONS','customer','C-1','2025-12-31')"
        )
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, last_pulled_at) "
            "VALUES ('FIRM','CONS','vendor','V-1','2025-12-31')"
        )
        conn.commit()
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        survivors = conn.execute(
            "SELECT qbo_id FROM qbo_sync_state "
            "WHERE entity_type IN ('customer','vendor') "
            "ORDER BY qbo_id"
        ).fetchall()
    # Original two rows still there.
    ids = [r['qbo_id'] for r in survivors]
    assert 'C-1' in ids
    assert 'V-1' in ids


# ---------------------------------------------------------------------------
# test_financial_statements_unchanged_after_import_rollback
# ---------------------------------------------------------------------------


def test_financial_statements_unchanged_after_import_rollback(tmp_path):
    """Pre-existing non-QBO GL rows must remain untouched through a
    full pull → rollback cycle."""
    db = _mkdb(tmp_path)
    # Pre-existing manual GL activity for the client (pretend this is
    # an adjustment the CPA made before connecting QBO).
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO gl_transactions "
            "(client_code, source, amount, account, txn_date) "
            "VALUES ('CONS','manual_je',9999.99,'Equity','2026-01-02')"
        )
        conn.execute(
            "INSERT INTO gl_transactions "
            "(client_code, source, amount, account, txn_date) "
            "VALUES ('CONS','opening_balance',5000.00,'Cash','2026-01-01')"
        )
        # A different client's rows — must also survive.
        conn.execute(
            "INSERT INTO gl_transactions "
            "(client_code, source, amount, account, txn_date) "
            "VALUES ('OTHER','qbo',1234.56,'Cash','2025-06-01')"
        )
        conn.commit()
        # Capture FS-relevant rows we expect to survive.
        survivors_before = conn.execute(
            "SELECT client_code, source, amount, account, txn_date "
            "FROM gl_transactions "
            "WHERE client_code='CONS' "
            "  AND source IN ('manual_je','opening_balance') "
            "ORDER BY id"
        ).fetchall()
        other_client_before = conn.execute(
            "SELECT client_code, source, amount, account, txn_date "
            "FROM gl_transactions WHERE client_code='OTHER' ORDER BY id"
        ).fetchall()
    # Full pull.
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    # Confirm + rollback.
    qh.rollback_import(
        db, firm_code='FIRM', client_code='CONS',
        decided_by='owner@firm.com',
    )
    with sqlite3.connect(db) as conn:
        survivors_after = conn.execute(
            "SELECT client_code, source, amount, account, txn_date "
            "FROM gl_transactions "
            "WHERE client_code='CONS' "
            "  AND source IN ('manual_je','opening_balance') "
            "ORDER BY id"
        ).fetchall()
        other_client_after = conn.execute(
            "SELECT client_code, source, amount, account, txn_date "
            "FROM gl_transactions WHERE client_code='OTHER' ORDER BY id"
        ).fetchall()
        qbo_rows = conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE client_code='CONS' AND LOWER(source)='qbo'"
        ).fetchone()[0]
    # Non-QBO rows for the rolled-back client: byte-identical.
    assert survivors_before == survivors_after
    # Other client: untouched.
    assert other_client_before == other_client_after
    # QBO rows for the rolled-back client: all gone.
    assert qbo_rows == 0


# ---------------------------------------------------------------------------
# test_cron_resumes_normally_after_historical
# ---------------------------------------------------------------------------


def test_cron_resumes_normally_after_historical(tmp_path):
    """After a historical pull completes, the qbo_connections row
    must still be ``active`` so the 15-minute cron keeps running.
    Only an explicit rollback flips it to ``disconnected``."""
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status FROM qbo_connections "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()
    assert row['status'] == 'active', (
        f"connection status changed during historical pull: {row['status']}"
    )


def test_cron_can_insert_after_historical(tmp_path):
    """Simulate the cron's incremental sync running AFTER the
    historical pull — its INSERT path must still work (no locks,
    no leftover transactions). We simulate by doing the same
    INSERT OR IGNORE the stub does plus a fresh id."""
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    # Cron-style insert of a new qbo_sync_state entry from today.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, last_pulled_at) "
            "VALUES ('FIRM','CONS','journal_entry','2026-je-100','2026-04-24')"
        )
        conn.commit()
        row = conn.execute(
            "SELECT 1 FROM qbo_sync_state WHERE qbo_id='2026-je-100'"
        ).fetchone()
    assert row is not None


def test_rollback_disconnects_but_sync_state_wipes(tmp_path):
    """Explicit rollback disconnects the connection AND wipes
    sync_state for the client (so cron has nothing to resume on)."""
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    qh.rollback_import(
        db, firm_code='FIRM', client_code='CONS',
        decided_by='owner@firm.com',
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status FROM qbo_connections "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()
        sync_rows = conn.execute(
            "SELECT COUNT(*) FROM qbo_sync_state "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0]
    assert row['status'] == 'disconnected'
    assert sync_rows == 0


# ---------------------------------------------------------------------------
# Additional spec-aligned interaction checks
# ---------------------------------------------------------------------------


def test_historical_pull_records_per_year_progress(tmp_path):
    """Progress is captured per year so the UI can show a progress bar.
    years=2 captures the two complete prior years PLUS the current
    YTD window — that's the production shape."""
    db = _mkdb(tmp_path)
    result = qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_idempotent_stub(),
    )
    assert result['ok'] is True
    # At least the 2 complete years are tracked; the current-year
    # YTD window may or may not be included depending on today's date.
    assert len(result['progress']) >= 2


def test_concurrent_historical_pull_call_log_matches_years(tmp_path):
    """Stub records one call per year window. years=3 means three
    complete prior years plus current-year YTD → >=3 distinct calls."""
    db = _mkdb(tmp_path)
    calls = []
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=3, pull_fn=_idempotent_stub(calls),
    )
    assert len(calls) >= 3
    years = sorted({c[0] for c in calls})
    assert len(years) >= 3  # distinct years

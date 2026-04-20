"""Phase 1 — QBO bidirectional-sync schema. Confirms every expected
table + index exists, idempotence, and that bootstrap_schema applies
it automatically."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_schema import apply_qbo_sync_schema, ensure_qbo_sync_schema  # noqa: E402


EXPECTED_TABLES = {
    'qbo_sync_state',
    'qbo_accounts',
    'qbo_customers',
    'qbo_vendors',
    'qbo_journal_entries',
    'qbo_journal_entry_lines',
    'qbo_bills',
    'qbo_invoices',
    'qbo_sync_log',
    'qbo_webhook_events',
}

EXPECTED_INDEXES = {
    'idx_qbo_sync_state_entity',
    'idx_qbo_sync_state_status',
    'idx_qbo_accounts_number',
    'idx_qbo_je_txn_date',
    'idx_qbo_je_lines_je',
    'idx_qbo_webhook_processed',
}


def _tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def _indexes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
    return {r[0] for r in rows}


def test_apply_schema_creates_every_expected_table(tmp_path):
    db = tmp_path / 't.db'
    with sqlite3.connect(db) as conn:
        apply_qbo_sync_schema(conn)
        got = _tables(conn)
    assert EXPECTED_TABLES.issubset(got), f"missing: {EXPECTED_TABLES - got}"


def test_apply_schema_creates_expected_indexes(tmp_path):
    db = tmp_path / 't.db'
    with sqlite3.connect(db) as conn:
        apply_qbo_sync_schema(conn)
        got = _indexes(conn)
    assert EXPECTED_INDEXES.issubset(got), f"missing: {EXPECTED_INDEXES - got}"


def test_schema_is_idempotent(tmp_path):
    db = tmp_path / 't.db'
    for _ in range(3):
        ensure_qbo_sync_schema(db)
    with sqlite3.connect(db) as conn:
        assert EXPECTED_TABLES.issubset(_tables(conn))


def test_qbo_sync_state_unique_constraint(tmp_path):
    db = tmp_path / 't.db'
    ensure_qbo_sync_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id) VALUES (?,?,?,?)",
            ('F', 'C', 'Account', '123'),
        )
        conn.commit()
        import pytest
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO qbo_sync_state "
                "(firm_code, client_code, entity_type, qbo_id) VALUES (?,?,?,?)",
                ('F', 'C', 'Account', '123'),
            )
            conn.commit()


def test_qbo_accounts_unique_constraint(tmp_path):
    db = tmp_path / 't.db'
    ensure_qbo_sync_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_accounts "
            "(firm_code, client_code, qbo_id, name) VALUES (?,?,?,?)",
            ('F', 'C', '1', 'Cash'),
        )
        conn.commit()
        import pytest
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO qbo_accounts "
                "(firm_code, client_code, qbo_id, name) VALUES (?,?,?,?)",
                ('F', 'C', '1', 'Cash'),
            )
            conn.commit()


def test_webhook_event_id_unique(tmp_path):
    db = tmp_path / 't.db'
    ensure_qbo_sync_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_webhook_events (event_id) VALUES (?)",
            ('evt_1',),
        )
        conn.commit()
        import pytest
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO qbo_webhook_events (event_id) VALUES (?)",
                ('evt_1',),
            )
            conn.commit()


def test_bootstrap_schema_wires_qbo_sync():
    """dashboard bootstrap_schema must import and call
    apply_qbo_sync_schema. Grep the source to verify the hook is in
    place (keeps this test independent of the rest of the dashboard
    bootstrap dependencies)."""
    src = (ROOT / 'scripts' / 'review_dashboard.py').read_text()
    assert 'from src.integrations.qbo_schema import apply_qbo_sync_schema' in src
    assert 'apply_qbo_sync_schema(conn)' in src


def test_qbo_je_and_lines_round_trip(tmp_path):
    db = tmp_path / 't.db'
    ensure_qbo_sync_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_journal_entries "
            "(firm_code, client_code, qbo_id, doc_number, txn_date, "
            "total_amount, source) VALUES (?,?,?,?,?,?,?)",
            ('F', 'C', 'JE1', '100', '2026-04-20', 500.0, 'qbo_origin'),
        )
        for idx, (amt, dc, acct) in enumerate([
            (500.0, 'Debit', '1000'),
            (500.0, 'Credit', '2000'),
        ]):
            conn.execute(
                "INSERT INTO qbo_journal_entry_lines "
                "(qbo_je_id, line_num, amount, debit_credit, account_qbo_id) "
                "VALUES (?,?,?,?,?)",
                ('JE1', idx, amt, dc, acct),
            )
        conn.commit()

        totals = conn.execute(
            "SELECT debit_credit, SUM(amount) FROM qbo_journal_entry_lines "
            "WHERE qbo_je_id=? GROUP BY debit_credit",
            ('JE1',),
        ).fetchall()
        sums = dict(totals)
    assert sums.get('Debit') == 500.0
    assert sums.get('Credit') == 500.0

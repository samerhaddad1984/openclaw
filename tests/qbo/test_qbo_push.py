"""Phase 4 — QBO push: JEs (create/update/void), Bills. Uses FakeQBO."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_push import QBOPush, QBOPushConflict  # noqa: E402
from tests.qbo._stubs import FakeQBO, make_db, patch_request_with_fake  # noqa: E402


def _bootstrap_je_source(db: Path):
    """Seed manual_journal_entries + accounts so push_journal_entry works."""
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_journal_entries (
                entry_id TEXT PRIMARY KEY,
                client_code TEXT, period TEXT, entry_date TEXT,
                debit_account TEXT, credit_account TEXT,
                amount REAL, description TEXT,
                document_id TEXT, status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                document_id TEXT PRIMARY KEY, vendor TEXT,
                document_date TEXT, due_date TEXT,
                amount REAL, gl_account TEXT,
                invoice_number TEXT
            )
        """)
        conn.commit()


def _seed_accounts_and_vendors(db: Path, firm='F1', client='C1'):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_accounts (firm_code, client_code, qbo_id, "
            "name, account_number) VALUES (?,?,?,?,?)",
            (firm, client, '1', 'Cash', '1000'),
        )
        conn.execute(
            "INSERT INTO qbo_accounts (firm_code, client_code, qbo_id, "
            "name, account_number) VALUES (?,?,?,?,?)",
            (firm, client, '2', 'Office', '5400'),
        )
        conn.execute(
            "INSERT INTO qbo_vendors (firm_code, client_code, qbo_id, "
            "display_name) VALUES (?,?,?,?)",
            (firm, client, '100', 'Acme Supplies'),
        )
        conn.commit()


def _mk_push(tmp_path):
    db = make_db(tmp_path)
    _bootstrap_je_source(db)
    _seed_accounts_and_vendors(db)
    fake = FakeQBO()
    push = QBOPush('F1', 'C1', db_path=db, sandbox=True)
    patch_request_with_fake(push, fake)
    return push, db, fake


# --- JE create ---

def test_push_je_creates_in_qbo(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-001', 'C1', '2026-04', '2026-04-20', '5400', '1000',
             75.25, 'office supplies', 'draft'),
        )
        conn.commit()

    result = push.push_journal_entry('je-001')
    assert result['status'] == 'ok'
    assert result['qbo_id'] == 'JE-NEW'
    assert result['sync_token'] == '0'

    # Side effects:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        state = dict(conn.execute(
            "SELECT * FROM qbo_sync_state WHERE local_id='je-001'"
        ).fetchone())
        mirror = dict(conn.execute(
            "SELECT * FROM qbo_journal_entries WHERE local_je_id='je-001'"
        ).fetchone())
    assert state['entity_type'] == 'JournalEntry'
    assert state['sync_source'] == 'otocpa_origin'
    assert state['qbo_sync_token'] == '0'
    assert state['last_pushed_at']
    assert mirror['source'] == 'otocpa_origin'
    assert mirror['total_amount'] == 75.25


def test_push_je_missing_account_mapping_raises(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-bad', 'C1', '2026-04', '2026-04-20', 'UNMAPPED', '1000',
             10.0, '', 'draft'),
        )
        conn.commit()
    with pytest.raises(RuntimeError, match='QBO account mapping'):
        push.push_journal_entry('je-bad')


def test_push_je_payload_shape(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-p', 'C1', '2026-04', '2026-04-18', '5400', '1000',
             12.50, 'office', 'draft'),
        )
        conn.commit()

    push.push_journal_entry('je-p')
    posted = [c for c in fake.call_log if c[0] == 'POST']
    assert posted, "No POST recorded"
    body = posted[0][2]
    assert body['TxnDate'] == '2026-04-18'
    assert body['PrivateNote'] == 'office'
    assert len(body['Line']) == 2
    assert body['Line'][0]['JournalEntryLineDetail']['PostingType'] == 'Debit'
    assert body['Line'][0]['JournalEntryLineDetail']['AccountRef']['value'] == '2'
    assert body['Line'][1]['JournalEntryLineDetail']['PostingType'] == 'Credit'
    assert body['Line'][1]['JournalEntryLineDetail']['AccountRef']['value'] == '1'


# --- JE update ---

def test_push_je_update_requires_prior_push(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-2', 'C1', '2026-04', '2026-04-20', '5400', '1000',
             25.0, '', 'draft'),
        )
        conn.commit()
    with pytest.raises(RuntimeError, match='No prior QBO push'):
        push.push_journal_entry_update('je-2')


def test_push_je_update_sends_current_sync_token(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-u', 'C1', '2026-04', '2026-04-20', '5400', '1000',
             50.0, '', 'draft'),
        )
        conn.commit()
    push.push_journal_entry('je-u')

    # Update should send Id + SyncToken from prior push
    fake.call_log.clear()
    push.push_journal_entry_update('je-u')
    posted = [c for c in fake.call_log if c[0] == 'POST']
    body = posted[0][2]
    assert body.get('Id') == 'JE-NEW'
    assert body.get('SyncToken') == '0'


# --- JE void ---

def test_push_je_void_after_create(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-v', 'C1', '2026-04', '2026-04-20', '5400', '1000',
             25.0, '', 'draft'),
        )
        conn.commit()
    push.push_journal_entry('je-v')
    result = push.push_journal_entry_void('je-v')
    assert result['status'] == 'voided'
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT sync_status FROM qbo_sync_state WHERE local_id='je-v'"
        ).fetchone()[0]
    assert status == 'voided'


def test_push_je_void_without_prior_push_raises(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with pytest.raises(RuntimeError, match='No prior QBO push'):
        push.push_journal_entry_void('never-pushed')


# --- Bill push ---

def test_push_bill_creates_in_qbo(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, vendor, document_date, "
            "due_date, amount, gl_account, invoice_number) VALUES "
            "(?,?,?,?,?,?,?)",
            ('DOC-1', 'Acme Supplies', '2026-04-20', '2026-05-20',
             125.00, '5400', 'INV-99'),
        )
        conn.commit()
    # Fake a Bill response
    def bill_response(method, path, body=None):
        fake.call_log.append((method, path, body))
        if '/bill' in path:
            return {'Bill': {'Id': 'BILL-NEW', 'SyncToken': '0',
                              'MetaData': {'LastUpdatedTime': '2026-04-20T12:00:00Z'}}}
        return {}
    push._request = bill_response  # type: ignore[method-assign]

    result = push.push_bill('DOC-1')
    assert result['qbo_id'] == 'BILL-NEW'
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT qbo_id, source, local_document_id FROM qbo_bills "
            "WHERE local_document_id='DOC-1'"
        ).fetchone()
        state = conn.execute(
            "SELECT sync_source FROM qbo_sync_state WHERE local_id='DOC-1'"
        ).fetchone()
    assert row == ('BILL-NEW', 'otocpa_origin', 'DOC-1')
    assert state[0] == 'otocpa_origin'


def test_push_bill_missing_vendor_mapping_raises(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, vendor, document_date, "
            "amount, gl_account) VALUES (?,?,?,?,?)",
            ('DOC-X', 'Unknown Corp', '2026-04-20', 10.0, '5400'),
        )
        conn.commit()
    with pytest.raises(RuntimeError, match='No QBO vendor mapped'):
        push.push_bill('DOC-X')


# --- Conflict surface ---

def test_push_conflict_raises_QBOPushConflict(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-c', 'C1', '2026-04', '2026-04-20', '5400', '1000',
             25.0, '', 'draft'),
        )
        conn.commit()
    push.push_journal_entry('je-c')

    # Now make the update call raise a simulated stale-object error
    def conflict_resp(method, path, body=None):
        raise RuntimeError("QBO POST /journalentry failed: 400 'Stale Object Update'")
    push._request = conflict_resp  # type: ignore[method-assign]

    with pytest.raises(QBOPushConflict):
        push.push_journal_entry_update('je-c')


def test_push_records_last_pushed_at(tmp_path):
    push, db, fake = _mk_push(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            ('je-ts', 'C1', '2026-04', '2026-04-20', '5400', '1000',
             5.0, '', 'draft'),
        )
        conn.commit()
    push.push_journal_entry('je-ts')
    with sqlite3.connect(db) as conn:
        ts = conn.execute(
            "SELECT last_pushed_at FROM qbo_sync_state WHERE local_id='je-ts'"
        ).fetchone()[0]
    assert ts is not None
    assert ts.startswith('20')  # ISO format

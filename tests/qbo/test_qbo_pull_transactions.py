"""Phase 3 — pull JournalEntries, Bills, Invoices. Confirm qbo_origin
JEs mirror into gl_transactions so TB/P&L/BS see them."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_pull import QBOPull  # noqa: E402
from tests.qbo._stubs import FakeQBO, make_db, patch_request_with_fake  # noqa: E402


def _je(qbo_id: str, *,
         txn_date: str = '2026-04-20',
         lines: list[tuple[float, str, str]],  # (amount, 'Debit'|'Credit', acct_id)
         last_mod: str = '2026-04-20T10:00:00Z',
         sync_token: str = '0',
         doc: str | None = None,
         memo: str | None = None) -> dict:
    return {
        'Id': qbo_id,
        'DocNumber': doc or qbo_id,
        'TxnDate': txn_date,
        'TotalAmt': sum(a for a, _, _ in lines if _ == 'Debit') or sum(a for a, _, _ in lines) / 2,
        'SyncToken': sync_token,
        'PrivateNote': memo,
        'MetaData': {'LastUpdatedTime': last_mod},
        'Line': [
            {
                'LineNum': i + 1,
                'Amount': amt,
                'DetailType': 'JournalEntryLineDetail',
                'Description': f'line {i + 1}',
                'JournalEntryLineDetail': {
                    'PostingType': side,
                    'AccountRef': {'value': acct},
                }
            }
            for i, (amt, side, acct) in enumerate(lines)
        ],
    }


def _mk_pull(tmp_path, fake):
    db = make_db(tmp_path)
    pull = QBOPull('F1', 'C1', db_path=db, sandbox=True)
    patch_request_with_fake(pull, fake)
    return pull, db


def _seed_accounts(fake):
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'AcctNum': '1000', 'Active': True, 'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
        {'Id': '2', 'Name': 'Office Exp', 'AccountType': 'Expense',
         'AcctNum': '5400', 'Active': True, 'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])


# --- JEs ---

def test_pull_journal_entries_first_time_full(tmp_path):
    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE1', lines=[(100.0, 'Debit', '2'), (100.0, 'Credit', '1')]),
        _je('JE2', lines=[(50.0, 'Debit', '2'), (50.0, 'Credit', '1')]),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    n = pull.pull_journal_entries()
    assert n == 2
    with sqlite3.connect(db) as conn:
        headers = conn.execute(
            "SELECT qbo_id, source, total_amount FROM qbo_journal_entries "
            "ORDER BY qbo_id"
        ).fetchall()
        lines = conn.execute(
            "SELECT qbo_je_id, COUNT(*) FROM qbo_journal_entry_lines GROUP BY qbo_je_id"
        ).fetchall()
    assert headers == [('JE1', 'qbo_origin', 100.0), ('JE2', 'qbo_origin', 50.0)]
    assert dict(lines) == {'JE1': 2, 'JE2': 2}


def test_pull_journal_entries_incremental_since_date(tmp_path):
    fake = FakeQBO()
    _seed_accounts(fake)
    # First pull: old JE
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE1', lines=[(100.0, 'Debit', '2'), (100.0, 'Credit', '1')],
            last_mod='2026-04-15T10:00:00Z'),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()

    # Second pull: filtered since 2026-04-16 → returns only JE2
    fake._queries.clear()
    fake.add_query(
        "SELECT * FROM JournalEntry WHERE MetaData.LastUpdatedTime >= '2026-04-16T00:00:00Z'",
        [_je('JE2', lines=[(50.0, 'Debit', '2'), (50.0, 'Credit', '1')],
             last_mod='2026-04-17T10:00:00Z')],
    )
    n = pull.pull_journal_entries(since_date='2026-04-16T00:00:00Z')
    assert n == 1
    with sqlite3.connect(db) as conn:
        ids = [r[0] for r in conn.execute(
            "SELECT qbo_id FROM qbo_journal_entries ORDER BY qbo_id").fetchall()]
    assert ids == ['JE1', 'JE2']


def test_pull_je_marks_qbo_origin_when_new(tmp_path):
    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-X', lines=[(9.0, 'Debit', '2'), (9.0, 'Credit', '1')]),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()
    with sqlite3.connect(db) as conn:
        src = conn.execute(
            "SELECT source FROM qbo_journal_entries WHERE qbo_id='JE-X'"
        ).fetchone()
    assert src[0] == 'qbo_origin'


def test_pull_je_preserves_otocpa_origin_source(tmp_path):
    """When sync_state is already otocpa_origin (e.g. we pushed this JE
    to QBO), a subsequent pull must NOT flip it back to qbo_origin."""
    db = make_db(tmp_path)
    # Seed qbo_sync_state as if we already pushed the JE.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, "
            "qbo_sync_token, sync_status, sync_source, "
            "last_pushed_at) VALUES (?,?,?,?,?,?,?,?)",
            ('F1', 'C1', 'JournalEntry', 'JE-MINE', '0',
             'synced', 'otocpa_origin', '2026-04-20T09:00:00Z'),
        )
        # Also seed qbo_journal_entries with local_je_id to preserve linkage.
        conn.execute(
            "INSERT INTO qbo_journal_entries "
            "(firm_code, client_code, qbo_id, local_je_id, "
            " source, last_synced) VALUES (?,?,?,?,?,?)",
            ('F1', 'C1', 'JE-MINE', 42, 'otocpa_origin', '2026-04-20T09:00:00Z'),
        )
        conn.commit()

    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-MINE', lines=[(10.0, 'Debit', '2'), (10.0, 'Credit', '1')],
            last_mod='2026-04-20T11:00:00Z'),
    ])
    pull = QBOPull('F1', 'C1', db_path=db, sandbox=True)
    patch_request_with_fake(pull, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()

    with sqlite3.connect(db) as conn:
        header = conn.execute(
            "SELECT source, local_je_id FROM qbo_journal_entries WHERE qbo_id='JE-MINE'"
        ).fetchone()
        state = conn.execute(
            "SELECT sync_source FROM qbo_sync_state WHERE qbo_id='JE-MINE'"
        ).fetchone()
    assert header[0] == 'otocpa_origin'
    assert header[1] == 42
    assert state[0] == 'otocpa_origin'


def test_pull_je_lines_stored_correctly(tmp_path):
    fake = FakeQBO()
    _seed_accounts(fake)
    # 4-line JE with customer reference
    je = _je('JE-M', lines=[
        (300.0, 'Debit', '2'), (100.0, 'Credit', '1'),
        (200.0, 'Credit', '1'), (0.0, 'Debit', '1'),
    ])
    # Attach a customer to line[0].
    je['Line'][0]['JournalEntryLineDetail']['Entity'] = {
        'Type': 'Customer',
        'EntityRef': {'value': 'CUST-9'},
    }
    fake.add_query('SELECT * FROM JournalEntry', [je])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT line_num, amount, debit_credit, account_qbo_id, customer_qbo_id "
            "FROM qbo_journal_entry_lines WHERE qbo_je_id='JE-M' "
            "ORDER BY line_num"
        ).fetchall()
    # The 4-line JE includes a 0-amount line that still stored (no filter here).
    assert len(rows) == 4
    assert rows[0] == (1, 300.0, 'Debit', '2', 'CUST-9')


def test_qbo_origin_je_mirrors_into_gl_transactions(tmp_path):
    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-GL', txn_date='2026-03-15',
            lines=[(200.0, 'Debit', '2'), (200.0, 'Credit', '1')],
            memo='April meals'),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()
    with sqlite3.connect(db) as conn:
        rows = sorted(conn.execute(
            "SELECT account_code, side, amount, source, period, entry_date "
            "FROM gl_transactions WHERE entry_id='QBO:JE-GL'"
        ).fetchall())
    assert rows == sorted([
        ('5400', 'debit', 200.0, 'qbo', '2026-03', '2026-03-15'),
        ('1000', 'credit', 200.0, 'qbo', '2026-03', '2026-03-15'),
    ])


def test_qbo_origin_trial_balance_debits_equal_credits(tmp_path):
    """Mirrored JEs keep debits=credits at the gl layer."""
    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-A', lines=[(100.0, 'Debit', '2'), (100.0, 'Credit', '1')]),
        _je('JE-B', lines=[(50.0, 'Debit', '2'), (50.0, 'Credit', '1')]),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()
    with sqlite3.connect(db) as conn:
        totals = dict(conn.execute(
            "SELECT side, SUM(amount) FROM gl_transactions WHERE source='qbo' GROUP BY side"
        ).fetchall())
    assert totals['debit'] == 150.0
    assert totals['credit'] == 150.0


def test_pull_je_idempotent_mirror(tmp_path):
    """Re-pulling the same JE replaces (not duplicates) GL rows."""
    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-I', lines=[(100.0, 'Debit', '2'), (100.0, 'Credit', '1')],
            last_mod='2026-04-20T10:00:00Z'),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()

    # Same data, newer timestamp + updated amount — expect GL re-written
    fake._queries.clear()
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-I', lines=[(150.0, 'Debit', '2'), (150.0, 'Credit', '1')],
            last_mod='2026-04-21T10:00:00Z'),
    ])
    pull.pull_journal_entries()
    with sqlite3.connect(db) as conn:
        total = conn.execute(
            "SELECT SUM(amount) FROM gl_transactions "
            "WHERE entry_id='QBO:JE-I' AND side='debit'"
        ).fetchone()[0]
    assert total == 150.0


# --- bills ---

def test_pull_bills_basic(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Bill', [
        {'Id': 'B1', 'VendorRef': {'value': 'V9'}, 'DocNumber': 'INV-1',
         'TxnDate': '2026-04-20', 'DueDate': '2026-05-20',
         'TotalAmt': 500.0, 'Balance': 500.0,
         'PrivateNote': 'quarterly invoice',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake)
    n = pull.pull_bills()
    assert n == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT doc_number, vendor_qbo_id, total_amount, memo, source "
            "FROM qbo_bills WHERE qbo_id='B1'"
        ).fetchone()
    assert row == ('INV-1', 'V9', 500.0, 'quarterly invoice', 'qbo_origin')


def test_pull_invoices_basic(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Invoice', [
        {'Id': 'I1', 'CustomerRef': {'value': 'C9'}, 'DocNumber': 'SI-1',
         'TxnDate': '2026-04-01', 'DueDate': '2026-05-01',
         'TotalAmt': 1500.0, 'Balance': 1500.0,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake)
    n = pull.pull_invoices()
    assert n == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT doc_number, customer_qbo_id, total_amount, source "
            "FROM qbo_invoices WHERE qbo_id='I1'"
        ).fetchone()
    assert row == ('SI-1', 'C9', 1500.0, 'qbo_origin')


def test_pull_je_with_missing_account_skips_gl_row(tmp_path):
    """If a JE line references an unknown account, we skip the GL row
    rather than crashing — the JE header is still stored for later
    reconciliation."""
    fake = FakeQBO()
    _seed_accounts(fake)
    fake.add_query('SELECT * FROM JournalEntry', [
        _je('JE-Q', lines=[(10.0, 'Debit', '999'), (10.0, 'Credit', '1')]),
    ])
    pull, db = _mk_pull(tmp_path, fake)
    pull.pull_accounts()
    pull.pull_journal_entries()
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT side, account_code FROM gl_transactions "
            "WHERE entry_id='QBO:JE-Q'"
        ).fetchall()
    # Only the credit side (acct 1 → 1000) mirrored. Debit (acct 999) skipped.
    assert rows == [('credit', '1000')]

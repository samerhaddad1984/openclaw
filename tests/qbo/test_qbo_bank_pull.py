"""Phase 2 — QBO bank pull (Purchases, Deposits, Transfers, Checks)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.bank_source_schema import ensure_bank_source_schema  # noqa: E402
from src.integrations.qbo_bank_pull import QBOBankPull  # noqa: E402
from tests.qbo._stubs import FakeQBO, make_db, patch_request_with_fake  # noqa: E402


def _mk(tmp_path, fake=None):
    db = make_db(tmp_path)
    ensure_bank_source_schema(db)
    pull = QBOBankPull('F1', 'C1', db_path=db, sandbox=True)
    if fake is not None:
        patch_request_with_fake(pull, fake)
    return pull, db


def _acct(qid, name='Checking', active=True):
    return {'Id': qid, 'Name': name, 'AccountType': 'Bank',
            'Active': active, 'SyncToken': '0',
            'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}}


# --- detection ---

def test_detect_bank_accounts_empty(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [])
    pull, db = _mk(tmp_path, fake)
    assert pull.detect_bank_accounts() == []


def test_detect_bank_accounts_returns_active(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1', 'Chequing'), _acct('2', 'Savings')])
    pull, db = _mk(tmp_path, fake)
    got = pull.detect_bank_accounts()
    assert [a['Id'] for a in got] == ['1', '2']


def test_has_bank_feeds_true_when_purchase_exists(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT Id FROM Purchase WHERE AccountRef = '1'",
                    [{'Id': 'P1'}])
    pull, db = _mk(tmp_path, fake)
    assert pull.has_bank_feeds() is True


def test_has_bank_feeds_false_when_no_transactions(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT Id FROM Purchase WHERE AccountRef = '1'", [])
    pull, db = _mk(tmp_path, fake)
    assert pull.has_bank_feeds() is False


def test_has_bank_feeds_false_when_no_bank_accounts(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [])
    pull, db = _mk(tmp_path, fake)
    assert pull.has_bank_feeds() is False


# --- pull purchases ---

def test_pull_purchases_stores_as_negative(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1', 'Chequing')])
    fake.add_query("SELECT * FROM Purchase WHERE AccountRef = '1'", [
        {'Id': 'P1', 'TxnDate': '2026-04-10', 'TotalAmt': 50.00,
         'EntityRef': {'name': 'Acme', 'value': 'V1'},
         'SyncToken': '0'},
    ])
    fake.add_query("SELECT * FROM Deposit WHERE DepositToAccountRef = '1'", [])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [])
    fake.add_query("SELECT * FROM Check WHERE AccountRef = '1'", [])
    pull, db = _mk(tmp_path, fake)
    n = pull.pull_bank_transactions()
    assert n == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT amount, merchant_name, source, external_id, category "
            "FROM bank_transactions"
        ).fetchone()
    assert row == (-50.0, 'Acme', 'qbo', 'P1', 'Purchase')


def test_pull_deposits_stores_positive(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT * FROM Purchase WHERE AccountRef = '1'", [])
    fake.add_query("SELECT * FROM Deposit WHERE DepositToAccountRef = '1'", [
        {'Id': 'D1', 'TxnDate': '2026-04-11', 'TotalAmt': 200.00,
         'SyncToken': '0',
         'Line': [{'Description': 'customer Atlas'}]},
    ])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [])
    fake.add_query("SELECT * FROM Check WHERE AccountRef = '1'", [])
    pull, db = _mk(tmp_path, fake)
    n = pull.pull_bank_transactions()
    assert n == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT amount, description, category FROM bank_transactions"
        ).fetchone()
    assert row == (200.0, 'customer Atlas', 'Deposit')


def test_pull_transfer_creates_two_entries(tmp_path):
    """A transfer with both From=1 and To=1 in the account listing
    creates OUT from 1; the dest account does its own pull for IN."""
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1', 'Chequing'), _acct('2', 'Savings')])
    # Purchases, deposits, checks empty for both accounts
    for a in ('1', '2'):
        fake.add_query(f"SELECT * FROM Purchase WHERE AccountRef = '{a}'", [])
        fake.add_query(f"SELECT * FROM Deposit WHERE DepositToAccountRef = '{a}'", [])
        fake.add_query(f"SELECT * FROM Check WHERE AccountRef = '{a}'", [])
    transfer = {
        'Id': 'T1', 'TxnDate': '2026-04-12', 'Amount': 100.00,
        'FromAccountRef': {'value': '1', 'name': 'Chequing'},
        'ToAccountRef': {'value': '2', 'name': 'Savings'},
        'SyncToken': '0',
    }
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [transfer])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '2' OR ToAccountRef = '2')",
                    [transfer])
    pull, db = _mk(tmp_path, fake)
    n = pull.pull_bank_transactions()
    # Expect 2 rows: out from 1, in to 2.
    assert n == 2
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT amount, description, external_id FROM bank_transactions "
            "ORDER BY external_id"
        ).fetchall()
    assert rows == [
        (100.0, 'Transfer from Chequing', 'T1:in'),
        (-100.0, 'Transfer to Savings', 'T1:out'),
    ]


def test_pull_checks_stores_negative(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT * FROM Purchase WHERE AccountRef = '1'", [])
    fake.add_query("SELECT * FROM Deposit WHERE DepositToAccountRef = '1'", [])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [])
    fake.add_query("SELECT * FROM Check WHERE AccountRef = '1'", [
        {'Id': 'C1', 'TxnDate': '2026-04-13', 'TotalAmt': 75.25,
         'DocNumber': '1041',
         'EntityRef': {'name': 'Utility Co', 'value': 'V5'},
         'SyncToken': '0'},
    ])
    pull, db = _mk(tmp_path, fake)
    n = pull.pull_bank_transactions()
    assert n == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT amount, description, category FROM bank_transactions"
        ).fetchone()
    assert row == (-75.25, 'Utility Co', 'Check')


# --- incremental / upsert semantics ---

def test_incremental_uses_since_date_in_query(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query(
        "SELECT * FROM Purchase WHERE AccountRef = '1' AND "
        "MetaData.LastUpdatedTime >= '2026-04-15T00:00:00Z'",
        [{'Id': 'P9', 'TxnDate': '2026-04-16', 'TotalAmt': 12.0,
          'SyncToken': '0'}],
    )
    fake.add_query(
        "SELECT * FROM Deposit WHERE DepositToAccountRef = '1' AND "
        "MetaData.LastUpdatedTime >= '2026-04-15T00:00:00Z'", [])
    fake.add_query(
        "SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1') "
        "AND MetaData.LastUpdatedTime >= '2026-04-15T00:00:00Z'", [])
    fake.add_query(
        "SELECT * FROM Check WHERE AccountRef = '1' AND "
        "MetaData.LastUpdatedTime >= '2026-04-15T00:00:00Z'", [])
    pull, db = _mk(tmp_path, fake)
    n = pull.pull_bank_transactions(since_date='2026-04-15T00:00:00Z')
    assert n == 1


def test_upsert_updates_when_sync_token_changes(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT * FROM Purchase WHERE AccountRef = '1'", [
        {'Id': 'P1', 'TxnDate': '2026-04-10', 'TotalAmt': 50.0,
         'SyncToken': '0'},
    ])
    fake.add_query("SELECT * FROM Deposit WHERE DepositToAccountRef = '1'", [])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [])
    fake.add_query("SELECT * FROM Check WHERE AccountRef = '1'", [])
    pull, db = _mk(tmp_path, fake)
    pull.pull_bank_transactions()

    # Bump the SyncToken + change the amount
    fake._queries["SELECT * FROM PURCHASE WHERE ACCOUNTREF = '1'"] = [
        {'Id': 'P1', 'TxnDate': '2026-04-10', 'TotalAmt': 75.0,
         'SyncToken': '1'},
    ]
    pull.pull_bank_transactions()
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT amount, qbo_sync_token FROM bank_transactions "
            "WHERE external_id='P1'"
        ).fetchall()
    assert rows == [(-75.0, '1')]


def test_upsert_noop_when_sync_token_unchanged(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT * FROM Purchase WHERE AccountRef = '1'", [
        {'Id': 'P1', 'TxnDate': '2026-04-10', 'TotalAmt': 50.0,
         'SyncToken': '0'},
    ])
    fake.add_query("SELECT * FROM Deposit WHERE DepositToAccountRef = '1'", [])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [])
    fake.add_query("SELECT * FROM Check WHERE AccountRef = '1'", [])
    pull, db = _mk(tmp_path, fake)
    pull.pull_bank_transactions()

    # Simulate a spec quirk: server returns TotalAmt=999 but same SyncToken.
    # We must NOT overwrite.
    fake._queries["SELECT * FROM PURCHASE WHERE ACCOUNTREF = '1'"] = [
        {'Id': 'P1', 'TxnDate': '2026-04-10', 'TotalAmt': 999.0,
         'SyncToken': '0'},
    ]
    pull.pull_bank_transactions()
    with sqlite3.connect(db) as conn:
        amount = conn.execute(
            "SELECT amount FROM bank_transactions WHERE external_id='P1'"
        ).fetchone()[0]
    assert amount == -50.0  # original, unchanged


def test_idempotent_multi_run(tmp_path):
    fake = FakeQBO()
    fake.add_query("SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true",
                    [_acct('1')])
    fake.add_query("SELECT * FROM Purchase WHERE AccountRef = '1'", [
        {'Id': f'P{i}', 'TxnDate': '2026-04-10', 'TotalAmt': float(i),
         'SyncToken': '0'} for i in range(1, 6)
    ])
    fake.add_query("SELECT * FROM Deposit WHERE DepositToAccountRef = '1'", [])
    fake.add_query("SELECT * FROM Transfer WHERE (FromAccountRef = '1' OR ToAccountRef = '1')",
                    [])
    fake.add_query("SELECT * FROM Check WHERE AccountRef = '1'", [])
    pull, db = _mk(tmp_path, fake)
    for _ in range(3):
        pull.pull_bank_transactions()
    with sqlite3.connect(db) as conn:
        n = conn.execute("SELECT COUNT(*) FROM bank_transactions").fetchone()[0]
    assert n == 5

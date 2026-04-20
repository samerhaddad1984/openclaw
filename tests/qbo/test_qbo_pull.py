"""Phase 2 tests — QBO pull for Account / Customer / Vendor."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_pull import QBOPull, QBOAuthError  # noqa: E402
from tests.qbo._stubs import FakeQBO, make_db, patch_request_with_fake  # noqa: E402


def _mk_pull(tmp_path, firm='F1', client='C1', fake=None):
    db = make_db(tmp_path, firm=firm, client=client)
    pull = QBOPull(firm, client, db_path=db, sandbox=True)
    if fake is not None:
        patch_request_with_fake(pull, fake)
    return pull, db


# --- accounts ---

def test_pull_accounts_inserts_new(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'AcctNum': '1000', 'Active': True,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
        {'Id': '2', 'Name': 'AR', 'AccountType': 'Accounts Receivable',
         'AcctNum': '1200', 'Active': True,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    n = pull.pull_accounts()
    assert n == 2
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT qbo_id, name, account_type, account_number "
            "FROM qbo_accounts ORDER BY qbo_id"
        ).fetchall()
    assert rows == [('1', 'Cash', 'Bank', '1000'),
                    ('2', 'AR', 'Accounts Receivable', '1200')]


def test_pull_accounts_updates_existing_when_newer(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    pull.pull_accounts()

    # Second run with newer LastUpdatedTime and a different name.
    fake._queries.clear()
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Petty Cash', 'AccountType': 'Bank',
         'SyncToken': '1',
         'MetaData': {'LastUpdatedTime': '2026-04-21T10:00:00Z'}},
    ])
    pull.pull_accounts()
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT name FROM qbo_accounts WHERE qbo_id='1'"
        ).fetchone()
        token = conn.execute(
            "SELECT qbo_sync_token FROM qbo_sync_state "
            "WHERE entity_type='Account' AND qbo_id='1'"
        ).fetchone()
    assert row[0] == 'Petty Cash'
    assert token[0] == '1'


def test_pull_accounts_skips_when_unchanged(tmp_path):
    fake = FakeQBO()
    row = {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
           'SyncToken': '0',
           'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}}
    fake.add_query('SELECT * FROM Account', [row])
    pull, db = _mk_pull(tmp_path, fake=fake)
    pull.pull_accounts()

    with sqlite3.connect(db) as conn:
        before = conn.execute(
            "SELECT last_synced FROM qbo_accounts WHERE qbo_id='1'"
        ).fetchone()

    # Re-run with same LastUpdatedTime → skip write.
    import time; time.sleep(0.01)
    # Fake a name change — if we correctly skip, name stays 'Cash'.
    fake._queries.clear()
    fake.add_query('SELECT * FROM Account', [
        {**row, 'Name': 'WOULD_HAVE_CHANGED'}
    ])
    pull.pull_accounts()
    with sqlite3.connect(db) as conn:
        after = conn.execute(
            "SELECT name FROM qbo_accounts WHERE qbo_id='1'"
        ).fetchone()
    assert after[0] == 'Cash'  # skipped


def test_pull_accounts_pagination(tmp_path):
    fake = FakeQBO()
    items = [
        {'Id': str(i), 'Name': f'Acct{i}', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}}
        for i in range(1, 2501)
    ]
    fake.add_query('SELECT * FROM Account', items)
    pull, db = _mk_pull(tmp_path, fake=fake)
    # Shrink page size so we actually paginate.
    pull._query_page_size = 1000  # documentation hint
    # pull_accounts calls _query which uses 1000 by default
    n = pull.pull_accounts()
    assert n == 2500
    with sqlite3.connect(db) as conn:
        ct = conn.execute("SELECT COUNT(*) FROM qbo_accounts").fetchone()[0]
    assert ct == 2500


# --- customers ---

def test_pull_customers_inserts(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Customer', [
        {'Id': '10', 'DisplayName': 'Acme Co',
         'CompanyName': 'Acme Corporation',
         'PrimaryEmailAddr': {'Address': 'ap@acme.com'},
         'PrimaryPhone': {'FreeFormNumber': '555-1111'},
         'BillAddr': {'Line1': '1 Main St'},
         'Balance': 1234.5, 'Active': True,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    n = pull.pull_customers()
    assert n == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT display_name, email, phone, balance FROM qbo_customers "
            "WHERE qbo_id='10'"
        ).fetchone()
    assert row == ('Acme Co', 'ap@acme.com', '555-1111', 1234.5)


# --- vendors ---

def test_pull_vendors_handles_inactive(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Vendor', [
        {'Id': '20', 'DisplayName': 'Bell Canada', 'Active': True,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
        {'Id': '21', 'DisplayName': 'Old Vendor', 'Active': False,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    n = pull.pull_vendors()
    assert n == 2
    with sqlite3.connect(db) as conn:
        actives = conn.execute(
            "SELECT qbo_id, active FROM qbo_vendors ORDER BY qbo_id"
        ).fetchall()
    assert dict(actives) == {'20': 1, '21': 0}


# --- rate limiting / auth ---

def test_pull_rate_limited_retries(tmp_path):
    fake = FakeQBO()
    fake.raise_429_once = True
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    n = pull.pull_accounts()
    assert n == 1
    assert fake.raise_429_once is False  # consumed


def test_pull_token_expired_triggers_refresh(tmp_path, monkeypatch):
    fake = FakeQBO()
    fake.raise_401_once = True
    fake.add_query('SELECT * FROM Account', [])

    pull, db = _mk_pull(tmp_path, fake=fake)

    refresh_called = {'n': 0}
    from src.integrations import qbo_pull as _mod

    def fake_refresh(firm, client, db_path):
        refresh_called['n'] += 1
        return {'access_token': 'new_at', 'realm_id': 'realm-1',
                'status': 'active', 'expires_at': 9999999999}
    monkeypatch.setattr(_mod, 'refresh_access_token', fake_refresh)

    n = pull.pull_accounts()
    assert refresh_called['n'] >= 1
    assert n == 0


def test_pull_auth_missing_raises(tmp_path):
    # Make a DB with NO connection.
    from src.agents.tools.qbo_oauth import _ensure_table
    from src.integrations.qbo_schema import ensure_qbo_sync_schema
    db = tmp_path / 'q.db'
    _ensure_table(db)
    ensure_qbo_sync_schema(db)
    with pytest.raises(QBOAuthError):
        QBOPull('F1', 'C1', db_path=db)


# --- sync state + idempotence + scoping ---

def test_sync_state_populated_on_pull(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'SyncToken': '5',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    pull.pull_accounts()
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT entity_type, qbo_id, qbo_sync_token, sync_status, sync_source "
            "FROM qbo_sync_state WHERE qbo_id='1'"
        ).fetchone()
    assert row == ('Account', '1', '5', 'synced', 'qbo_origin')


def test_pull_firm_scoped(tmp_path):
    """Two firms with the same DB should not see each other's data."""
    fake_f1 = FakeQBO()
    fake_f1.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'F1 Cash', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull1, db = _mk_pull(tmp_path, firm='F1', client='C', fake=fake_f1)
    pull1.pull_accounts()

    # Add a second connection for F2/C on the same DB.
    from src.agents.tools.qbo_oauth import store_qbo_tokens
    store_qbo_tokens(firm_code='F2', client_code='C', realm_id='realm-2',
                     access_token='at2', refresh_token='rt2',
                     expires_in=3600, db_path=db)
    fake_f2 = FakeQBO()
    fake_f2.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'F2 Cash', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull2 = QBOPull('F2', 'C', db_path=db, sandbox=True)
    patch_request_with_fake(pull2, fake_f2)
    pull2.pull_accounts()

    with sqlite3.connect(db) as conn:
        names = dict(conn.execute(
            "SELECT firm_code, name FROM qbo_accounts WHERE qbo_id='1'"
        ).fetchall())
    assert names == {'F1': 'F1 Cash', 'F2': 'F2 Cash'}


def test_pull_idempotent_multiple_runs(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
        {'Id': '2', 'Name': 'AR', 'AccountType': 'Accounts Receivable',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    for _ in range(3):
        pull.pull_accounts()
    with sqlite3.connect(db) as conn:
        ct = conn.execute("SELECT COUNT(*) FROM qbo_accounts").fetchone()[0]
    assert ct == 2  # no duplicates


def test_pull_all_three_entities(tmp_path):
    fake = FakeQBO()
    fake.add_query('SELECT * FROM Account', [
        {'Id': '1', 'Name': 'Cash', 'AccountType': 'Bank',
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    fake.add_query('SELECT * FROM Customer', [
        {'Id': '10', 'DisplayName': 'Acme', 'Active': True,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    fake.add_query('SELECT * FROM Vendor', [
        {'Id': '20', 'DisplayName': 'Bell', 'Active': True,
         'SyncToken': '0',
         'MetaData': {'LastUpdatedTime': '2026-04-20T10:00:00Z'}},
    ])
    pull, db = _mk_pull(tmp_path, fake=fake)
    assert pull.pull_accounts() == 1
    assert pull.pull_customers() == 1
    assert pull.pull_vendors() == 1

    with sqlite3.connect(db) as conn:
        counts = {
            'accounts':  conn.execute("SELECT COUNT(*) FROM qbo_accounts").fetchone()[0],
            'customers': conn.execute("SELECT COUNT(*) FROM qbo_customers").fetchone()[0],
            'vendors':   conn.execute("SELECT COUNT(*) FROM qbo_vendors").fetchone()[0],
            'state':     conn.execute("SELECT COUNT(*) FROM qbo_sync_state").fetchone()[0],
        }
    assert counts == {'accounts': 1, 'customers': 1, 'vendors': 1, 'state': 3}

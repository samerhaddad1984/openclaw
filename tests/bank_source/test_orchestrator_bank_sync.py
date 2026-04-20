"""Phase 6 — QBOSyncOrchestrator.incremental_sync pulls bank txs per
clients.bank_source, and runs dedup when 'both' is active."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.qbo_oauth import _ensure_table, store_qbo_tokens  # noqa: E402
from src.integrations.bank_source_schema import ensure_bank_source_schema  # noqa: E402
from src.integrations.qbo_schema import ensure_qbo_sync_schema  # noqa: E402
from src.integrations.qbo_sync import QBOSyncOrchestrator  # noqa: E402


class StubPuller:
    def __init__(self, firm, client, db_path, sandbox=False):
        self.firm = firm; self.client = client; self.db_path = db_path
    def pull_accounts(self): return 0
    def pull_customers(self): return 0
    def pull_vendors(self): return 0
    def pull_journal_entries(self, since_date=None): return 0
    def pull_bills(self, since_date=None): return 0
    def pull_invoices(self, since_date=None): return 0
    def pull_payments(self, since_date=None): return 0


_NOOP_WP = (lambda _db: [], lambda *a, **k: None)


def _mk_db(tmp_path, *, bank_source='none', firm='F1', client='C1',
            plaid_active=False):
    db = tmp_path / 'o.db'
    _ensure_table(db)
    ensure_qbo_sync_schema(db)
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS clients "
            "(client_code TEXT PRIMARY KEY, firm_code TEXT, "
            " bank_source TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bank_connections "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, client_code TEXT, "
            " active INTEGER DEFAULT 1)"
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, bank_source) "
            "VALUES (?,?,?)",
            (client, firm, bank_source),
        )
        if plaid_active:
            conn.execute(
                "INSERT INTO bank_connections (client_code, active) VALUES (?,?)",
                (client, 1),
            )
        conn.commit()
    store_qbo_tokens(firm_code=firm, client_code=client,
                       realm_id='R1', access_token='at',
                       refresh_token='rt', expires_in=3600, db_path=db)
    return db


def test_bank_pull_skipped_when_bank_source_none(tmp_path, monkeypatch):
    db = _mk_db(tmp_path, bank_source='none')
    calls = {'count': 0}

    class StubBankPull:
        def __init__(self, *a, **k): pass
        def pull_bank_transactions(self, since_date=None):
            calls['count'] += 1
            return 5

    import src.integrations.qbo_bank_pull as _qbp
    monkeypatch.setattr(_qbp, 'QBOBankPull', StubBankPull)

    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                 puller_cls=StubPuller,
                                 webhook_processor=_NOOP_WP)
    out = orch.incremental_sync()
    assert 'bank_transactions' not in out
    assert calls['count'] == 0


def test_bank_pull_runs_when_bank_source_qbo(tmp_path, monkeypatch):
    db = _mk_db(tmp_path, bank_source='qbo')

    class StubBankPull:
        def __init__(self, *a, **k): pass
        def pull_bank_transactions(self, since_date=None):
            return 3

    import src.integrations.qbo_bank_pull as _qbp
    monkeypatch.setattr(_qbp, 'QBOBankPull', StubBankPull)

    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                 puller_cls=StubPuller,
                                 webhook_processor=_NOOP_WP)
    out = orch.incremental_sync()
    assert out['bank_transactions'] == 3


def test_both_source_runs_pull_and_dedup(tmp_path, monkeypatch):
    db = _mk_db(tmp_path, bank_source='both', plaid_active=True)

    class StubBankPull:
        def __init__(self, firm, client, db_path, sandbox=False):
            self.firm = firm; self.client = client; self.db_path = db_path
        def pull_bank_transactions(self, since_date=None):
            # Seed an intentionally-duplicate pair so dedup has work.
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO bank_transactions "
                    "(id, firm_code, client_code, source, external_id, "
                    " date, amount, description) VALUES "
                    "(?,?,?,?,?,?,?,?)",
                    ('qbo-1', self.firm, self.client, 'qbo', 'Q1',
                     '2026-04-20', -50.0, 'Acme'),
                )
                conn.execute(
                    "INSERT INTO bank_transactions "
                    "(id, firm_code, client_code, source, external_id, "
                    " date, amount, description) VALUES "
                    "(?,?,?,?,?,?,?,?)",
                    ('plaid-1', self.firm, self.client, 'plaid', 'P1',
                     '2026-04-20', -50.0, 'ACME'),
                )
                conn.commit()
            return 2

    import src.integrations.qbo_bank_pull as _qbp
    monkeypatch.setattr(_qbp, 'QBOBankPull', StubBankPull)

    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                 puller_cls=StubPuller,
                                 webhook_processor=_NOOP_WP)
    out = orch.incremental_sync()
    assert out['bank_transactions'] == 2
    assert out.get('duplicates_hidden', 0) == 1
    with sqlite3.connect(db) as conn:
        hid = conn.execute(
            "SELECT hidden_duplicate FROM bank_transactions WHERE id='plaid-1'"
        ).fetchone()[0]
    assert hid == 1


def test_bank_pull_tolerates_missing_clients_table(tmp_path, monkeypatch):
    """Test DBs without the clients table must not break incremental sync."""
    # Make a minimal DB without the clients table.
    db = tmp_path / 'minimal.db'
    _ensure_table(db)
    ensure_qbo_sync_schema(db)
    ensure_bank_source_schema(db)
    # No INSERT into clients (table absent or empty).
    store_qbo_tokens(firm_code='F1', client_code='C1',
                       realm_id='R1', access_token='at',
                       refresh_token='rt', expires_in=3600, db_path=db)

    class BoomBankPull:
        def __init__(self, *a, **k): pass
        def pull_bank_transactions(self, since_date=None):
            raise AssertionError("should not be called when bank_source=none")

    import src.integrations.qbo_bank_pull as _qbp
    monkeypatch.setattr(_qbp, 'QBOBankPull', BoomBankPull)

    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                 puller_cls=StubPuller,
                                 webhook_processor=_NOOP_WP)
    out = orch.incremental_sync()
    assert 'bank_transactions' not in out
    assert out['ok'] is True


def test_both_source_without_plaid_still_pulls(tmp_path, monkeypatch):
    """bank_source='both' but Plaid rows absent -> pull runs, dedup
    finds no matches -> no crash."""
    db = _mk_db(tmp_path, bank_source='both', plaid_active=False)

    class StubBankPull:
        def __init__(self, *a, **k): pass
        def pull_bank_transactions(self, since_date=None):
            return 0

    import src.integrations.qbo_bank_pull as _qbp
    monkeypatch.setattr(_qbp, 'QBOBankPull', StubBankPull)

    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                 puller_cls=StubPuller,
                                 webhook_processor=_NOOP_WP)
    out = orch.incremental_sync()
    assert out['bank_transactions'] == 0
    assert out.get('duplicates_hidden', 0) == 0

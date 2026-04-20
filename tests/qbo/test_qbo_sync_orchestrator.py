"""Phase 7 — QBOSyncOrchestrator initial + incremental + scheduled."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_sync import (  # noqa: E402
    QBOSyncOrchestrator,
    scheduled_sync_all,
    sync_status,
)
from tests.qbo._stubs import make_db  # noqa: E402


class StubPuller:
    """Enough surface to satisfy the orchestrator without real QBO."""

    def __init__(self, firm, client, db_path, sandbox=False):
        self.firm = firm; self.client = client; self.db_path = db_path
        self.sandbox = sandbox
        self.pulls: list[str] = []
        self.since: dict[str, str | None] = {}

    def pull_accounts(self):    self.pulls.append('accounts');    return 3
    def pull_customers(self):   self.pulls.append('customers');   return 2
    def pull_vendors(self):     self.pulls.append('vendors');     return 4
    def pull_journal_entries(self, since_date=None):
        self.since['je'] = since_date
        self.pulls.append('je');   return 5
    def pull_bills(self, since_date=None):
        self.since['bills'] = since_date
        self.pulls.append('bills');    return 1
    def pull_invoices(self, since_date=None):
        self.since['inv'] = since_date
        self.pulls.append('invoices'); return 2
    def pull_payments(self, since_date=None): return 0


def _fake_webhook_processor():
    calls: list[dict] = []
    def pending(db):
        return []
    def process(db, ev, puller_cls=None):
        calls.append(ev)
    return (pending, process), calls


def test_initial_sync_calls_every_puller(tmp_path):
    db = make_db(tmp_path)
    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=StubPuller)
    out = orch.initial_sync()
    assert out['ok'] is True
    # Order matters: refs before transactions
    # Spy by reconstructing from stub not exposed — check counts instead:
    assert out['accounts'] == 3
    assert out['customers'] == 2
    assert out['vendors'] == 4
    assert out['journal_entries'] == 5
    assert out['bills'] == 1
    assert out['invoices'] == 2


def test_initial_sync_writes_log_row(tmp_path):
    db = make_db(tmp_path)
    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=StubPuller)
    orch.initial_sync(triggered_by='manual')
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT direction, entities_synced, errors, triggered_by "
            "FROM qbo_sync_log WHERE firm_code='F1'"
        ).fetchone()
    assert row == ('full_sync', 17, 0, 'manual')


def test_initial_sync_records_error_and_marks_log(tmp_path):
    class BoomPuller(StubPuller):
        def pull_accounts(self):
            raise RuntimeError('no realm')
    db = make_db(tmp_path)
    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=BoomPuller)
    out = orch.initial_sync()
    assert out['ok'] is False
    with sqlite3.connect(db) as conn:
        errs = conn.execute(
            "SELECT errors FROM qbo_sync_log WHERE firm_code='F1'"
        ).fetchone()[0]
    assert errs == 1


def test_incremental_sync_passes_since_to_puller(tmp_path):
    db = make_db(tmp_path)
    # Prior successful sync timestamp:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_log (firm_code, client_code, started_at, "
            "completed_at, direction, entities_synced, errors) "
            "VALUES (?,?,?,?,?,?,?)",
            ('F1', 'C1', '2026-04-19T00:00:00Z',
             '2026-04-19T00:01:00Z', 'full_sync', 10, 0),
        )
        conn.commit()

    captured: dict = {}

    class SpyPuller(StubPuller):
        def pull_journal_entries(self, since_date=None):
            captured['je_since'] = since_date
            return 1

    (wp, log) = _fake_webhook_processor()
    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=SpyPuller,
                                webhook_processor=wp)
    out = orch.incremental_sync()
    assert captured['je_since'] == '2026-04-19T00:01:00Z'
    assert out['ok'] is True


def test_incremental_sync_defaults_when_no_prior_run(tmp_path):
    db = make_db(tmp_path)
    captured: dict = {}

    class SpyPuller(StubPuller):
        def pull_journal_entries(self, since_date=None):
            captured['je_since'] = since_date
            return 0

    (wp, _) = _fake_webhook_processor()
    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=SpyPuller,
                                webhook_processor=wp)
    orch.incremental_sync(window_days=30)
    assert captured['je_since'].startswith('20')  # ISO
    assert 'T' in captured['je_since']


def test_scheduled_sync_all_iterates_every_active_connection(tmp_path):
    db = make_db(tmp_path, firm='F1', client='C1')
    # Add a second active connection
    from src.agents.tools.qbo_oauth import store_qbo_tokens
    store_qbo_tokens(firm_code='F1', client_code='C2', realm_id='realm-2',
                     access_token='at2', refresh_token='rt2',
                     expires_in=3600, db_path=db)

    calls: list[tuple[str, str]] = []

    class OrchStub:
        def __init__(self, firm, client, db_path, sandbox=False):
            self.firm = firm; self.client = client
        def incremental_sync(self, triggered_by='manual'):
            calls.append((self.firm, self.client))
            return {'ok': True}

    out = scheduled_sync_all(db, orchestrator_cls=OrchStub)
    assert out['connections'] == 2
    assert sorted(calls) == [('F1', 'C1'), ('F1', 'C2')]


def test_scheduled_sync_all_tolerates_per_client_failure(tmp_path):
    db = make_db(tmp_path)

    class OrchStub:
        def __init__(self, firm, client, db_path, sandbox=False):
            pass
        def incremental_sync(self, triggered_by='manual'):
            raise RuntimeError('boom')

    out = scheduled_sync_all(db, orchestrator_cls=OrchStub)
    assert out['connections'] == 1
    [(_key, v)] = out['results'].items()
    assert v['ok'] is False
    assert 'boom' in v['error']


def test_sync_status_rolls_up_state(tmp_path):
    db = make_db(tmp_path)
    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=StubPuller)
    orch.initial_sync()
    # Seed a conflict
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state (firm_code, client_code, "
            "entity_type, qbo_id, sync_status) VALUES (?,?,?,?,?)",
            ('F1', 'C1', 'Bill', 'B1', 'conflict'),
        )
        conn.execute(
            "INSERT INTO qbo_webhook_events (event_id, processed) VALUES (?,?)",
            ('ev-pending', 0),
        )
        conn.commit()

    status = sync_status(db, firm_code='F1', client_code='C1')
    assert status['conflicts_pending'] == 1
    assert status['webhooks_pending'] == 1
    assert status['last_successful_sync']['direction'] == 'full_sync'
    assert status['last_successful_sync']['entities_synced'] == 17


def test_incremental_sync_drains_webhook_queue(tmp_path):
    db = make_db(tmp_path)
    drained: list = []

    def pending(db):  # no real events
        drained.append('called')
        return []
    def process(db, ev, puller_cls=None):
        drained.append(ev)

    orch = QBOSyncOrchestrator('F1', 'C1', db_path=db,
                                puller_cls=StubPuller,
                                webhook_processor=(pending, process))
    out = orch.incremental_sync()
    assert 'called' in drained
    assert out.get('webhook_events') == 0

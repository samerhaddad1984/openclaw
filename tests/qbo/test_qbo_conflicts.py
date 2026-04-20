"""Phase 5 — conflict detection and resolution strategies."""
from __future__ import annotations

import sqlite3
import sys
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_conflict_resolver import (  # noqa: E402
    QBOConflictResolver,
    detect_conflicts,
    mark_local_modified,
)
from tests.qbo._stubs import FakeQBO, make_db, patch_request_with_fake  # noqa: E402


def _seed(db, row: dict):
    with sqlite3.connect(db) as conn:
        keys = ','.join(row.keys())
        placeholders = ','.join('?' * len(row))
        conn.execute(
            f"INSERT INTO qbo_sync_state ({keys}) VALUES ({placeholders})",
            tuple(row.values()),
        )
        conn.commit()


def test_detect_conflicts_returns_empty_when_nothing_conflicts(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q1',
        'last_pushed_at': '2026-04-19T00:00:00Z',
        'last_qbo_modified': '2026-04-19T00:00:00Z',
        'last_local_modified': '2026-04-19T00:00:00Z',
        'sync_status': 'synced',
    })
    assert detect_conflicts(db, firm_code='F1', client_code='C1') == []


def test_detect_conflicts_flags_both_sides_modified(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q2',
        'last_pushed_at': '2026-04-19T00:00:00Z',
        'last_qbo_modified': '2026-04-20T00:00:00Z',
        'last_local_modified': '2026-04-20T00:00:00Z',
        'sync_status': 'synced',
    })
    conflicts = detect_conflicts(db, firm_code='F1', client_code='C1')
    assert len(conflicts) == 1
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT sync_status FROM qbo_sync_state WHERE qbo_id='Q2'"
        ).fetchone()[0]
    assert status == 'conflict'


def test_detect_conflicts_skips_when_only_one_side_moved(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q3',
        'last_pushed_at': '2026-04-19T00:00:00Z',
        'last_qbo_modified': '2026-04-20T00:00:00Z',  # QBO changed
        'last_local_modified': '2026-04-19T00:00:00Z',  # local didn't
        'sync_status': 'synced',
    })
    assert detect_conflicts(db, firm_code='F1', client_code='C1') == []


def test_mark_local_modified_updates_timestamp(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q4',
        'local_id': 'je-4', 'sync_status': 'synced',
    })
    mark_local_modified(db, firm_code='F1', client_code='C1',
                         entity_type='JournalEntry', local_id='je-4')
    with sqlite3.connect(db) as conn:
        ts = conn.execute(
            "SELECT last_local_modified FROM qbo_sync_state WHERE qbo_id='Q4'"
        ).fetchone()[0]
    assert ts is not None


def test_list_conflicts_returns_only_conflict_rows(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q5',
        'sync_status': 'conflict',
    })
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'Bill', 'qbo_id': 'B1',
        'sync_status': 'synced',
    })
    resolver = QBOConflictResolver('F1', 'C1', db_path=db)
    conflicts = resolver.list_conflicts()
    assert len(conflicts) == 1
    assert conflicts[0]['qbo_id'] == 'Q5'


def test_resolve_flag_for_review_sets_details(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q6',
        'sync_status': 'conflict', 'local_id': 'je-6',
    })
    resolver = QBOConflictResolver('F1', 'C1', db_path=db)
    out = resolver.resolve(entity_type='JournalEntry', qbo_id='Q6',
                             strategy='flag_for_review')
    assert out['status'] == 'pending_review'
    with sqlite3.connect(db) as conn:
        details = conn.execute(
            "SELECT conflict_details FROM qbo_sync_state WHERE qbo_id='Q6'"
        ).fetchone()[0]
    parsed = json.loads(details)
    assert parsed['entity_type'] == 'JournalEntry'
    assert parsed['qbo_id'] == 'Q6'


def test_resolve_otocpa_wins_pushes_update(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q7',
        'sync_status': 'conflict', 'local_id': 'je-7',
        'qbo_sync_token': '3',
    })

    class FakePusher:
        def __init__(self):
            self.calls = []
        def push_journal_entry_update(self, local_id):
            self.calls.append(local_id)
            return {'status': 'updated', 'qbo_id': 'Q7'}

    pusher = FakePusher()
    resolver = QBOConflictResolver('F1', 'C1', db_path=db)
    out = resolver.resolve(entity_type='JournalEntry', qbo_id='Q7',
                             strategy='otocpa_wins', pusher=pusher)
    assert out['status'] == 'resolved_by_push'
    assert pusher.calls == ['je-7']
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT sync_status, conflict_details FROM qbo_sync_state "
            "WHERE qbo_id='Q7'"
        ).fetchone()
    assert status[0] == 'synced'
    assert status[1] is None


def test_resolve_qbo_wins_pulls_entity(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'Account', 'qbo_id': 'Q8',
        'sync_status': 'conflict',
    })

    class FakePuller:
        def __init__(self):
            self.calls = []
            self.firm_code = 'F1'
            self.client_code = 'C1'
        def _query(self, q, max_results=1):
            self.calls.append(q)
            return [{'Id': 'Q8', 'Name': 'PulledCash',
                      'SyncToken': '7',
                      'MetaData': {'LastUpdatedTime': '2026-04-21T00:00:00Z'}}]
        def _upsert_account(self, acct):
            assert acct['Id'] == 'Q8'
            self.upserted = acct

    puller = FakePuller()
    resolver = QBOConflictResolver('F1', 'C1', db_path=db)
    out = resolver.resolve(entity_type='Account', qbo_id='Q8',
                             strategy='qbo_wins', puller=puller)
    assert out['status'] == 'resolved_by_pull'
    assert puller.upserted['Name'] == 'PulledCash'
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT sync_status FROM qbo_sync_state WHERE qbo_id='Q8'"
        ).fetchone()[0]
    assert status == 'synced'


def test_resolve_unknown_strategy_raises(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q9',
        'sync_status': 'conflict',
    })
    resolver = QBOConflictResolver('F1', 'C1', db_path=db)
    with pytest.raises(ValueError):
        resolver.resolve(entity_type='JournalEntry', qbo_id='Q9',
                           strategy='coin_flip')


def test_resolve_merge_falls_back_to_review(tmp_path):
    db = make_db(tmp_path)
    _seed(db, {
        'firm_code': 'F1', 'client_code': 'C1',
        'entity_type': 'JournalEntry', 'qbo_id': 'Q10',
        'sync_status': 'conflict', 'local_id': 'je-10',
    })
    resolver = QBOConflictResolver('F1', 'C1', db_path=db)
    out = resolver.resolve(entity_type='JournalEntry', qbo_id='Q10',
                             strategy='merge')
    assert out['status'] == 'pending_review'

"""Phase 6 — QBO webhook verify + store + async processing."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_webhook import (  # noqa: E402
    handle_webhook,
    parse_webhook_body,
    pending_events,
    process_one_event,
    resolve_firm_client,
    store_webhook_events,
    verify_qbo_signature,
)
from tests.qbo._stubs import make_db  # noqa: E402


TOKEN = 'topsecret'


def _sign(body: bytes) -> str:
    mac = hmac.new(TOKEN.encode(), body, hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def _payload(entity: str = 'JournalEntry', op: str = 'Update',
              realm: str = 'realm-1', eid: str = '42',
              last: str = '2026-04-20T10:00:00Z') -> str:
    return json.dumps({
        'eventNotifications': [{
            'realmId': realm,
            'dataChangeEvent': {
                'entities': [{
                    'name': entity, 'id': eid,
                    'operation': op, 'lastUpdated': last,
                }]
            }
        }]
    })


def test_signature_accept_valid(tmp_path):
    body = b'{"ok":true}'
    sig = _sign(body)
    assert verify_qbo_signature(body, sig, TOKEN) is True


def test_signature_reject_invalid(tmp_path):
    body = b'{"ok":true}'
    assert verify_qbo_signature(body, 'wrong', TOKEN) is False
    assert verify_qbo_signature(body, '', TOKEN) is False


def test_signature_reject_missing_token(tmp_path):
    body = b'{"ok":true}'
    sig = _sign(body)
    assert verify_qbo_signature(body, sig, '') is False


def test_parse_body_flattens_entities(tmp_path):
    raw = json.dumps({
        'eventNotifications': [{
            'realmId': 'R1',
            'dataChangeEvent': {
                'entities': [
                    {'name': 'Account', 'id': '1',
                     'operation': 'Create', 'lastUpdated': '2026-01-01T00:00:00Z'},
                    {'name': 'JournalEntry', 'id': '42',
                     'operation': 'Update', 'lastUpdated': '2026-01-01T01:00:00Z'},
                ]
            }
        }]
    })
    events = parse_webhook_body(raw)
    assert len(events) == 2
    assert events[0]['entity_type'] == 'Account'
    assert events[1]['operation'] == 'Update'
    # Stable synthetic event_id
    assert events[1]['event_id'] == 'R1:JournalEntry:42:2026-01-01T01:00:00Z'


def test_store_webhook_events_idempotent(tmp_path):
    db = make_db(tmp_path)
    body = _payload()
    events = parse_webhook_body(body)
    assert store_webhook_events(db, events) == 1
    # Re-storing the same event set does nothing.
    assert store_webhook_events(db, events) == 0
    with sqlite3.connect(db) as conn:
        ct = conn.execute("SELECT COUNT(*) FROM qbo_webhook_events").fetchone()[0]
    assert ct == 1


def test_handle_webhook_happy_path(tmp_path):
    db = make_db(tmp_path)
    body = _payload()
    sig = _sign(body.encode())
    out = handle_webhook(body, sig, db_path=db, verifier_token=TOKEN)
    assert out == {'ok': True, 'events_received': 1,
                   'events_stored': 1, 'status': 200}


def test_handle_webhook_bad_signature_returns_401_never_raises(tmp_path):
    db = make_db(tmp_path)
    out = handle_webhook(_payload(), 'bogus',
                          db_path=db, verifier_token=TOKEN)
    assert out['ok'] is False
    assert out['status'] == 401


def test_handle_webhook_duplicate_is_noop(tmp_path):
    db = make_db(tmp_path)
    body = _payload()
    sig = _sign(body.encode())
    handle_webhook(body, sig, db_path=db, verifier_token=TOKEN)
    out2 = handle_webhook(body, sig, db_path=db, verifier_token=TOKEN)
    assert out2['events_stored'] == 0
    assert out2['status'] == 200


def test_pending_events_returns_unprocessed(tmp_path):
    db = make_db(tmp_path)
    body = _payload()
    store_webhook_events(db, parse_webhook_body(body))
    got = pending_events(db)
    assert len(got) == 1
    assert got[0]['processed'] == 0


def test_resolve_firm_client_from_realm(tmp_path):
    db = make_db(tmp_path, realm='my-realm', firm='F1', client='C1')
    firm, client = resolve_firm_client(db, 'my-realm')
    assert (firm, client) == ('F1', 'C1')
    firm2, client2 = resolve_firm_client(db, 'unknown')
    assert (firm2, client2) == (None, None)


def test_process_delete_event_marks_deleted(tmp_path):
    db = make_db(tmp_path, realm='R1')
    # Seed sync_state
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, sync_status) "
            "VALUES (?,?,?,?,?)",
            ('F1', 'C1', 'JournalEntry', '42', 'synced'),
        )
        conn.commit()
    store_webhook_events(db, parse_webhook_body(
        _payload(op='Delete', realm='R1', eid='42')
    ))
    ev = pending_events(db)[0]
    process_one_event(db, ev)
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT sync_status FROM qbo_sync_state WHERE qbo_id='42'"
        ).fetchone()[0]
    assert status == 'deleted'


def test_process_merge_event_flags_conflict(tmp_path):
    db = make_db(tmp_path, realm='R1')
    store_webhook_events(db, parse_webhook_body(
        _payload(op='Merge', realm='R1', eid='99', entity='Customer')
    ))
    ev = pending_events(db)[0]
    process_one_event(db, ev)
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT sync_status, conflict_details FROM qbo_sync_state "
            "WHERE qbo_id='99' AND entity_type='Customer'"
        ).fetchone()
    assert row[0] == 'conflict'
    assert 'qbo_merge_event' in (row[1] or '')


def test_process_update_event_pulls_via_puller(tmp_path):
    db = make_db(tmp_path, realm='R1')
    store_webhook_events(db, parse_webhook_body(
        _payload(op='Update', realm='R1', eid='7', entity='Account')
    ))

    class FakePullerCls:
        def __init__(self, firm, client, db_path):
            self.firm = firm; self.client = client; self.db = db_path
            self.query_sql = None
            self.upserted = None
            self.firm_code = firm; self.client_code = client
        def _query(self, q, max_results=1):
            self.query_sql = q
            return [{'Id': '7', 'Name': 'Pulled', 'AccountType': 'Bank',
                      'SyncToken': '0',
                      'MetaData': {'LastUpdatedTime': '2026-04-20T11:00:00Z'}}]
        def _upsert_account(self, acct):
            self.upserted = acct

    captured = {}

    def make(*args, **kwargs):
        inst = FakePullerCls(*args, **kwargs)
        captured['inst'] = inst
        return inst

    ev = pending_events(db)[0]
    process_one_event(db, ev, puller_cls=make)
    assert captured['inst'].upserted['Id'] == '7'
    # Processed flag set
    with sqlite3.connect(db) as conn:
        pr = conn.execute(
            "SELECT processed FROM qbo_webhook_events WHERE event_id=?",
            (ev['event_id'],),
        ).fetchone()[0]
    assert pr == 1


def test_process_event_records_error_on_exception(tmp_path):
    db = make_db(tmp_path, realm='R1')
    store_webhook_events(db, parse_webhook_body(
        _payload(op='Update', realm='R1', eid='1', entity='Account')
    ))

    class BoomPuller:
        def __init__(self, *a, **k):
            self.firm_code = 'F1'; self.client_code = 'C1'
        def _query(self, q, max_results=1):
            raise RuntimeError('simulated')

    ev = pending_events(db)[0]
    process_one_event(db, ev, puller_cls=BoomPuller)
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT processed, error FROM qbo_webhook_events WHERE event_id=?",
            (ev['event_id'],),
        ).fetchone()
    assert row[0] == 1
    assert 'simulated' in row[1]

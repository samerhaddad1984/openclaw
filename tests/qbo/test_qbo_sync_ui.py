"""Phase 9 — pure render helpers + HTTP handlers for the QBO sync UI."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_sync_ui import (  # noqa: E402
    handle_incremental_sync,
    handle_initial_sync,
    handle_resolve_conflict,
    handle_sync_status_api,
    handle_webhook_route,
    render_conflicts_page,
    render_sync_dashboard,
)
from tests.qbo._stubs import make_db  # noqa: E402


def test_dashboard_renders_empty_state(tmp_path):
    db = make_db(tmp_path)
    html = render_sync_dashboard(db, firm_code='F1', client_code='C1')
    assert 'QBO sync' in html
    assert 'F1' in html and 'C1' in html
    assert 'never' in html  # no prior sync
    assert 'Run initial sync' in html
    assert 'Run incremental sync' in html


def test_dashboard_renders_completed_sync(tmp_path):
    db = make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_log (firm_code, client_code, started_at, "
            "completed_at, direction, entities_synced, errors) VALUES "
            "(?,?,?,?,?,?,?)",
            ('F1', 'C1', '2026-04-20T09:00:00Z',
             '2026-04-20T09:01:00Z', 'incremental', 42, 0),
        )
        conn.commit()
    html = render_sync_dashboard(db, firm_code='F1', client_code='C1')
    assert '2026-04-20T09:01:00Z' in html
    assert '42 entities' in html


def test_dashboard_shows_conflict_count(tmp_path):
    db = make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        for i in range(3):
            conn.execute(
                "INSERT INTO qbo_sync_state "
                "(firm_code, client_code, entity_type, qbo_id, sync_status) "
                "VALUES (?,?,?,?,?)",
                ('F1', 'C1', 'JournalEntry', f'Q{i}', 'conflict'),
            )
        conn.commit()
    html = render_sync_dashboard(db, firm_code='F1', client_code='C1')
    assert '>3' in html or 'Conflicts pending</th><td>3' in html


def test_conflicts_page_empty(tmp_path):
    db = make_db(tmp_path)
    html = render_conflicts_page(db, firm_code='F1', client_code='C1')
    assert 'Conflicts' in html
    assert 'None.' in html


def test_conflicts_page_renders_resolve_buttons(tmp_path):
    db = make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, local_id, "
            "sync_status, conflict_details) VALUES (?,?,?,?,?,?,?)",
            ('F1', 'C1', 'JournalEntry', 'Q1', 'je-1', 'conflict',
             json.dumps({'flagged_at': '2026-04-20T00:00:00Z'})),
        )
        conn.commit()
    html = render_conflicts_page(db, firm_code='F1', client_code='C1')
    assert 'Q1' in html
    assert 'je-1' in html
    assert 'otocpa_wins' in html
    assert 'qbo_wins' in html
    assert 'flag_for_review' in html
    assert '/qbo/conflicts/resolve' in html


def test_handle_resolve_conflict_returns_json(tmp_path):
    db = make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, sync_status) "
            "VALUES (?,?,?,?,?)",
            ('F1', 'C1', 'JournalEntry', 'Q1', 'conflict'),
        )
        conn.commit()
    status, ctype, body = handle_resolve_conflict(
        db, firm_code='F1', client_code='C1',
        entity_type='JournalEntry', qbo_id='Q1',
        strategy='flag_for_review',
    )
    assert status == 200
    assert ctype == 'application/json'
    parsed = json.loads(body)
    assert parsed['status'] == 'pending_review'


def test_handle_resolve_conflict_unknown_strategy_500(tmp_path):
    db = make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, sync_status) "
            "VALUES (?,?,?,?,?)",
            ('F1', 'C1', 'JournalEntry', 'Q1', 'conflict'),
        )
        conn.commit()
    status, _, body = handle_resolve_conflict(
        db, firm_code='F1', client_code='C1',
        entity_type='JournalEntry', qbo_id='Q1',
        strategy='coin_flip',
    )
    assert status == 500
    assert json.loads(body)['ok'] is False


def test_handle_sync_status_api_returns_json(tmp_path):
    db = make_db(tmp_path)
    status, ctype, body = handle_sync_status_api(
        db, firm_code='F1', client_code='C1',
    )
    assert status == 200
    assert ctype == 'application/json'
    parsed = json.loads(body)
    assert parsed['firm_code'] == 'F1'
    assert parsed['conflicts_pending'] == 0


def test_handle_webhook_route_always_200_even_bad_signature(tmp_path):
    db = make_db(tmp_path)
    status, _, body = handle_webhook_route(
        b'{"eventNotifications":[]}', signature_header='bogus',
        db_path=db, verifier_token='realtoken',
    )
    # 200 always (Stripe-style). The body carries the real status.
    assert status == 200
    payload = json.loads(body)
    assert payload['ok'] is False
    assert payload['status'] == 401

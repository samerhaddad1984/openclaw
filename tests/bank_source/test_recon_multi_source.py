"""Phase 5 — multi-source reconciliation helpers."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.bank_source_recon import (  # noqa: E402
    get_unmatched_bank_transactions,
    period_totals_by_source,
    source_badge,
)
from src.integrations.bank_source_schema import ensure_bank_source_schema  # noqa: E402


def _mk_db(tmp_path):
    db = tmp_path / 'rc.db'
    ensure_bank_source_schema(db)
    return db


def _seed(db, *, id, source='plaid', date='2026-04-20',
          amount=-50.0, description='x',
          matched=None, hidden=0, firm='F1', client='C1',
          external=None):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions "
            "(id, firm_code, client_code, source, external_id, date, "
            " amount, description, matched_document_id, hidden_duplicate) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (id, firm, client, source, external, date, amount, description,
             matched, hidden),
        )
        conn.commit()


def test_get_unmatched_excludes_matched(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='bt-1', matched=None)
    _seed(db, id='bt-2', matched='DOC-9')
    rows = get_unmatched_bank_transactions(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    ids = [r['id'] for r in rows]
    assert ids == ['bt-1']


def test_get_unmatched_excludes_hidden_by_default(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', hidden=0)
    _seed(db, id='plaid-1', source='plaid', hidden=1)
    rows = get_unmatched_bank_transactions(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    assert [r['id'] for r in rows] == ['qbo-1']


def test_get_unmatched_includes_hidden_when_requested(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', hidden=0)
    _seed(db, id='plaid-1', source='plaid', hidden=1)
    rows = get_unmatched_bank_transactions(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
        include_hidden=True,
    )
    assert sorted(r['id'] for r in rows) == ['plaid-1', 'qbo-1']


def test_get_unmatched_period_filter(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='mar', date='2026-03-31')
    _seed(db, id='apr', date='2026-04-15')
    _seed(db, id='may', date='2026-05-01')
    rows = get_unmatched_bank_transactions(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    assert [r['id'] for r in rows] == ['apr']


def test_get_unmatched_firm_client_scoped(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='good', firm='F1', client='C1')
    _seed(db, id='bad-firm', firm='F2', client='C1')
    _seed(db, id='bad-client', firm='F1', client='C2')
    rows = get_unmatched_bank_transactions(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    assert [r['id'] for r in rows] == ['good']


def test_source_badge_shapes():
    assert 'QBO' in source_badge('qbo')
    assert 'Plaid' in source_badge('plaid')
    assert 'Plaid' in source_badge(None)  # defaults
    # case-insensitive
    assert 'QBO' in source_badge('QBO')


def test_period_totals_by_source_separates_hidden(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', amount=-100.0)
    _seed(db, id='qbo-2', source='qbo', amount=-25.0)
    _seed(db, id='plaid-1', source='plaid', amount=200.0)
    _seed(db, id='plaid-2', source='plaid', amount=-100.0, hidden=1)
    totals = period_totals_by_source(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    assert totals['visible_count'] == 3
    assert totals['hidden_count'] == 1
    assert totals['sources']['qbo']['count'] == 2
    assert totals['sources']['qbo']['total'] == -125.0
    assert totals['sources']['plaid']['count'] == 1
    assert totals['sources']['plaid']['hidden'] == 1


def test_totals_handles_empty_period(tmp_path):
    db = _mk_db(tmp_path)
    totals = period_totals_by_source(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    assert totals == {'sources': {}, 'visible_count': 0, 'hidden_count': 0}


def test_dedup_flow_end_to_end(tmp_path):
    """Exercise the full flow: seed rows from two sources, run dedup,
    confirm reconciliation query drops the Plaid duplicate."""
    from src.engines.bank_tx_dedup import BankTransactionDeduplicator

    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
          amount=-50.0, description='Acme Supplies', external='Q-1')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
          amount=-50.0, description='ACME SUPPLIES', external='P-1')
    # Dedup hides plaid-1
    BankTransactionDeduplicator('F1', 'C1', db).mark_duplicates(
        auto_apply=True,
    )
    rows = get_unmatched_bank_transactions(
        db, firm_code='F1', client_code='C1',
        period_start='2026-04-01', period_end='2026-04-30',
    )
    assert [r['id'] for r in rows] == ['qbo-1']

    # UI-side: show a source badge for each surviving row
    for row in rows:
        badge = source_badge(row['source'])
        assert 'source-badge' in badge

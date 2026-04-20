"""Phase 3 — bank-transaction dedup across QBO + Plaid sources."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.bank_tx_dedup import (  # noqa: E402
    BankTransactionDeduplicator,
    _compute_confidence,
    _days_between,
    _desc_similarity,
)
from src.integrations.bank_source_schema import ensure_bank_source_schema  # noqa: E402


def _mk_db(tmp_path):
    db = tmp_path / 'd.db'
    ensure_bank_source_schema(db)
    return db


def _seed(db, *, id, source, date, amount, description=None,
          firm='F1', client='C1', external_id=None, hidden=0):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions "
            "(id, firm_code, client_code, source, external_id, date, "
            " amount, description, hidden_duplicate) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (id, firm, client, source, external_id, date, amount,
             description, hidden),
        )
        conn.commit()


# --- helper tests ---

def test_days_between_simple():
    assert _days_between('2026-04-20', '2026-04-22') == 2.0
    assert _days_between('2026-04-20', '2026-04-20') == 0.0
    assert _days_between('2026-04-20T10:00:00Z', '2026-04-22T23:59:59Z') == 2.0
    assert _days_between('bogus', '2026-04-20') is None


def test_compute_confidence_weights():
    c = _compute_confidence(amount_match=True, date_delta=0.0,
                              tolerance_days=2.0, desc_similarity=1.0)
    assert 0.99 <= c <= 1.0
    c2 = _compute_confidence(amount_match=True, date_delta=2.0,
                               tolerance_days=2.0, desc_similarity=0.0)
    assert abs(c2 - 0.60) < 1e-9
    c3 = _compute_confidence(amount_match=False, date_delta=0.0,
                               tolerance_days=2.0, desc_similarity=0.0)
    # No amount + no desc but perfect date: only date component scores.
    assert abs(c3 - 0.20) < 1e-9
    c4 = _compute_confidence(amount_match=False, date_delta=2.0,
                               tolerance_days=2.0, desc_similarity=0.0)
    assert c4 == 0.0


def test_desc_similarity_neutral_when_empty():
    assert _desc_similarity('', 'Acme Coffee') == 0.5
    assert _desc_similarity(None, None) == 0.5


# --- detection tests ---

def test_no_duplicates_returns_empty(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20', amount=-50.0,
           description='Acme')
    dup = BankTransactionDeduplicator('F1', 'C1', db).find_duplicates()
    assert dup == []


def test_exact_match_detected(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.00, description='Acme Supplies')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-50.00, description='ACME SUPPLIES')
    dup = BankTransactionDeduplicator('F1', 'C1', db).find_duplicates()
    assert len(dup) == 1
    # QBO wins when both sources present
    assert dup[0]['primary_source'] == 'qbo'
    assert dup[0]['primary_id'] == 'qbo-1'
    assert dup[0]['duplicate_source'] == 'plaid'
    assert dup[0]['duplicate_id'] == 'plaid-1'
    assert dup[0]['confidence'] >= 0.9


def test_within_tolerance_days_detected(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.00, description='Acme')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-22',
           amount=-50.00, description='Acme')
    dup = BankTransactionDeduplicator('F1', 'C1', db).find_duplicates()
    assert len(dup) == 1
    assert dup[0]['date_delta_days'] == 2.0


def test_beyond_tolerance_days_skipped(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.00, description='Acme')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-25',  # 5 days
           amount=-50.00, description='Acme')
    dup = BankTransactionDeduplicator('F1', 'C1', db).find_duplicates(
        tolerance_days=2.0,
    )
    assert dup == []


def test_amount_mismatch_not_detected(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.00, description='Acme')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-55.00, description='Acme')
    dup = BankTransactionDeduplicator('F1', 'C1', db).find_duplicates()
    assert dup == []


def test_same_source_not_flagged(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20', amount=-50.0)
    _seed(db, id='qbo-2', source='qbo', date='2026-04-20', amount=-50.0)
    dup = BankTransactionDeduplicator('F1', 'C1', db).find_duplicates()
    assert dup == []


def test_cross_client_not_flagged(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.0, client='CA')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-50.0, client='CB')
    # Looking at client=CA only; plaid-1 is on CB.
    dup = BankTransactionDeduplicator('F1', 'CA', db).find_duplicates()
    assert dup == []


# --- mark / persist tests ---

def test_mark_duplicates_hides_high_confidence(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.0, description='Acme Supplies')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-50.0, description='Acme Supplies')
    dedup = BankTransactionDeduplicator('F1', 'C1', db)
    n = dedup.mark_duplicates(auto_apply=True)
    assert n == 1
    with sqlite3.connect(db) as conn:
        hid = conn.execute(
            "SELECT hidden_duplicate FROM bank_transactions WHERE id='plaid-1'"
        ).fetchone()[0]
        # The QBO winner remains visible.
        visible = conn.execute(
            "SELECT hidden_duplicate FROM bank_transactions WHERE id='qbo-1'"
        ).fetchone()[0]
    assert hid == 1
    assert visible == 0


def test_mark_duplicates_records_audit_row(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.0, description='Acme')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-50.0, description='Acme')
    dedup = BankTransactionDeduplicator('F1', 'C1', db)
    dedup.mark_duplicates(auto_apply=True, resolved_by='auto')
    log = dedup.list_dedup_log()
    assert len(log) == 1
    assert log[0]['primary_source'] == 'qbo'
    assert log[0]['primary_id'] == 'qbo-1'
    assert log[0]['duplicate_id'] == 'plaid-1'
    assert log[0]['match_confidence'] >= 0.9
    assert log[0]['resolved_by'] == 'auto'


def test_low_confidence_not_auto_hidden(tmp_path):
    """Confidence below threshold: the dedup row is recorded but the
    duplicate stays visible. CPA reviews manually."""
    db = _mk_db(tmp_path)
    # Different description + 2 days apart → date_delta 2 with tolerance
    # 2 → date_score=0; desc_similarity low → total ~0.60 + small.
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-50.0, description='Acme Supplies Co')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-22',
           amount=-50.0, description='AMZN Mktp')
    dedup = BankTransactionDeduplicator('F1', 'C1', db)
    n = dedup.mark_duplicates(auto_apply=True)
    assert n == 0  # below 0.75 threshold
    with sqlite3.connect(db) as conn:
        hid = conn.execute(
            "SELECT hidden_duplicate FROM bank_transactions WHERE id='plaid-1'"
        ).fetchone()[0]
    assert hid == 0
    # But we did record the audit trail so the UI can show it.
    assert len(dedup.list_dedup_log()) == 1


def test_mark_duplicates_is_idempotent(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20', amount=-50.0,
           description='Acme')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20', amount=-50.0,
           description='Acme')
    dedup = BankTransactionDeduplicator('F1', 'C1', db)
    for _ in range(3):
        dedup.mark_duplicates(auto_apply=True)
    assert len(dedup.list_dedup_log()) == 1


def test_unmark_duplicate_restores_visibility(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20', amount=-50.0,
           description='Acme')
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20', amount=-50.0,
           description='Acme')
    dedup = BankTransactionDeduplicator('F1', 'C1', db)
    dedup.mark_duplicates(auto_apply=True)
    ok = dedup.unmark_duplicate(duplicate_id='plaid-1', resolved_by='sam')
    assert ok is True
    with sqlite3.connect(db) as conn:
        hid = conn.execute(
            "SELECT hidden_duplicate FROM bank_transactions WHERE id='plaid-1'"
        ).fetchone()[0]
    assert hid == 0
    # Audit trail preserved with the manual unmark breadcrumb.
    log = dedup.list_dedup_log()
    assert log[0]['resolved_by'].startswith('manual_unmark:')


def test_qbo_wins_over_plaid_when_both_present(tmp_path):
    db = _mk_db(tmp_path)
    # Seed plaid first to prove source-rank, not insert-order, decides.
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-25.0, description='Coffee')
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20',
           amount=-25.0, description='Coffee')
    dedup = BankTransactionDeduplicator('F1', 'C1', db)
    dups = dedup.find_duplicates()
    assert dups[0]['primary_source'] == 'qbo'
    assert dups[0]['primary_id'] == 'qbo-1'


def test_hidden_rows_excluded_from_reconciliation(tmp_path):
    """Reconciliation queries MUST filter hidden_duplicate=0. This test
    pins the contract so the recon engine stays consistent."""
    db = _mk_db(tmp_path)
    _seed(db, id='qbo-1', source='qbo', date='2026-04-20', amount=-50.0)
    _seed(db, id='plaid-1', source='plaid', date='2026-04-20',
           amount=-50.0, hidden=1)
    with sqlite3.connect(db) as conn:
        visible = conn.execute(
            "SELECT id FROM bank_transactions "
            "WHERE firm_code='F1' AND client_code='C1' "
            "AND COALESCE(hidden_duplicate, 0) = 0"
        ).fetchall()
    assert [r[0] for r in visible] == ['qbo-1']

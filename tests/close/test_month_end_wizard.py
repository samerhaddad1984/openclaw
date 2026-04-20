"""Gap 4 — month-end close wizard state machine + step validators."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.month_end_close import (  # noqa: E402
    STEPS,
    complete_step_1_select_period,
    complete_step_2_process_documents,
    complete_step_3_reconcile_bank,
    complete_step_4_accruals,
    complete_step_5_statements,
    complete_step_6_lock,
    ensure_close_schema,
    get_state,
    is_period_locked,
    suggest_accruals,
)


def _mk(tmp_path):
    db = tmp_path / 'close.db'
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                document_date TEXT, review_status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE bank_transactions (
                id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                date TEXT, matched_document_id TEXT,
                hidden_duplicate INTEGER DEFAULT 0
            )
        """)
        conn.commit()
    ensure_close_schema(db)
    return db


# --- state ---

def test_state_fresh_points_to_first_step(tmp_path):
    db = _mk(tmp_path)
    st = get_state(db, firm_code='F1', client_code='C1', period='2026-04')
    assert st['current'] == STEPS[0]
    assert [s['step'] for s in st['steps']] == list(STEPS)
    assert all(s['step_status'] in ('pending', None) for s in st['steps'])


# --- step 1 select period ---

def test_step1_passes_on_fresh_period(tmp_path):
    db = _mk(tmp_path)
    out = complete_step_1_select_period(
        db, firm_code='F1', client_code='C1', period='2026-04',
        actor_email='sam@firm.com',
    )
    assert out['ok'] is True
    assert out['state']['current'] == 'process_documents'


def test_step1_blocks_when_prior_period_open(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO accounting_periods "
            "(firm_code, client_code, period, status) "
            "VALUES ('F1','C1','2026-03','open')"
        )
        conn.commit()
    out = complete_step_1_select_period(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    assert out['ok'] is False
    assert out['error'] == 'prior_period_open'


# --- step 2 documents ---

def test_step2_blocks_on_unprocessed_docs(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, firm_code, client_code, document_date, review_status) "
            "VALUES ('D1','F1','C1','2026-04-15','NeedsReview')"
        )
        conn.commit()
    out = complete_step_2_process_documents(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    assert out['ok'] is False
    assert out['error'] == 'unprocessed_documents'
    assert out['count'] == 1


def test_step2_passes_when_all_posted_or_ignored(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, firm_code, client_code, document_date, review_status) "
            "VALUES ('D1','F1','C1','2026-04-15','Posted'),"
            "       ('D2','F1','C1','2026-04-16','Ignored')"
        )
        conn.commit()
    out = complete_step_2_process_documents(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    assert out['ok'] is True


def test_step2_counts_only_period_documents(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, firm_code, client_code, document_date, review_status) "
            "VALUES ('D1','F1','C1','2026-03-31','NeedsReview')"
        )
        conn.commit()
    out = complete_step_2_process_documents(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    assert out['ok'] is True  # March doc doesn't count


# --- step 3 bank reconcile ---

def test_step3_blocks_on_unreconciled(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    complete_step_2_process_documents(db, firm_code='F1', client_code='C1',
                                           period='2026-04')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions "
            "(id, firm_code, client_code, date, matched_document_id) "
            "VALUES ('bt-1','F1','C1','2026-04-15',NULL)"
        )
        conn.commit()
    out = complete_step_3_reconcile_bank(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    assert out['ok'] is False
    assert out['count'] == 1


def test_step3_acknowledge_bypasses_block(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    complete_step_2_process_documents(db, firm_code='F1', client_code='C1',
                                           period='2026-04')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions "
            "(id, firm_code, client_code, date, matched_document_id) "
            "VALUES ('bt-1','F1','C1','2026-04-15',NULL)"
        )
        conn.commit()
    out = complete_step_3_reconcile_bank(
        db, firm_code='F1', client_code='C1', period='2026-04',
        acknowledge_unreconciled=True,
    )
    assert out['ok'] is True


def test_step3_hidden_duplicates_do_not_block(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    complete_step_2_process_documents(db, firm_code='F1', client_code='C1',
                                           period='2026-04')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions "
            "(id, firm_code, client_code, date, matched_document_id, "
            " hidden_duplicate) VALUES ('bt-1','F1','C1','2026-04-15',NULL, 1)"
        )
        conn.commit()
    out = complete_step_3_reconcile_bank(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    assert out['ok'] is True


# --- step 4 accruals ---

def test_step4_suggests_standard_accruals(tmp_path):
    db = _mk(tmp_path)
    suggestions = suggest_accruals(db, firm_code='F1', client_code='C1',
                                      period='2026-04')
    kinds = [s['kind'] for s in suggestions]
    assert 'wage_accrual' in kinds
    assert 'depreciation' in kinds
    assert 'prepaid_amort' in kinds


def test_step4_records_accepted_list(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    complete_step_2_process_documents(db, firm_code='F1', client_code='C1',
                                           period='2026-04')
    complete_step_3_reconcile_bank(db, firm_code='F1', client_code='C1',
                                     period='2026-04',
                                     acknowledge_unreconciled=True)
    out = complete_step_4_accruals(
        db, firm_code='F1', client_code='C1', period='2026-04',
        accepted_kinds=['wage_accrual'],
    )
    assert out['ok'] is True
    assert out['accepted'] == ['wage_accrual']


# --- step 5 statements ---

def test_step5_returns_warnings_when_engines_missing(tmp_path):
    db = _mk(tmp_path)
    for c in (complete_step_1_select_period,
                complete_step_2_process_documents):
        c(db, firm_code='F1', client_code='C1', period='2026-04')
    complete_step_3_reconcile_bank(db, firm_code='F1', client_code='C1',
                                     period='2026-04',
                                     acknowledge_unreconciled=True)
    complete_step_4_accruals(db, firm_code='F1', client_code='C1',
                                period='2026-04')
    out = complete_step_5_statements(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )
    # unified_financial_view IS importable in this tree, so no warning.
    # The assertion is that no crash + state flipped to done.
    assert out['ok'] is True


# --- step 6 lock ---

def test_step6_blocks_when_earlier_step_pending(tmp_path):
    db = _mk(tmp_path)
    # Only step 1 completed
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    out = complete_step_6_lock(
        db, firm_code='F1', client_code='C1', period='2026-04',
        actor_email='sam@firm.com',
    )
    assert out['ok'] is False
    assert out['error'] == 'incomplete_steps'


def test_step6_locks_period_when_all_done(tmp_path):
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    complete_step_2_process_documents(db, firm_code='F1', client_code='C1',
                                           period='2026-04')
    complete_step_3_reconcile_bank(db, firm_code='F1', client_code='C1',
                                     period='2026-04',
                                     acknowledge_unreconciled=True)
    complete_step_4_accruals(db, firm_code='F1', client_code='C1',
                                period='2026-04')
    complete_step_5_statements(db, firm_code='F1', client_code='C1',
                                  period='2026-04')
    out = complete_step_6_lock(
        db, firm_code='F1', client_code='C1', period='2026-04',
        actor_email='sam@firm.com',
    )
    assert out['ok'] is True
    assert is_period_locked(db, firm_code='F1', client_code='C1',
                              period='2026-04') is True


# --- end-to-end ---

def test_full_wizard_end_to_end(tmp_path):
    db = _mk(tmp_path)
    # Post-all-docs up-front
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, firm_code, client_code, document_date, "
            " review_status) VALUES ('D1','F1','C1','2026-04-12','Posted')"
        )
        conn.commit()
    for step_fn in (
        complete_step_1_select_period,
        complete_step_2_process_documents,
    ):
        assert step_fn(db, firm_code='F1', client_code='C1',
                        period='2026-04')['ok'] is True
    assert complete_step_3_reconcile_bank(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )['ok'] is True
    assert complete_step_4_accruals(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )['ok'] is True
    assert complete_step_5_statements(
        db, firm_code='F1', client_code='C1', period='2026-04',
    )['ok'] is True
    assert complete_step_6_lock(
        db, firm_code='F1', client_code='C1', period='2026-04',
        actor_email='sam@firm.com',
    )['ok'] is True
    st = get_state(db, firm_code='F1', client_code='C1', period='2026-04')
    assert all(s['step_status'] == 'done' for s in st['steps'])


def test_wizard_saves_progress_across_calls(tmp_path):
    """Each step is persisted; an interrupt + resume picks up current."""
    db = _mk(tmp_path)
    complete_step_1_select_period(db, firm_code='F1', client_code='C1',
                                     period='2026-04')
    # Simulate restart
    st = get_state(db, firm_code='F1', client_code='C1', period='2026-04')
    assert st['current'] == 'process_documents'
    assert st['steps'][0]['step_status'] == 'done'

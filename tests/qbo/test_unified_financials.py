"""Phase 8 — unified trial balance / IS / BS combining native + QBO-origin."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.qbo_financial_view import (  # noqa: E402
    unified_balance_sheet,
    unified_income_statement,
    unified_trial_balance,
)
from src.integrations.qbo_schema import ensure_qbo_sync_schema  # noqa: E402


def _mk_db(tmp_path: Path) -> Path:
    db = tmp_path / 'fin.db'
    ensure_qbo_sync_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE gl_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id TEXT NOT NULL,
                client_code TEXT NOT NULL,
                period TEXT NOT NULL,
                entry_date TEXT NOT NULL,
                account_code TEXT NOT NULL,
                side TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                source TEXT NOT NULL DEFAULT 'manual_je'
            )
        """)
        conn.commit()
    return db


def _seed_qbo_gl(db: Path, *, client='C1', period='2026-04',
                   entry_id='QBO:JE1',
                   entry_date='2026-04-10',
                   lines: list[tuple[str, str, float]]):
    """Each line: (account_code, side, amount)."""
    with sqlite3.connect(db) as conn:
        for acct, side, amt in lines:
            conn.execute(
                "INSERT INTO gl_transactions "
                "(entry_id, client_code, period, entry_date, account_code, "
                " side, amount, source) VALUES (?,?,?,?,?,?,?,?)",
                (entry_id, client, period, entry_date, acct, side, amt, 'qbo'),
            )
        conn.commit()


def test_tb_pure_qbo_origin_balanced(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('5400', 'debit', 150.0),
                             ('1000', 'credit', 150.0)])
    tb = unified_trial_balance(db, client_code='C1', period='2026-04',
                                 native_tb=[])
    assert tb['balanced'] is True
    assert tb['total_debits'] == 150.0
    assert tb['total_credits'] == 150.0
    # Sources
    by_acct = {r['account_code']: r for r in tb['accounts']}
    assert by_acct['5400']['sources'] == ['qbo']
    assert by_acct['1000']['qbo_origin_credit'] == 150.0


def test_tb_combines_native_and_qbo(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('5400', 'debit', 50.0),
                             ('1000', 'credit', 50.0)])
    # Native TB has a different expense line on 5400 and an AR on 1200
    native = [
        {'account_code': '5400', 'debit': 100.0, 'credit': 0.0},
        {'account_code': '1200', 'debit': 200.0, 'credit': 0.0},
        {'account_code': '4100', 'debit': 0.0,   'credit': 300.0},
    ]
    tb = unified_trial_balance(db, client_code='C1', period='2026-04',
                                 native_tb=native)
    by = {r['account_code']: r for r in tb['accounts']}
    assert by['5400']['native_debit'] == 100.0
    assert by['5400']['qbo_origin_debit'] == 50.0
    assert by['5400']['debit'] == 150.0
    assert by['5400']['sources'] == ['native', 'qbo']
    assert by['1200']['sources'] == ['native']
    assert by['1000']['sources'] == ['qbo']


def test_tb_doesnt_read_gl_when_no_qbo_source(tmp_path):
    """gl_transactions rows with source='manual_je' must be ignored —
    they are already represented by the native TB."""
    db = _mk_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO gl_transactions "
            "(entry_id, client_code, period, entry_date, account_code, "
            " side, amount, source) VALUES (?,?,?,?,?,?,?,?)",
            ('MJ1', 'C1', '2026-04', '2026-04-01', '5400', 'debit', 99.0,
             'manual_je'),
        )
        conn.commit()
    native = [{'account_code': '5400', 'debit': 99.0, 'credit': 0.0}]
    tb = unified_trial_balance(db, client_code='C1', period='2026-04',
                                 native_tb=native)
    by = {r['account_code']: r for r in tb['accounts']}
    assert by['5400']['native_debit'] == 99.0
    assert by['5400']['qbo_origin_debit'] == 0.0
    assert by['5400']['debit'] == 99.0
    assert by['5400']['sources'] == ['native']


def test_tb_balanced_flag(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('5400', 'debit', 100.0),
                             ('1000', 'credit', 100.0)])
    tb = unified_trial_balance(db, client_code='C1', period='2026-04',
                                 native_tb=[])
    assert tb['balanced'] is True

    # Now seed an unbalanced run
    (tmp_path / 'db2').mkdir()
    db2 = _mk_db(tmp_path / 'db2')
    _seed_qbo_gl(db2, lines=[('5400', 'debit', 100.0)])  # missing credit
    tb2 = unified_trial_balance(db2, client_code='C1', period='2026-04',
                                  native_tb=[])
    assert tb2['balanced'] is False


def test_source_attribution_counts(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('5400', 'debit', 20.0),
                             ('1000', 'credit', 20.0)])
    native = [
        {'account_code': '5400', 'debit': 10.0, 'credit': 0.0},  # both
        {'account_code': '1200', 'debit': 30.0, 'credit': 0.0},  # native only
    ]
    tb = unified_trial_balance(db, client_code='C1', period='2026-04',
                                 native_tb=native)
    assert tb['sources']['native_accounts'] == 2  # 5400 + 1200
    assert tb['sources']['qbo_origin_accounts'] == 2  # 5400 + 1000
    assert tb['sources']['both'] == 1  # only 5400


def test_income_statement_rolls_revenue_vs_expense(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('5400', 'debit', 50.0),
                             ('4100', 'credit', 200.0),
                             ('1000', 'debit', 150.0)])  # cash from sale
    # To satisfy the unified view we also need matching entries; the
    # IS function only cares about the net per account.
    is_ = unified_income_statement(db, client_code='C1', period='2026-04')
    assert is_['revenue_total'] == 200.0
    assert is_['expense_total'] == 50.0
    assert is_['net_income']    == 150.0


def test_balance_sheet_basic(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('1000', 'debit', 500.0),   # cash asset
                             ('2000', 'credit', 300.0), # AP liability
                             ('3000', 'credit', 200.0)])  # equity
    bs = unified_balance_sheet(db, client_code='C1', period='2026-04')
    assert bs['assets_total'] == 500.0
    assert bs['liabilities_total'] == 300.0
    assert bs['equity_total'] == 200.0
    assert bs['balanced'] is True


def test_balance_sheet_unbalanced_detected(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('1000', 'debit', 500.0)])
    bs = unified_balance_sheet(db, client_code='C1', period='2026-04')
    assert bs['balanced'] is False


def test_period_filter_excludes_other_months(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('5400', 'debit', 100.0)], period='2026-04')
    _seed_qbo_gl(db, lines=[('5400', 'debit', 200.0)], period='2026-05')
    tb = unified_trial_balance(db, client_code='C1', period='2026-04',
                                 native_tb=[])
    by = {r['account_code']: r for r in tb['accounts']}
    assert by['5400']['qbo_origin_debit'] == 100.0


def test_custom_account_classifier(tmp_path):
    db = _mk_db(tmp_path)
    _seed_qbo_gl(db, lines=[('SALES', 'credit', 500.0),
                             ('WAGES', 'debit', 300.0)])
    is_ = unified_income_statement(
        db, client_code='C1', period='2026-04',
        account_classifier=lambda c: {'SALES': 'revenue',
                                        'WAGES': 'expense'}.get(c, 'other'),
    )
    assert is_['revenue_total'] == 500.0
    assert is_['expense_total'] == 300.0
    assert is_['net_income']    == 200.0

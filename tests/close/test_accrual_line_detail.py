"""Blocker 4: per-line accrual suggestions + CPA overrides.

Covers:
- suggest_accruals_detailed returns structured per-kind lines
- depreciation comes from accrual_engine (one line per asset)
- wages are averaged per employee from payroll_entries
- prepaid is 1/12 per active prepaid_expenses row
- post_suggested_accruals_lines honours include/exclude + amount edits
- audit trail (accrual_line_overrides) records both included + skipped
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import month_end_close as mc  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'accrual.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                review_status TEXT, document_date TEXT,
                amount REAL, vendor TEXT, gl_account TEXT
            );
            CREATE TABLE fixed_assets (
                asset_id TEXT PRIMARY KEY,
                client_code TEXT,
                asset_name TEXT,
                cca_class INTEGER,
                cost REAL,
                current_ucc REAL,
                acquisition_date TEXT,
                status TEXT
            );
            CREATE TABLE manual_journal_entries (
                entry_id TEXT PRIMARY KEY,
                client_code TEXT, period TEXT, entry_date TEXT,
                prepared_by TEXT,
                debit_account TEXT, credit_account TEXT,
                amount REAL, description TEXT,
                document_id TEXT, source TEXT, status TEXT,
                auto_reverse INTEGER, accrual_type TEXT
            );
            CREATE TABLE payroll_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, employee_id TEXT,
                employee_name TEXT, pay_period TEXT,
                gross_pay REAL
            );
            CREATE TABLE prepaid_expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, description TEXT,
                balance REAL, status TEXT DEFAULT 'active',
                debit_account TEXT, credit_account TEXT
            );
            CREATE TABLE posting_jobs (
                posting_id TEXT, document_id TEXT,
                target_system TEXT, posting_status TEXT
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code) VALUES ('A','FIRM')",
        )
        # 2 fixed assets (different CCA classes)
        conn.execute(
            "INSERT INTO fixed_assets (asset_id, client_code, asset_name, "
            "cca_class, cost, current_ucc, acquisition_date, status) "
            "VALUES ('A-42','A','Ford F-150 2024',10,45000,45000,'2024-01-01','active')",
        )
        conn.execute(
            "INSERT INTO fixed_assets (asset_id, client_code, asset_name, "
            "cca_class, cost, current_ucc, acquisition_date, status) "
            "VALUES ('A-43','A','Office Dell',50,2000,2000,'2024-01-01','active')",
        )
        # 2 employees × 2 months
        for emp_id, name, p1, p2 in [
            ('E1', 'Jane Smith', '2026-02-15', '2026-03-15'),
            ('E2', 'Bob Wong',   '2026-02-15', '2026-03-15'),
        ]:
            for p in (p1, p2):
                gross = 5000.0 if emp_id == 'E1' else 4000.0
                conn.execute(
                    "INSERT INTO payroll_entries (client_code, employee_id, "
                    "employee_name, pay_period, gross_pay) "
                    "VALUES ('A', ?, ?, ?, ?)",
                    (emp_id, name, p, gross),
                )
        # 2 prepaid expenses
        conn.execute(
            "INSERT INTO prepaid_expenses (client_code, description, balance, "
            "status) VALUES ('A','Insurance 2026', 6000.0, 'active')",
        )
        conn.execute(
            "INSERT INTO prepaid_expenses (client_code, description, balance, "
            "status) VALUES ('A','Software license', 1200.0, 'active')",
        )
        conn.commit()
    return db


def test_depreciation_returns_per_asset_lines(tmp_path):
    db = _mkdb(tmp_path)
    result = mc.suggest_accruals_detailed(
        db, firm_code='FIRM', client_code='A', period='2026-04',
    )
    dep = result['depreciation']
    # One line per eligible asset (rate>0, cost>0) — both assets qualify.
    assert dep['summary']['line_count'] == 2
    keys = {l['line_key'] for l in dep['lines']}
    assert len(keys) == 2
    # Each line carries the account pair + amount + source.
    for l in dep['lines']:
        assert l['amount_cad'] > 0
        assert l['account_debit']
        assert l['account_credit']
        assert l['source'] == 'accrual_engine'


def test_wages_returns_per_employee_lines(tmp_path):
    db = _mkdb(tmp_path)
    result = mc.suggest_accruals_detailed(
        db, firm_code='FIRM', client_code='A', period='2026-04',
    )
    wages = result['wage_accrual']
    assert wages['summary']['line_count'] == 2
    by_emp = {l['employee_id']: l for l in wages['lines']}
    # Jane: avg of two 5000 rows = 5000; Bob: avg of two 4000 rows = 4000
    assert by_emp['E1']['amount_cad'] == 5000.00
    assert by_emp['E2']['amount_cad'] == 4000.00
    assert by_emp['E1']['employee_name'] == 'Jane Smith'


def test_prepaid_returns_per_item_lines(tmp_path):
    db = _mkdb(tmp_path)
    result = mc.suggest_accruals_detailed(
        db, firm_code='FIRM', client_code='A', period='2026-04',
    )
    prep = result['prepaid_amort']
    assert prep['summary']['line_count'] == 2
    by_bal = {l['balance']: l for l in prep['lines']}
    # 6000/12=500; 1200/12=100
    assert by_bal[6000.0]['amount_cad'] == 500.0
    assert by_bal[1200.0]['amount_cad'] == 100.0


def test_empty_data_returns_empty_lines(tmp_path):
    db = _mkdb(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM payroll_entries")
        conn.execute("DELETE FROM prepaid_expenses")
        conn.execute("DELETE FROM fixed_assets")
        conn.commit()
    result = mc.suggest_accruals_detailed(
        db, firm_code='FIRM', client_code='A', period='2026-04',
    )
    for kind in ('depreciation', 'wage_accrual', 'prepaid_amort'):
        assert result[kind]['summary']['line_count'] == 0
        assert result[kind]['lines'] == []
        assert 'No' in result[kind]['hint'] or 'enter' in result[kind]['hint']


def test_cpa_can_edit_individual_amount(tmp_path):
    db = _mkdb(tmp_path)
    # CPA overrides Jane's wage from 5000 → 3500
    line_decisions = [
        {'kind': 'wage_accrual', 'line_key': 'wage:E1',
         'include': True, 'amount': 3500.0,
         'notes': 'only 14 days of the last pay period'},
    ]
    result = mc.post_suggested_accruals_lines(
        db, firm_code='FIRM', client_code='A', period='2026-04',
        line_decisions=line_decisions, actor_email='sam@firm.com',
    )
    assert result['ok'] is True
    assert len(result['posted']) == 1
    assert result['posted'][0]['amount'] == 3500.0
    assert result['posted'][0]['suggested'] == 5000.0
    assert result['posted'][0]['override'] is True
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT amount, description FROM manual_journal_entries "
            "WHERE accrual_type='wage_accrual'"
        ).fetchone()
    assert row[0] == 3500.0


def test_cpa_can_exclude_individual_line(tmp_path):
    db = _mkdb(tmp_path)
    # Include Jane but exclude Bob
    line_decisions = [
        {'kind': 'wage_accrual', 'line_key': 'wage:E1',
         'include': True, 'amount': 5000.0, 'notes': None},
        {'kind': 'wage_accrual', 'line_key': 'wage:E2',
         'include': False, 'amount': 4000.0, 'notes': 'Terminated mid-period'},
    ]
    result = mc.post_suggested_accruals_lines(
        db, firm_code='FIRM', client_code='A', period='2026-04',
        line_decisions=line_decisions, actor_email='sam@firm.com',
    )
    assert len(result['posted']) == 1
    assert len(result['skipped']) == 1
    assert result['skipped'][0]['line_key'] == 'wage:E2'
    assert result['skipped'][0]['reason'] == 'not_included'


def test_posted_jes_reflect_cpa_changes(tmp_path):
    db = _mkdb(tmp_path)
    # Override one prepaid line amount
    line_decisions = [
        {'kind': 'prepaid_amort',
         'line_key': f'prepaid:1',
         'include': True, 'amount': 600.0,  # override 500 → 600
         'notes': 'catch-up for missed Jan'},
    ]
    mc.post_suggested_accruals_lines(
        db, firm_code='FIRM', client_code='A', period='2026-04',
        line_decisions=line_decisions, actor_email='sam@firm.com',
    )
    with sqlite3.connect(db) as conn:
        amount = conn.execute(
            "SELECT amount FROM manual_journal_entries "
            "WHERE accrual_type='prepaid_amort'"
        ).fetchone()[0]
    assert amount == 600.0


def test_audit_trail_captures_overrides(tmp_path):
    db = _mkdb(tmp_path)
    line_decisions = [
        {'kind': 'wage_accrual', 'line_key': 'wage:E1',
         'include': True, 'amount': 4800.0,
         'notes': 'CPA: pro-rated for bonus month'},
        {'kind': 'wage_accrual', 'line_key': 'wage:E2',
         'include': False, 'amount': 0,
         'notes': 'Terminated'},
        {'kind': 'prepaid_amort', 'line_key': 'prepaid:2',
         'include': True, 'amount': 100.0, 'notes': None},
    ]
    mc.post_suggested_accruals_lines(
        db, firm_code='FIRM', client_code='A', period='2026-04',
        line_decisions=line_decisions, actor_email='sam@firm.com',
    )
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT kind, line_key, suggested_amount, final_amount, "
            "       included, actor_email, notes, entry_id "
            "FROM accrual_line_overrides "
            "WHERE client_code='A' AND period='2026-04' ORDER BY id"
        ).fetchall()
    by_key = {r[1]: r for r in rows}
    # wage:E1 — included with override
    e1 = by_key['wage:E1']
    assert e1[2] == 5000.0 and e1[3] == 4800.0 and e1[4] == 1
    assert e1[5] == 'sam@firm.com'
    assert e1[7]  # entry_id populated
    # wage:E2 — excluded, audited
    e2 = by_key['wage:E2']
    assert e2[4] == 0
    assert e2[7] is None  # no entry_id because not posted
    assert 'Terminated' in (e2[6] or '')
    # prepaid:2 — included without override
    p2 = by_key['prepaid:2']
    assert p2[4] == 1


def test_legacy_post_all_path_still_works(tmp_path):
    """Old callers that pass accepted_kinds (not line decisions) must
    still produce valid JEs — the legacy wrapper routes through the
    new line-level engine under the hood."""
    db = _mkdb(tmp_path)
    result = mc.post_suggested_accruals(
        db, firm_code='FIRM', client_code='A', period='2026-04',
        accepted_kinds=['wage_accrual'], actor_email='sam@firm.com',
    )
    # Expect one line per employee → 2 posts
    wages = [p for p in result['posted'] if p['kind'] == 'wage_accrual']
    assert len(wages) == 2
    with sqlite3.connect(db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM manual_journal_entries "
            "WHERE accrual_type='wage_accrual'"
        ).fetchone()[0]
    assert n == 2

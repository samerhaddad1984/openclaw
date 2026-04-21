"""Item 5: wizard step 4 Post is idempotent per request_id.

Double-clicking the button used to double-post wage + prepaid accruals
(depreciation is idempotent via accrual_engine but the other two
accrual kinds mint fresh entry_ids on each call). The new
idempotent_post_accruals_lines wrapper caches the result per
request_id so a second POST with the same id returns the cached
result instead of re-executing.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import month_end_close as mc  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'idemp.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                review_status TEXT, document_date TEXT,
                amount REAL, vendor TEXT, gl_account TEXT
            );
            CREATE TABLE fixed_assets (
                asset_id TEXT PRIMARY KEY,
                client_code TEXT, asset_name TEXT, cca_class INTEGER,
                cost REAL, current_ucc REAL,
                acquisition_date TEXT, status TEXT
            );
            CREATE TABLE manual_journal_entries (
                entry_id TEXT PRIMARY KEY,
                client_code TEXT, period TEXT, entry_date TEXT,
                prepared_by TEXT, debit_account TEXT,
                credit_account TEXT, amount REAL, description TEXT,
                document_id TEXT, source TEXT, status TEXT,
                auto_reverse INTEGER, accrual_type TEXT
            );
            CREATE TABLE payroll_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, employee_id TEXT,
                employee_name TEXT, pay_period TEXT, gross_pay REAL
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
        conn.execute("INSERT INTO firms VALUES ('F','Sam')")
        conn.execute("INSERT INTO clients (client_code, firm_code) VALUES ('A','F')")
        conn.execute(
            "INSERT INTO payroll_entries (client_code, employee_id, "
            "employee_name, pay_period, gross_pay) "
            "VALUES ('A','E1','Jane','2026-02-15', 5000)",
        )
        conn.execute(
            "INSERT INTO payroll_entries (client_code, employee_id, "
            "employee_name, pay_period, gross_pay) "
            "VALUES ('A','E1','Jane','2026-03-15', 5000)",
        )
        conn.commit()
    return db


def _decisions():
    return [
        {'kind': 'wage_accrual', 'line_key': 'wage:E1',
         'include': True, 'amount': 5000.0, 'notes': None,
         'account_debit': '5100', 'account_credit': '2150'},
    ]


def test_first_post_succeeds(tmp_path):
    db = _mkdb(tmp_path)
    r = mc.idempotent_post_accruals_lines(
        db, firm_code='F', client_code='A', period='2026-04',
        line_decisions=_decisions(), actor_email='sam@firm.com',
        request_id='req_first_' + 'x' * 20,
    )
    assert r['ok'] is True
    assert len(r['posted']) == 1
    assert r.get('idempotent_replay') is False


def test_duplicate_request_id_returns_cached_result(tmp_path):
    db = _mkdb(tmp_path)
    rid = 'req_dup_' + 'y' * 20
    first = mc.idempotent_post_accruals_lines(
        db, firm_code='F', client_code='A', period='2026-04',
        line_decisions=_decisions(), actor_email='sam@firm.com',
        request_id=rid,
    )
    second = mc.idempotent_post_accruals_lines(
        db, firm_code='F', client_code='A', period='2026-04',
        line_decisions=_decisions(), actor_email='sam@firm.com',
        request_id=rid,
    )
    assert second.get('idempotent_replay') is True
    # Posted entry_ids are the same (cached)
    assert first['posted'][0]['entry_id'] == second['posted'][0]['entry_id']
    # DB has exactly one manual JE — not two
    with sqlite3.connect(db) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM manual_journal_entries "
            "WHERE accrual_type='wage_accrual'"
        ).fetchone()[0]
    assert n == 1


def test_different_request_ids_both_execute(tmp_path):
    db = _mkdb(tmp_path)
    r1 = mc.idempotent_post_accruals_lines(
        db, firm_code='F', client_code='A', period='2026-04',
        line_decisions=_decisions(), actor_email='sam@firm.com',
        request_id='req_A_' + 'a' * 20,
    )
    r2 = mc.idempotent_post_accruals_lines(
        db, firm_code='F', client_code='A', period='2026-04',
        line_decisions=_decisions(), actor_email='sam@firm.com',
        request_id='req_B_' + 'b' * 20,
    )
    assert r1['posted'][0]['entry_id'] != r2['posted'][0]['entry_id']
    with sqlite3.connect(db) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM manual_journal_entries "
            "WHERE accrual_type='wage_accrual'"
        ).fetchone()[0]
    assert n == 2


def test_concurrent_double_click_only_one_posts(tmp_path):
    db = _mkdb(tmp_path)
    rid = 'req_concurrent_' + 'z' * 20

    results: list[dict] = []
    barrier = threading.Barrier(2)

    def run():
        barrier.wait()
        r = mc.idempotent_post_accruals_lines(
            db, firm_code='F', client_code='A', period='2026-04',
            line_decisions=_decisions(), actor_email='sam@firm.com',
            request_id=rid,
        )
        results.append(r)

    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=run)
    t1.start(); t2.start(); t1.join(); t2.join()

    # One executed, one replayed.
    executed = [r for r in results if not r.get('idempotent_replay')]
    replayed = [r for r in results if r.get('idempotent_replay')]
    assert len(executed) == 1
    assert len(replayed) == 1
    # DB has exactly one posting
    with sqlite3.connect(db) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM manual_journal_entries "
            "WHERE accrual_type='wage_accrual'"
        ).fetchone()[0]
    assert n == 1


def test_wizard_step4_html_contains_request_id_and_onsubmit(tmp_path):
    from src.integrations.gap_routes import _render_wizard_step4_accruals
    # Empty detailed payload — we only care about the form wrapper.
    detailed = {
        'period': '2026-04',
        'depreciation': {'summary': {'total_amount_cad': 0, 'line_count': 0},
                          'lines': [], 'hint': 'No active fixed assets',
                          'description': 'Monthly depreciation',
                          'default_debit_account': '', 'default_credit_account': ''},
        'wage_accrual':  {'summary': {'total_amount_cad': 0, 'line_count': 0},
                          'lines': [], 'hint': 'No payroll',
                          'description': 'Wage accrual',
                          'default_debit_account': '', 'default_credit_account': ''},
        'prepaid_amort': {'summary': {'total_amount_cad': 0, 'line_count': 0},
                          'lines': [], 'hint': 'No prepaid',
                          'description': 'Prepaid',
                          'default_debit_account': '', 'default_credit_account': ''},
    }
    html = _render_wizard_step4_accruals(detailed=detailed, hidden='')
    assert 'client_request_id' in html
    assert 'value="wz4_' in html
    assert 'onsubmit="return _wz4Submit' in html
    assert 'Posting' in html

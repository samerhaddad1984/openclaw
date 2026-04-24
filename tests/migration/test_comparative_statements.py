"""Scope 2.5 — prior-year comparative with imported data."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import comparative_statements as cs  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'cs.db'
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
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
            );
            CREATE TABLE chart_of_accounts (
                account_code TEXT PRIMARY KEY,
                account_name TEXT,
                account_type TEXT
            );
            CREATE TABLE historical_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                source_format TEXT, source_filename TEXT,
                source_blob_path TEXT, source_sha256 TEXT,
                row_count INTEGER, status TEXT,
                mapping_json TEXT, preview_json TEXT,
                posted_entry_count INTEGER,
                posted_at TEXT, posted_by TEXT,
                rolled_back_at TEXT, rolled_back_by TEXT,
                created_at TEXT
            );
            INSERT INTO chart_of_accounts (account_code, account_name, account_type)
            VALUES
              ('1000','Cash','asset'),
              ('2000','Accounts Payable','liability'),
              ('3000','Retained Earnings','equity'),
              ('4000','Revenue','revenue'),
              ('5000','Expense','expense');
            """
        )
        conn.commit()
    return db_path


def _insert(db, client, rows):
    with sqlite3.connect(db) as conn:
        for r in rows:
            conn.execute(
                "INSERT INTO gl_transactions "
                "(entry_id, client_code, period, entry_date, account_code, "
                " side, amount, source) VALUES (?,?,?,?,?,?,?,?)",
                (r.get('entry_id', 'E1'), client, r.get('period', '2024-01'),
                 r['entry_date'], r['account_code'], r['side'],
                 r['amount'], r.get('source', 'manual_je')),
            )
        conn.commit()


def _mark_historical_import(db, client, fmt='caseware', at='2026-04-24'):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO historical_imports "
            "(firm_code, client_code, source_format, status, "
            " posted_at, row_count, posted_entry_count) "
            "VALUES ('FIRM', ?, ?, 'posted', ?, 10, 10)",
            (client, fmt, at),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------


def test_period_bounds_year():
    assert cs._period_bounds('2024') == ('2024-01-01', '2024-12-31')


def test_period_bounds_month_handles_february():
    assert cs._period_bounds('2024-02') == ('2024-02-01', '2024-02-29')


# ---------------------------------------------------------------------------
# Extraction + source detection
# ---------------------------------------------------------------------------


def test_get_prior_year_gl_rows_filters_by_date_and_client(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-06-15', 'account_code': '1000',
         'side': 'debit', 'amount': 100, 'source': 'historical_caseware'},
        {'entry_date': '2025-01-15', 'account_code': '1000',
         'side': 'debit', 'amount': 50},  # current year → excluded
    ])
    _insert(db, 'OTHER', [
        {'entry_date': '2024-07-01', 'account_code': '1000',
         'side': 'debit', 'amount': 9000},
    ])
    rows = cs.get_prior_year_gl_rows(db, 'CLI', '2024')
    assert len(rows) == 1
    assert rows[0]['amount'] == 100


def test_detect_prior_year_source_imported_historical(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-06-15', 'account_code': '1000',
         'side': 'debit', 'amount': 100, 'source': 'historical_caseware'},
    ])
    rows = cs.get_prior_year_gl_rows(db, 'CLI', '2024')
    assert cs.detect_prior_year_source(rows) == cs.SOURCE_HISTORICAL


def test_detect_prior_year_source_native(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-06-15', 'account_code': '1000',
         'side': 'debit', 'amount': 100, 'source': 'manual_je'},
    ])
    rows = cs.get_prior_year_gl_rows(db, 'CLI', '2024')
    assert cs.detect_prior_year_source(rows) == cs.SOURCE_NATIVE


def test_detect_prior_year_source_empty_returns_none():
    assert cs.detect_prior_year_source([]) is None


def test_source_label_describes_caseware_format(db):
    _mark_historical_import(db, 'CLI', fmt='caseware')
    label = cs.get_source_label(db, 'CLI', cs.SOURCE_HISTORICAL)
    assert 'Caseware' in label


def test_source_label_describes_sage_format(db):
    _mark_historical_import(db, 'CLI', fmt='sage50')
    label = cs.get_source_label(db, 'CLI', cs.SOURCE_HISTORICAL)
    assert 'Sage' in label


def test_get_import_date_returns_iso_date(db):
    _mark_historical_import(db, 'CLI', at='2026-04-24T11:00:00+00:00')
    assert cs.get_import_date(db, 'CLI') == '2026-04-24'


# ---------------------------------------------------------------------------
# Roll-up + statement assembly
# ---------------------------------------------------------------------------


def test_roll_up_rows_computes_net_and_tracks_sources():
    rolled = cs.roll_up_rows([
        {'account_code': '1000', 'side': 'debit', 'amount': 150,
         'source': 'historical_caseware'},
        {'account_code': '1000', 'side': 'credit', 'amount': 40,
         'source': 'manual_je'},
    ])
    assert float(rolled['1000']['net']) == 110
    assert cs.SOURCE_HISTORICAL in rolled['1000']['sources']
    assert cs.SOURCE_NATIVE in rolled['1000']['sources']


def test_build_comparative_populates_prior_year(db):
    _insert(db, 'CLI', [
        # 2024 prior year
        {'entry_date': '2024-12-31', 'account_code': '1000',
         'side': 'debit', 'amount': 5000, 'source': 'historical_caseware'},
        {'entry_date': '2024-12-31', 'account_code': '3000',
         'side': 'credit', 'amount': 5000, 'source': 'historical_caseware'},
        {'entry_date': '2024-06-01', 'account_code': '4000',
         'side': 'credit', 'amount': 1200, 'source': 'historical_caseware'},
        {'entry_date': '2024-06-01', 'account_code': '5000',
         'side': 'debit', 'amount': 400, 'source': 'historical_caseware'},
    ])
    _mark_historical_import(db, 'CLI', fmt='caseware')
    current = {'balance_sheet': {}, 'income_statement': {}}
    comparative = cs.build_comparative(db, 'CLI', '2025', '2024', current)
    assert comparative['prior_year'] is not None
    totals = comparative['prior_year']['totals']
    assert totals['assets'] == 5000
    assert totals['equity'] == 5000
    assert totals['revenue'] == 1200
    assert totals['expenses'] == 400


def test_build_comparative_adds_disclosure_note(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-12-31', 'account_code': '1000',
         'side': 'debit', 'amount': 100, 'source': 'historical_caseware'},
    ])
    _mark_historical_import(db, 'CLI', fmt='caseware', at='2026-04-20')
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', {})
    note = comp['comparative_metadata']['disclosure_note']
    assert '2024' in note
    assert 'Caseware' in note
    assert '2026-04-20' in note
    assert 'OtoCPA' in note


def test_build_comparative_no_prior_data_disables_column(db):
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', {})
    assert comp['prior_year'] is None
    assert comp['comparative_metadata']['enabled'] is False
    assert comp['comparative_metadata']['disclosure_note'] == ''


def test_build_comparative_native_prior_year_omits_disclosure(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-06-01', 'account_code': '1000',
         'side': 'debit', 'amount': 500, 'source': 'manual_je'},
    ])
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', {})
    assert comp['prior_year'] is not None
    # Native source → no disclosure footnote required
    assert comp['comparative_metadata']['source'] == cs.SOURCE_NATIVE
    assert comp['comparative_metadata']['disclosure_note'] == ''


def test_build_comparative_labels_imported_period(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-06-01', 'account_code': '1000',
         'side': 'debit', 'amount': 500, 'source': 'historical_sage50'},
    ])
    _mark_historical_import(db, 'CLI', fmt='sage50')
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', {})
    assert comp['comparative_metadata']['source'] == cs.SOURCE_HISTORICAL
    # Individual line items retain their sources tag
    bs_assets = comp['prior_year']['balance_sheet']['assets']
    assert any(cs.SOURCE_HISTORICAL in i['sources'] for i in bs_assets)


def test_toggle_comparative_off_hides_prior_year(db):
    _insert(db, 'CLI', [
        {'entry_date': '2024-06-01', 'account_code': '1000',
         'side': 'debit', 'amount': 500, 'source': 'historical_caseware'},
    ])
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', {})
    off = cs.toggle_comparative(comp, enabled=False)
    assert off['prior_year'] is None
    assert off['comparative_metadata']['enabled'] is False
    # Toggle back on only if underlying data exists
    on = cs.toggle_comparative(comp, enabled=True)
    assert on['comparative_metadata']['enabled'] is True


def test_toggle_comparative_cannot_turn_on_without_data(db):
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', {})
    # No prior data → enabling does not fabricate rows
    on = cs.toggle_comparative(comp, enabled=True)
    assert on['prior_year'] is None
    assert on['comparative_metadata']['enabled'] is False


def test_comparative_preserves_current_year_untouched(db):
    current = {
        'balance_sheet': {'assets': {'total': 12345}},
        'income_statement': {'net_income': 9999},
    }
    comp = cs.build_comparative(db, 'CLI', '2025', '2024', current)
    assert comp['current_year'] is current
    assert comp['current_year']['balance_sheet']['assets']['total'] == 12345

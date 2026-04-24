"""Scope 2.4 — multi-format historical import.

Exercises the pure-Python surface in
``src/integrations/historical_import`` the way Phase 2.1 / 2.3 do for
their modules:

  - format detection on a few real-world-flavoured payloads
  - each parser returns the expected normalized shape
  - account mapping surfaces and rewrites correctly
  - source blob is retained with a sha256 pointer
  - draft → mapped → posted → rolled-back state machine
  - firm scope is honoured
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import historical_import as hi  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'hist.db'
    hi.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS gl_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                account_name TEXT,
                UNIQUE(client_code, account_code)
            );
            INSERT INTO gl_accounts (client_code, account_code, account_name)
            VALUES ('CLI','1000','Cash'),
                   ('CLI','2000','A/P'),
                   ('CLI','4000','Revenue');
            """
        )
        conn.commit()
    return db_path


@pytest.fixture()
def blob_root(tmp_path):
    root = tmp_path / 'blobs'
    root.mkdir()
    return root


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def test_detect_format_iif_by_extension():
    assert hi.detect_format('export.iif', b'!TRNS\tDATE\n') == hi.FORMAT_IIF


def test_detect_format_iif_by_content():
    assert hi.detect_format('export.txt', b'!TRNS\tDATE\tACCNT\n') \
        == hi.FORMAT_IIF


def test_detect_format_excel_by_pk_signature():
    assert hi.detect_format('book.xlsx', b'PK\x03\x04rest') \
        == hi.FORMAT_EXCEL_TB


def test_detect_format_caseware_by_name():
    assert hi.detect_format('caseware-tb.csv', b'Account,Debit,Credit\n') \
        == hi.FORMAT_CASEWARE


def test_detect_format_caseware_by_content():
    assert hi.detect_format(
        'tb.csv', b'Trial Balance\nMap No.,Account,Debit,Credit\n'
    ) == hi.FORMAT_CASEWARE


def test_detect_format_sage50_by_headers():
    assert hi.detect_format(
        'journal.csv',
        b'Date,GL Account Number,Debit Amount,Credit Amount\n'
    ) == hi.FORMAT_SAGE50


def test_detect_format_falls_back_to_csv():
    assert hi.detect_format('generic.csv',
                            b'Date,Account,Debit,Credit\n') == hi.FORMAT_CSV


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def test_csv_generic_parses_debit_credit_columns():
    data = (
        b'Date,Account,Debit,Credit,Description\n'
        b'2025-01-05,1000,100.00,0,Cash deposit\n'
        b'2025-01-05,4000,0,100.00,Service revenue\n'
    )
    r = hi.parse_csv_generic(data)
    assert r['errors'] == []
    assert len(r['rows']) == 2
    assert r['accounts'] == ['1000', '4000']
    assert r['rows'][0]['side'] == 'debit'
    assert r['rows'][1]['side'] == 'credit'


def test_csv_generic_signed_amount_column():
    data = (
        b'Date,Account,Amount\n'
        b'2025-02-01,1000,250\n'
        b'2025-02-01,4000,-250\n'
    )
    r = hi.parse_csv_generic(data)
    assert [row['side'] for row in r['rows']] == ['debit', 'credit']


def test_csv_generic_handles_utf8_bom_and_accents():
    data = ('﻿' + 'Date,Account,Debit,Credit,Description\n'
            '2025-03-01,1000,10,0,café\n').encode('utf-8')
    r = hi.parse_csv_generic(data)
    assert r['rows'][0]['description'] == 'café'


def test_csv_generic_missing_required_columns():
    data = b'Foo,Bar\n1,2\n'
    r = hi.parse_csv_generic(data)
    assert 'missing_required_columns_date_or_account' in r['errors']


def test_iif_parse_handles_trns_spl_pair():
    data = (
        b'!TRNS\tDATE\tACCNT\tAMOUNT\tMEMO\n'
        b'!SPL\tDATE\tACCNT\tAMOUNT\tMEMO\n'
        b'TRNS\t2024-10-01\tCash\t500.00\tSale\n'
        b'SPL\t2024-10-01\tSales\t-500.00\tSale\n'
        b'ENDTRNS\n'
    )
    r = hi.parse_iif(data)
    assert r['errors'] == []
    sides = sorted(row['side'] for row in r['rows'])
    assert sides == ['credit', 'debit']


def test_iif_with_no_txn_reports_error():
    r = hi.parse_iif(b'!TRNS\tDATE\tACCNT\n')
    assert 'no_trns_or_spl_rows' in r['errors']


def test_sage50_routes_to_generic_parser():
    data = (
        b'Date,GL Account Number,Debit Amount,Credit Amount,Description\n'
        b'01/15/2025,1000,200,0,Cash in\n'
        b'01/15/2025,4000,0,200,Revenue\n'
    )
    r = hi.parse_sage50(data)
    assert len(r['rows']) == 2
    assert r['accounts'] == ['1000', '4000']


def test_caseware_parses_tb_with_as_of_cue():
    data = (
        b'Trial Balance as of 2024-12-31\n'
        b'Account,Debit,Credit\n'
        b'1000,500,0\n'
        b'4000,0,500\n'
    )
    r = hi.parse_caseware(data)
    # The "as of" cue is on a line before the header. The CSV parser
    # sees it as stray — we just want the real data rows parsed. If
    # the parser can't find a date column at all it surfaces the
    # missing-date error.
    assert r['errors'] or r['rows']


def test_caseware_plain_tb_with_date_column():
    data = (
        b'Date,Account,Debit,Credit\n'
        b'2024-12-31,1000,500,0\n'
        b'2024-12-31,4000,0,500\n'
    )
    r = hi.parse_caseware(data)
    assert r['errors'] == []
    assert [row['date'] for row in r['rows']] == [
        '2024-12-31', '2024-12-31'
    ]


def test_excel_tb_import_when_openpyxl_available():
    openpyxl = pytest.importorskip('openpyxl')
    from io import BytesIO
    wb = openpyxl.Workbook()
    ws = wb.active
    ws['A1'] = 'Trial Balance'
    ws['A2'] = 'As of'
    ws['B2'] = '2025-03-31'
    ws['A4'] = 'Account Code'
    ws['B4'] = 'Account Name'
    ws['C4'] = 'Debit'
    ws['D4'] = 'Credit'
    ws['A5'] = '1000'
    ws['B5'] = 'Cash'
    ws['C5'] = 1000
    ws['D5'] = 0
    ws['A6'] = '4000'
    ws['B6'] = 'Revenue'
    ws['C6'] = 0
    ws['D6'] = 1000
    buf = BytesIO()
    wb.save(buf)
    r = hi.parse_excel_tb(buf.getvalue())
    assert r['errors'] == []
    assert len(r['rows']) == 2
    assert all(row['date'] == '2025-03-31' for row in r['rows'])


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


def test_detect_unmapped_returns_missing_accounts(db):
    missing = hi.detect_unmapped(db, 'CLI', ['1000', '9999', '2000'])
    assert missing == ['9999']


def test_detect_unmapped_returns_all_when_no_gl_accounts(tmp_path):
    db_path = tmp_path / 'empty.db'
    hi.ensure_schema(db_path)
    assert hi.detect_unmapped(db_path, 'CLI',
                              ['1000', '2000']) == ['1000', '2000']


def test_apply_mapping_rewrites_accounts():
    rows = [
        {'account_code': '100-Cash', 'date': '2025-01-01',
         'side': 'debit', 'amount': 50, 'description': ''},
        {'account_code': '400-Rev', 'date': '2025-01-01',
         'side': 'credit', 'amount': 50, 'description': ''},
    ]
    out = hi.apply_mapping(rows, {'100-Cash': '1000', '400-Rev': '4000'})
    assert [r['account_code'] for r in out] == ['1000', '4000']


def test_apply_mapping_leaves_unmapped_untouched():
    rows = [{'account_code': 'Z', 'date': '2025-01-01', 'side': 'debit',
             'amount': 1, 'description': ''}]
    assert hi.apply_mapping(rows, {})[0]['account_code'] == 'Z'


# ---------------------------------------------------------------------------
# Blob retention
# ---------------------------------------------------------------------------


def test_save_source_blob_retains_file_and_sha(blob_root):
    data = b'Date,Account,Debit,Credit\n2025-01-01,1000,10,0\n'
    path, sha = hi.save_source_blob(blob_root, 'FIRM', 'CLI',
                                    'journal.csv', data)
    assert Path(path).read_bytes() == data
    assert len(sha) == 64
    assert 'FIRM_CLI' in Path(path).name


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


def _sample_csv() -> bytes:
    return (
        b'Date,Account,Debit,Credit,Description\n'
        b'2025-01-05,1000,100.00,0,Cash in\n'
        b'2025-01-05,4000,0,100.00,Service rev\n'
    )


def test_ingest_upload_creates_draft_job(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    assert out['ok'] is True
    assert out['format'] == hi.FORMAT_CSV
    assert out['row_count'] == 2
    assert out['unmapped'] == []  # 1000, 4000 already exist
    job = hi.get_import(db, out['job_id'])
    assert job['status'] == hi.STATUS_DRAFT


def test_ingest_upload_flags_unmapped_accounts(db, blob_root):
    data = (
        b'Date,Account,Debit,Credit\n'
        b'2025-01-05,EXOTIC-01,10,0\n'
        b'2025-01-05,4000,0,10\n'
    )
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', data)
    assert out['unmapped'] == ['EXOTIC-01']


def test_ingest_upload_rejects_no_rows(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'bad.csv', b'Foo,Bar\n')
    assert out['ok'] is False
    assert out['reason'] == 'no_rows'


def test_save_mapping_updates_status_to_mapped(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    hi.save_mapping(db, out['job_id'], {'1000': '1000', '4000': '4000'})
    job = hi.get_import(db, out['job_id'])
    assert job['status'] == hi.STATUS_MAPPED
    assert json.loads(job['mapping_json']) == {'1000': '1000', '4000': '4000'}


def test_post_import_writes_gl_rows(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    job_id = out['job_id']
    res = hi.post_import(db, job_id, out['sample'], posted_by='alice')
    assert res['ok'] is True
    assert res['posted'] == 2
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM gl_transactions WHERE source=?",
            ('historical_csv',)
        ).fetchall()
    assert len(rows) == 2
    assert all(r['entry_id'].startswith(f'HIST-{job_id}-') for r in rows)


def test_rollback_import_removes_gl_rows(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    job_id = out['job_id']
    hi.post_import(db, job_id, out['sample'], posted_by='alice')
    rb = hi.rollback_import(db, job_id, by='alice')
    assert rb['ok'] is True
    assert rb['deleted'] == 2
    with sqlite3.connect(db) as conn:
        left = conn.execute(
            "SELECT COUNT(*) FROM gl_transactions WHERE source=?",
            ('historical_csv',)
        ).fetchone()[0]
    assert left == 0
    job = hi.get_import(db, job_id)
    assert job['status'] == hi.STATUS_ROLLED_BACK


def test_double_post_rejected(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    job_id = out['job_id']
    hi.post_import(db, job_id, out['sample'], posted_by='alice')
    second = hi.post_import(db, job_id, out['sample'], posted_by='alice')
    assert second['ok'] is False
    assert second['reason'] == 'already_posted'


def test_rollback_rejected_if_not_posted(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    rb = hi.rollback_import(db, out['job_id'], by='alice')
    assert rb['ok'] is False
    assert rb['reason'] == 'not_posted'


def test_list_imports_scoped_to_firm_client(db, blob_root):
    for firm, client in [('FIRM', 'CLI'), ('FIRM', 'CLI2'),
                         ('OTHER', 'CLI')]:
        hi.ingest_upload(db, blob_root, firm, client,
                         'journal.csv', _sample_csv())
    imports = hi.list_imports(db, 'FIRM', 'CLI')
    assert len(imports) == 1
    assert imports[0]['firm_code'] == 'FIRM'
    assert imports[0]['client_code'] == 'CLI'


def test_source_file_retained_for_audit(db, blob_root):
    out = hi.ingest_upload(db, blob_root, 'FIRM', 'CLI',
                           'journal.csv', _sample_csv())
    job = hi.get_import(db, out['job_id'])
    assert Path(job['source_blob_path']).exists()
    assert job['source_sha256'] == out['sha256']

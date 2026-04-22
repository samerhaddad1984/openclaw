"""Scope 2 Phase 1 — bulk client CSV import.

Exercises the pure-Python surface in ``src/integrations/client_import``.
The HTTP wiring is thin; these tests validate the interesting parts:

  - the bilingual template can be generated and round-trips through csv
  - CSVs with common encodings (BOM, cp1252) still parse
  - the validator catches missing required fields, invalid emails,
    unknown employee emails, duplicate client_code within the file
    and against the existing DB, invalid language, invalid date
  - dry_run=True never writes
  - committed import actually inserts and returns the list of codes
  - the error-report CSV echoes rejected rows with reasons
  - firm scoping: inserted clients are tagged with the caller's firm
"""
from __future__ import annotations

import csv
import io
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import client_import as ci  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'imp.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT, email TEXT, firm_code TEXT,
                role TEXT, active INTEGER DEFAULT 1
            );
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, contact_email TEXT,
                whatsapp_number TEXT, language TEXT DEFAULT 'fr',
                primary_employee_email TEXT,
                secondary_employee_email TEXT,
                active INTEGER DEFAULT 1
            );
            """
        )
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute("INSERT INTO firms VALUES ('OTHERFIRM','Other CPA')")
        conn.execute(
            "INSERT INTO users (username, email, firm_code, role) "
            "VALUES ('alice','alice@firm.com','FIRM','employee'),"
            "('bob','bob@firm.com','FIRM','employee'),"
            "('carol','carol@other.com','OTHERFIRM','employee')"
        )
        conn.commit()
    return db


def _csv_of(rows, include_header=True):
    buf = io.StringIO()
    w = csv.writer(buf)
    if include_header:
        w.writerow(ci.TEMPLATE_HEADERS)
    for r in rows:
        w.writerow([r.get(h, '') for h in ci.TEMPLATE_HEADERS])
    return buf.getvalue().encode('utf-8')


# ---------------------------------------------------------------------------


def test_csv_template_downloadable():
    blob = ci.generate_template_csv()
    assert blob.startswith('﻿'.encode('utf-8'))  # BOM
    text = blob.decode('utf-8-sig')
    reader = csv.reader(io.StringIO(text))
    headers = next(reader)
    for col in ('client_code', 'client_name', 'email',
                'primary_employee_email'):
        assert col in headers
    # Template has a sample row.
    sample = next(reader)
    assert any('Construction' in c for c in sample)


def test_import_creates_clients(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': 'CLIENT1', 'client_name': 'Client One',
         'email': 'one@c.com', 'language': 'fr',
         'primary_employee_email': 'alice@firm.com'},
        {'client_code': 'CLIENT2', 'client_name': 'Client Two',
         'email': 'two@c.com', 'language': 'en',
         'primary_employee_email': 'bob@firm.com'},
    ])
    rows, _, fatal = ci.parse_csv(blob)
    assert fatal is None and len(rows) == 2
    result = ci.import_rows(db, firm_code='FIRM', rows=rows)
    assert result['imported'] == 2
    assert result['skipped'] == 0
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        got = list(conn.execute(
            "SELECT client_code, firm_code, language, "
            "primary_employee_email FROM clients ORDER BY client_code"
        ))
    assert [r['client_code'] for r in got] == ['CLIENT1', 'CLIENT2']
    assert all(r['firm_code'] == 'FIRM' for r in got)
    assert got[0]['language'] == 'fr'
    assert got[1]['language'] == 'en'


def test_validation_catches_missing_required(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': '', 'client_name': 'Missing Code'},
        {'client_code': 'OK1', 'client_name': ''},
        {'client_code': 'OK2', 'client_name': 'Valid'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows, dry_run=True)
    assert result['total'] == 3
    assert 0 in result['errors']  # missing client_code
    assert 1 in result['errors']  # missing client_name
    assert 2 not in result['errors']  # valid
    assert 'client_code is required' in '\n'.join(result['errors'][0])
    assert 'client_name is required' in '\n'.join(result['errors'][1])


def test_validation_catches_invalid_email(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': 'C1', 'client_name': 'Client',
         'email': 'not-an-email'},
        {'client_code': 'C2', 'client_name': 'Client',
         'primary_employee_email': 'also-bad'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows, dry_run=True)
    assert 0 in result['errors'] and 1 in result['errors']
    assert 'invalid email' in '\n'.join(result['errors'][0])
    assert 'primary_employee_email invalid' in '\n'.join(result['errors'][1])


def test_validation_catches_unknown_employee(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': 'C1', 'client_name': 'Client',
         'primary_employee_email': 'stranger@other.com'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows, dry_run=True)
    assert 0 in result['errors']
    assert 'unknown in firm' in '\n'.join(result['errors'][0])


def test_validation_catches_invalid_language_and_date(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': 'C1', 'client_name': 'Client', 'language': 'de'},
        {'client_code': 'C2', 'client_name': 'Client',
         'fiscal_year_end': '31-12-2026'},
        {'client_code': 'C3', 'client_name': 'Client',
         'fiscal_year_end': '2026-02-30'},  # not a real date
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows, dry_run=True)
    assert 'language must' in '\n'.join(result['errors'][0])
    assert 'fiscal_year_end' in '\n'.join(result['errors'][1])
    assert 'fiscal_year_end' in '\n'.join(result['errors'][2])


def test_dry_run_doesnt_commit(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': 'C1', 'client_name': 'Valid'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows, dry_run=True)
    assert result['dry_run'] is True
    assert result['imported'] == 0  # nothing inserted in dry-run
    with sqlite3.connect(db) as conn:
        n = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    assert n == 0


def test_error_report_downloadable(tmp_path):
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': '', 'client_name': 'missing code'},
        {'client_code': 'OK', 'client_name': 'Ok'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows, dry_run=True)
    err_csv = ci.generate_error_csv(rows, result['errors'])
    # BOM prefix preserved for Excel compatibility.
    assert err_csv.startswith('﻿'.encode('utf-8'))
    text = err_csv.decode('utf-8-sig')
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    assert 'error' in header
    reject_row = next(reader)
    assert 'missing code' in reject_row
    assert any('client_code' in cell for cell in reject_row)


def test_bulk_import_respects_firm_scope(tmp_path):
    """Clients inserted by firm A do not leak into firm B's list."""
    db = _mkdb(tmp_path)
    blob = _csv_of([
        {'client_code': 'C1', 'client_name': 'Client One'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    # Import for FIRM only.
    res = ci.import_rows(db, firm_code='FIRM', rows=rows)
    assert res['imported'] == 1
    with sqlite3.connect(db) as conn:
        firm_clients = conn.execute(
            "SELECT client_code FROM clients WHERE firm_code=?", ('FIRM',),
        ).fetchall()
        other_clients = conn.execute(
            "SELECT client_code FROM clients WHERE firm_code=?",
            ('OTHERFIRM',),
        ).fetchall()
    assert len(firm_clients) == 1
    assert len(other_clients) == 0


def test_duplicate_client_code_handled(tmp_path):
    db = _mkdb(tmp_path)
    # Pre-seed an existing client with the same code.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name) "
            "VALUES ('EXISTING','FIRM','Original')"
        )
        conn.commit()
    blob = _csv_of([
        {'client_code': 'EXISTING', 'client_name': 'Dup DB'},
        {'client_code': 'NEW1', 'client_name': 'New'},
        {'client_code': 'NEW1', 'client_name': 'Dup within CSV'},
    ])
    rows, _, _ = ci.parse_csv(blob)
    result = ci.import_rows(db, firm_code='FIRM', rows=rows)
    # Row 0: duplicate in DB. Row 2: duplicate within CSV.
    assert 0 in result['errors']
    assert 'already exists' in '\n'.join(result['errors'][0])
    assert 2 in result['errors']
    assert 'duplicate within CSV' in '\n'.join(result['errors'][2])
    # Row 1 should succeed.
    assert result['imported'] == 1
    with sqlite3.connect(db) as conn:
        names = {r[0] for r in conn.execute(
            "SELECT client_name FROM clients WHERE client_code='NEW1'"
        ).fetchall()}
    assert 'New' in names


def test_parse_handles_bom_and_blank_rows():
    blob = (
        '﻿'  # BOM
        'client_code,client_name,email\n'
        'C1,Client One,one@c.com\n'
        '\n'  # blank
        ',,\n'  # all-empty
        'C2,Client Two,two@c.com\n'
    ).encode('utf-8')
    rows, headers, fatal = ci.parse_csv(blob)
    assert fatal is None
    assert [r['client_code'] for r in rows] == ['C1', 'C2']
    assert 'client_code' in headers


def test_parse_missing_required_header_is_fatal():
    blob = b'foo,bar\nx,y\n'
    rows, headers, fatal = ci.parse_csv(blob)
    assert fatal and 'missing_header' in fatal


def test_render_import_page_lists_errors():
    preview = {
        'total': 3, 'imported': 1, 'skipped': 2, 'dry_run': True,
        'errors': {0: ['bad email'], 2: ['already exists']},
        'clients': ['OK1'],
    }
    html = ci.render_import_page(firm_code='FIRM', preview=preview)
    assert 'Dry-run' in html
    assert 'bad email' in html
    assert 'already exists' in html
    assert 'template.csv' in html

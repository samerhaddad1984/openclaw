"""Cleanup Item 1: per-uploader aggregation report."""
from __future__ import annotations

import csv
import io
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import uploader_reports as ur  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'ur.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                client_code TEXT, firm_code TEXT,
                vendor TEXT, amount REAL,
                document_date TEXT, review_status TEXT,
                uploaded_at TEXT, created_at TEXT,
                uploader_name TEXT, uploader_email TEXT,
                uploaded_by_portal_user_id INTEGER
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT, email TEXT,
                full_name TEXT, role TEXT, user_token TEXT UNIQUE,
                status TEXT DEFAULT 'active'
            );
        """)
        conn.execute("INSERT INTO clients VALUES ('A','FIRM','Alpha Inc')")
        conn.execute("INSERT INTO clients VALUES ('B','FIRM','Beta Ltd')")
        conn.execute("INSERT INTO clients VALUES ('X','OTHER','Across-firm')")
        # Portal users: owner of A (admin), bookkeeper (contributor, suspended),
        # office manager (contributor)
        for (email, name, role, status, client) in [
            ('owner@a.com', 'Owner', 'admin', 'active', 'A'),
            ('book@a.com', 'Bookkeeper', 'contributor', 'suspended', 'A'),
            ('om@a.com', 'Office Mgr', 'contributor', 'active', 'A'),
        ]:
            conn.execute(
                "INSERT INTO client_portal_users "
                "(firm_code, client_code, email, full_name, role, status, user_token) "
                "VALUES ('FIRM', ?, ?, ?, ?, ?, ?)",
                (client, email, name, role, status, f'tok_{email}_padding' * 3),
            )
        # Documents — 10 from owner, 5 from bookkeeper, 2 from
        # office manager, 3 anonymous, 1 from cross-firm uploader,
        # varying review_status + dates.
        docs = []
        today = datetime.now(timezone.utc).date().isoformat()
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        old = '2020-01-15'
        for i in range(10):
            docs.append((f'O{i}', 'A', 'Owner', 'owner@a.com',
                          100.0, today, 'Posted'))
        for i in range(5):
            docs.append((f'B{i}', 'A', 'Bookkeeper', 'book@a.com',
                          50.0, today, 'NeedsReview'))
        for i in range(2):
            docs.append((f'M{i}', 'A', 'Office Mgr', 'om@a.com',
                          25.0, today, 'Rejected'))
        for i in range(3):
            docs.append((f'N{i}', 'A', None, None,
                          10.0, today, 'New'))
        docs.append(('X1', 'X', 'Other', 'other@x.com',
                     999.0, today, 'Posted'))
        # One old doc (outside default current-month range) from owner
        docs.append(('OLD1', 'A', 'Owner', 'owner@a.com',
                      77.0, old, 'Posted'))
        for d in docs:
            conn.execute(
                "INSERT INTO documents (document_id, client_code, firm_code, "
                "uploader_name, uploader_email, amount, document_date, "
                "review_status, uploaded_at) "
                "VALUES (?, ?, 'FIRM', ?, ?, ?, ?, ?, datetime('now'))",
                d,
            )
        conn.commit()
    return db


def _today():
    return datetime.now(timezone.utc).date().isoformat()


def _week_ago():
    return (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat()


def _month_start():
    return datetime.now(timezone.utc).date().replace(day=1).isoformat()


def test_report_aggregates_correctly(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM', start=_week_ago(),
                         end=_today())
    by_email = {r['uploader_email']: r for r in rows}
    # Three concrete uploaders, anonymous excluded by default
    assert set(by_email.keys()) == {'owner@a.com', 'book@a.com', 'om@a.com'}
    assert by_email['owner@a.com']['document_count'] == 10
    assert by_email['owner@a.com']['total_amount'] == 1000.0
    assert by_email['owner@a.com']['approved_count'] == 10
    assert by_email['book@a.com']['document_count'] == 5
    assert by_email['book@a.com']['pending_count'] == 5
    assert by_email['om@a.com']['rejected_count'] == 2


def test_excludes_anonymous_by_default(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM', start=_week_ago(), end=_today())
    # Anonymous should not appear
    assert '__anonymous__' not in {r['uploader_email'] for r in rows}


def test_includes_anonymous_when_requested(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM',
                         start=_week_ago(), end=_today(),
                         include_anonymous=True)
    emails = {r['uploader_email'] for r in rows}
    assert '__anonymous__' in emails


def test_firm_scope_hides_cross_firm(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM',
                         start=_week_ago(), end=_today())
    emails = {r['uploader_email'] for r in rows}
    assert 'other@x.com' not in emails


def test_owner_scope_sees_cross_firm(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code=None,
                         start=_week_ago(), end=_today())
    emails = {r['uploader_email'] for r in rows}
    assert 'other@x.com' in emails


def test_date_range_filters(tmp_path):
    db = _mkdb(tmp_path)
    # Only the historical OLD1 doc matches
    rows = ur.aggregate(db, firm_code='FIRM',
                         start='2020-01-01', end='2020-12-31')
    by_email = {r['uploader_email']: r for r in rows}
    assert list(by_email.keys()) == ['owner@a.com']
    assert by_email['owner@a.com']['document_count'] == 1


def test_all_clients_vs_single_client(tmp_path):
    db = _mkdb(tmp_path)
    all_rows = ur.aggregate(db, firm_code='FIRM', start=_week_ago(),
                              end=_today())
    only_a = ur.aggregate(db, firm_code='FIRM', client_code='A',
                            start=_week_ago(), end=_today())
    # A has all three uploaders; all_rows same for FIRM (no B uploads
    # seeded for a different uploader).
    assert {r['uploader_email'] for r in only_a} == {
        'owner@a.com', 'book@a.com', 'om@a.com'
    }
    # When narrowing to B (no uploads) expect empty
    only_b = ur.aggregate(db, firm_code='FIRM', client_code='B',
                            start=_week_ago(), end=_today())
    assert only_b == []


def test_sorting_works_all_columns(tmp_path):
    db = _mkdb(tmp_path)
    rows_cnt = ur.aggregate(db, firm_code='FIRM',
                              start=_week_ago(), end=_today(),
                              sort_by='document_count', sort_dir='desc')
    assert rows_cnt[0]['document_count'] >= rows_cnt[-1]['document_count']
    rows_amt = ur.aggregate(db, firm_code='FIRM',
                              start=_week_ago(), end=_today(),
                              sort_by='total_amount', sort_dir='asc')
    amounts = [r['total_amount'] for r in rows_amt]
    assert amounts == sorted(amounts)


def test_invalid_sort_by_raises(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(ValueError):
        ur.aggregate(db, firm_code='FIRM',
                      start=_week_ago(), end=_today(),
                      sort_by='drop table users', sort_dir='asc')


def test_csv_export_format(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM',
                         start=_week_ago(), end=_today())
    csv_bytes = ur.render_csv(rows)
    assert csv_bytes.startswith(b'\xef\xbb\xbf') or csv_bytes.startswith(b'Uploader')  # BOM or header
    # Decode + parse
    text = csv_bytes.decode('utf-8-sig')
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    assert header[0] == 'Uploader'
    assert 'Email' in header
    assert 'Total amount (CAD)' in header
    # Body row count matches aggregate row count
    body = list(reader)
    assert len(body) == len(rows)


def test_drill_down_shows_documents(tmp_path):
    db = _mkdb(tmp_path)
    docs = ur.documents_for_uploader(
        db, firm_code='FIRM', uploader_email='owner@a.com',
        start=_week_ago(), end=_today(),
    )
    assert len(docs) == 10
    assert all(d['uploader_email'] == 'owner@a.com' for d in docs)


def test_drill_down_respects_firm_scope(tmp_path):
    db = _mkdb(tmp_path)
    # Requesting 'other@x.com' from FIRM scope returns empty
    docs = ur.documents_for_uploader(
        db, firm_code='FIRM', uploader_email='other@x.com',
        start=_week_ago(), end=_today(),
    )
    assert docs == []


def test_top_uploaders_this_week(tmp_path):
    db = _mkdb(tmp_path)
    top = ur.top_uploaders_this_week(db, firm_code='FIRM', limit=2)
    assert len(top) == 2
    # Top entry should be owner (10 docs)
    assert top[0]['uploader_email'] == 'owner@a.com'
    assert top[0]['document_count'] == 10


def test_includes_suspended_users_history(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM',
                         start=_week_ago(), end=_today())
    # Bookkeeper is suspended but her historical uploads are kept.
    by_email = {r['uploader_email']: r for r in rows}
    assert 'book@a.com' in by_email
    assert by_email['book@a.com']['document_count'] == 5
    assert by_email['book@a.com']['user_status'] == 'suspended'


def test_default_range_is_current_month(tmp_path):
    db = _mkdb(tmp_path)
    # No start/end → current month. All our docs are dated today
    # except OLD1; expect owner count == 10 (old doc excluded).
    rows = ur.aggregate(db, firm_code='FIRM')
    by_email = {r['uploader_email']: r for r in rows}
    if _today() >= _month_start():
        assert by_email['owner@a.com']['document_count'] == 10


def test_widget_empty_rows_still_renders(tmp_path):
    db = _mkdb(tmp_path)
    html = ur.render_top_uploaders_widget([], lang='en')
    assert 'Top uploaders' in html
    html_fr = ur.render_top_uploaders_widget([], lang='fr')
    assert 'Téléverseurs' in html_fr


def test_widget_renders_top_list():
    html = ur.render_top_uploaders_widget(
        [{'uploader_name': 'Owner', 'client_code': 'A',
          'document_count': 10, 'uploader_email': 'owner@a.com'},
         {'uploader_name': 'Book', 'client_code': 'A',
          'document_count': 5, 'uploader_email': 'book@a.com'}],
        lang='en',
    )
    assert 'Owner' in html
    assert 'Book' in html
    assert '/reports/by_uploader' in html


def test_report_page_structure(tmp_path):
    db = _mkdb(tmp_path)
    rows = ur.aggregate(db, firm_code='FIRM',
                         start=_week_ago(), end=_today())
    html = ur.render_report_page(
        rows=rows, start=_week_ago(), end=_today(),
        firm_code='FIRM', client_code=None,
        clients_available=[{'client_code': 'A', 'client_name': 'Alpha'}],
    )
    assert 'Uploader activity' in html
    assert 'Download CSV' in html
    # Sort links preserve the query params
    assert 'sort_by=document_count' in html
    assert 'View &rarr;' in html or 'View &#x2192;' in html

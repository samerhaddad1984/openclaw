"""Gap 5 — client upload status + notifications + threaded messaging."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.client_status import (  # noqa: E402
    build_client_status,
    create_notification,
    create_thread,
    ensure_client_status_schema,
    get_thread,
    list_threads,
    mark_notifications_read,
    post_message,
    recent_activity,
    unread_count,
    upload_status,
    ytd_summary,
)


def _mk(tmp_path):
    db = tmp_path / 'p.db'
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                client_code TEXT, firm_code TEXT,
                vendor TEXT, amount REAL,
                document_date TEXT, review_status TEXT,
                uploaded_at TEXT, review_timestamp TEXT
            )
        """)
        conn.commit()
    ensure_client_status_schema(db)
    return db


def _add_doc(db, *, doc_id, status, date='2026-04-20',
              amount=0.0, vendor=None, client='C1'):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, vendor, "
            "amount, document_date, review_status, uploaded_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (doc_id, client, vendor, amount, date, status,
             '2026-04-20T10:00:00Z'),
        )
        conn.commit()


# --- upload status ---

def test_upload_status_empty(tmp_path):
    db = _mk(tmp_path)
    s = upload_status(db, client_code='C1')
    assert s == {'total': 0, 'reviewed': 0, 'processing': 0,
                 'needs_attention': 0, 'this_month': 0}


def test_upload_status_classifies_buckets(tmp_path):
    db = _mk(tmp_path)
    _add_doc(db, doc_id='d1', status='Posted')
    _add_doc(db, doc_id='d2', status='Approved')
    _add_doc(db, doc_id='d3', status='Processing')
    _add_doc(db, doc_id='d4', status='New')
    _add_doc(db, doc_id='d5', status='NeedsReview')
    _add_doc(db, doc_id='d6', status='Rejected')
    s = upload_status(db, client_code='C1', period='2026-04')
    assert s['total'] == 6
    assert s['reviewed'] == 2
    assert s['processing'] == 2
    assert s['needs_attention'] == 2
    assert s['this_month'] == 6


def test_upload_status_scoped_to_client(tmp_path):
    db = _mk(tmp_path)
    _add_doc(db, doc_id='d1', status='Posted', client='C1')
    _add_doc(db, doc_id='d2', status='Posted', client='C2')
    s = upload_status(db, client_code='C1')
    assert s['total'] == 1


# --- recent activity ---

def test_recent_activity_interleaves_docs_and_notifications(tmp_path):
    db = _mk(tmp_path)
    _add_doc(db, doc_id='d1', status='Posted', vendor='Metro',
              amount=47.23)
    create_notification(
        db, client_code='C1', kind='question',
        title='CPA asked about Petro Canada',
        body='Please confirm mileage.',
        document_id='d1',
    )
    feed = recent_activity(db, client_code='C1')
    kinds = {e['kind'] for e in feed}
    assert kinds == {'document', 'notification'}


def test_recent_activity_chronological_order(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, vendor, "
            "amount, document_date, review_status, uploaded_at) VALUES "
            "('d1','C1','Metro',10.0,'2026-04-10','Posted','2026-04-10T10:00:00Z')"
        )
        conn.execute(
            "INSERT INTO documents (document_id, client_code, vendor, "
            "amount, document_date, review_status, uploaded_at) VALUES "
            "('d2','C1','Provigo',20.0,'2026-04-20','Posted','2026-04-20T10:00:00Z')"
        )
        conn.commit()
    feed = recent_activity(db, client_code='C1')
    assert feed[0]['ts'] >= feed[-1]['ts']


# --- YTD summary ---

def test_ytd_summary_totals(tmp_path):
    db = _mk(tmp_path)
    _add_doc(db, doc_id='d1', status='Posted', date='2026-04-10', amount=30.0)
    _add_doc(db, doc_id='d2', status='Posted', date='2026-04-15', amount=45.50)
    _add_doc(db, doc_id='d3', status='Posted', date='2025-12-15', amount=99.99)
    s = ytd_summary(db, client_code='C1', year=2026)
    assert s['total_receipts'] == 2
    assert s['total_expenses_cad'] == 75.5


# --- notifications ---

def test_create_notification_increments_unread(tmp_path):
    db = _mk(tmp_path)
    assert unread_count(db, client_code='C1') == 0
    create_notification(db, client_code='C1', kind='approval',
                          title='5 receipts recorded')
    assert unread_count(db, client_code='C1') == 1


def test_mark_all_as_read_empties_unread(tmp_path):
    db = _mk(tmp_path)
    for i in range(3):
        create_notification(db, client_code='C1', kind='approval',
                              title=f'Batch {i}')
    marked = mark_notifications_read(db, client_code='C1')
    assert marked == 3
    assert unread_count(db, client_code='C1') == 0


def test_mark_specific_notifications_read(tmp_path):
    db = _mk(tmp_path)
    ids = [create_notification(db, client_code='C1', kind='approval',
                                  title=f'{i}') for i in range(3)]
    mark_notifications_read(db, client_code='C1',
                              notification_ids=[ids[0]])
    assert unread_count(db, client_code='C1') == 2


def test_notification_sent_on_approval(tmp_path):
    """Contract test: callers upstream can wire this helper on approval."""
    db = _mk(tmp_path)
    create_notification(
        db, client_code='C1', kind='approval',
        title='5 of your receipts are recorded',
        body='The CPA reviewed and posted your recent batch.',
    )
    feed = recent_activity(db, client_code='C1')
    assert any(e['kind'] == 'notification'
                and '5 of your receipts' in e['summary']
                for e in feed)


def test_notification_sent_on_cpa_question(tmp_path):
    db = _mk(tmp_path)
    create_notification(
        db, client_code='C1', kind='question',
        title='Your CPA has a question',
        body='Please confirm mileage on the Petro Canada receipt.',
        document_id='D99',
    )
    feed = recent_activity(db, client_code='C1')
    q = next(e for e in feed if e['kind'] == 'notification')
    assert q['document_id'] == 'D99'
    assert q['unread'] is True


# --- threaded messaging ---

def test_messaging_thread_bidirectional(tmp_path):
    db = _mk(tmp_path)
    t_id = create_thread(db, firm_code='F1', client_code='C1',
                            subject='Petro Canada mileage',
                            document_id='D99')
    post_message(db, thread_id=t_id, sender_type='cpa',
                   sender_id='boss@f.com',
                   body='Was this business travel?')
    post_message(db, thread_id=t_id, sender_type='client',
                   sender_id='client@x.com',
                   body='Yes — Montreal to Quebec City.')
    # CPA opens the thread -> marks client posts read.
    thread = get_thread(db, thread_id=t_id, mark_read_as='cpa')
    assert len(thread['posts']) == 2
    assert thread['posts'][0]['sender_type'] == 'cpa'
    assert thread['posts'][1]['read_at'] is not None


def test_post_message_rejects_bad_sender_type(tmp_path):
    db = _mk(tmp_path)
    t_id = create_thread(db, firm_code='F1', client_code='C1',
                            subject='x')
    with pytest.raises(ValueError):
        post_message(db, thread_id=t_id, sender_type='system',
                       sender_id='bot', body='hi')


def test_list_threads_shows_unread_from_cpa(tmp_path):
    db = _mk(tmp_path)
    t_id = create_thread(db, firm_code='F1', client_code='C1',
                            subject='x')
    post_message(db, thread_id=t_id, sender_type='cpa',
                   sender_id='boss@f.com', body='q1')
    post_message(db, thread_id=t_id, sender_type='cpa',
                   sender_id='boss@f.com', body='q2')
    # Client hasn't opened yet; both show unread_from_cpa=2
    threads = list_threads(db, client_code='C1')
    assert threads[0]['unread_from_cpa'] == 2

    # Client opens the thread.
    get_thread(db, thread_id=t_id, mark_read_as='client')
    threads = list_threads(db, client_code='C1')
    assert threads[0]['unread_from_cpa'] == 0


def test_list_threads_sorted_by_last_post(tmp_path):
    db = _mk(tmp_path)
    t1 = create_thread(db, firm_code='F1', client_code='C1', subject='old')
    post_message(db, thread_id=t1, sender_type='cpa',
                   sender_id='x', body='a')
    t2 = create_thread(db, firm_code='F1', client_code='C1', subject='new')
    post_message(db, thread_id=t2, sender_type='cpa',
                   sender_id='x', body='b')
    threads = list_threads(db, client_code='C1')
    assert [t['subject'] for t in threads[:2]] == ['new', 'old']


# --- bundle ---

def test_build_client_status_includes_every_section(tmp_path):
    db = _mk(tmp_path)
    _add_doc(db, doc_id='d1', status='Posted', vendor='Metro',
              amount=47.23)
    create_notification(db, client_code='C1', kind='approval',
                          title='ok')
    out = build_client_status(db, client_code='C1')
    for key in ('upload_status', 'recent_activity', 'ytd_summary',
                 'unread_notifications', 'threads'):
        assert key in out
    assert out['upload_status']['reviewed'] == 1
    assert out['unread_notifications'] == 1

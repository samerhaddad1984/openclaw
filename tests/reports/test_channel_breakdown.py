"""Phase 5: queue / detail / report surface the ingest channel.

Covers:
- ``render_channel_badge`` renders a pill with the right label + icon
- ``build_channel_where_fragment`` filters rows correctly (NULL→portal)
- ``aggregate`` returns portal/whatsapp/email counts per uploader
- ``render_report_page`` lists the "Channel" column with breakdown
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import queue_filters as qf  # noqa: E402
from src.integrations import uploader_reports as ur  # noqa: E402


# ---------------------------------------------------------------------------
# Channel badge
# ---------------------------------------------------------------------------

def test_channel_badge_portal_fr():
    html = qf.render_channel_badge('portal', lang='fr')
    assert 'Portail' in html
    assert 'channel-portal' in html


def test_channel_badge_whatsapp_en():
    html = qf.render_channel_badge('whatsapp', lang='en')
    assert 'WhatsApp' in html
    assert 'channel-whatsapp' in html
    # Brand-green colour on the WhatsApp badge.
    assert '#25D366' in html


def test_channel_badge_unknown_falls_back_to_portal():
    html = qf.render_channel_badge('martian', lang='en')
    assert 'channel-portal' in html


def test_channel_badge_none_means_portal():
    html = qf.render_channel_badge(None, lang='fr')
    assert 'channel-portal' in html


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def test_parse_channel_filter_qs_empty():
    assert qf.parse_channel_filter_qs(None) == []
    assert qf.parse_channel_filter_qs('') == []


def test_parse_channel_filter_qs_valid():
    assert qf.parse_channel_filter_qs('portal,whatsapp') == [
        'portal', 'whatsapp',
    ]


def test_parse_channel_filter_qs_drops_invalid():
    assert qf.parse_channel_filter_qs('whatsapp,garbage') == ['whatsapp']


def test_build_channel_where_empty():
    frag, params = qf.build_channel_where_fragment([])
    assert frag == ''
    assert params == []


def test_build_channel_where_single():
    frag, params = qf.build_channel_where_fragment(['whatsapp'])
    assert 'uploaded_via_channel' in frag
    assert params == ['whatsapp']


def test_build_channel_where_filters_legacy_null_as_portal(tmp_path):
    db = tmp_path / 'ch.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                client_code TEXT,
                uploaded_via_channel TEXT,
                document_date TEXT
            );
            INSERT INTO documents VALUES ('D1','C','portal','2026-04-01');
            INSERT INTO documents VALUES ('D2','C','whatsapp','2026-04-02');
            INSERT INTO documents VALUES ('D3','C',NULL,'2026-04-03');
        """)
        conn.commit()
    frag, params = qf.build_channel_where_fragment(['portal'])
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            f"SELECT document_id FROM documents d WHERE {frag} "
            "ORDER BY document_id",
            tuple(params),
        ).fetchall()
    ids = [r[0] for r in rows]
    # D1 (explicit 'portal') + D3 (NULL defaulting to 'portal').
    assert ids == ['D1', 'D3']


def test_render_channel_filter_dropdown_preserves_other_params():
    html = qf.render_channel_filter_dropdown(
        selected=['whatsapp'],
        form_action='/',
        preserve_params={'client_code': 'C1', 'channel': 'x'},
        lang='en',
    )
    assert 'client_code' in html
    # 'channel' hidden is suppressed so the form re-emits it fresh.
    assert 'value="x"' not in html
    assert 'selected' in html


# ---------------------------------------------------------------------------
# Report aggregate with channel breakdown
# ---------------------------------------------------------------------------

def _mkdb_for_report(tmp_path):
    db = tmp_path / 'rep.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT);
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                email TEXT, role TEXT DEFAULT 'contributor',
                status TEXT DEFAULT 'active',
                user_token TEXT UNIQUE
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                client_code TEXT,
                uploader_name TEXT, uploader_email TEXT,
                uploaded_via_channel TEXT DEFAULT 'portal',
                amount REAL,
                review_status TEXT DEFAULT 'New',
                document_date TEXT,
                uploaded_at TEXT,
                created_at TEXT
            );
        """)
        conn.execute(
            "INSERT INTO clients VALUES ('C1','F')",
        )
        conn.execute(
            "INSERT INTO client_portal_users (firm_code, client_code, "
            "email, user_token) VALUES ('F','C1','marie@c1','tok1')",
        )
        for doc_id, ch in [
            ('D1', 'portal'), ('D2', 'portal'), ('D3', 'whatsapp'),
            ('D4', 'whatsapp'), ('D5', 'whatsapp'), ('D6', 'email'),
        ]:
            conn.execute(
                "INSERT INTO documents (document_id, client_code, "
                "uploader_name, uploader_email, uploaded_via_channel, "
                "amount, document_date) "
                "VALUES (?, 'C1', 'Marie', 'marie@c1', ?, 10.0, "
                "'2026-04-15')",
                (doc_id, ch),
            )
        conn.commit()
    return db


def test_aggregate_includes_channel_breakdown(tmp_path):
    db = _mkdb_for_report(tmp_path)
    rows = ur.aggregate(
        db, firm_code='F',
        start='2026-04-01', end='2026-04-30',
    )
    assert len(rows) == 1
    r = rows[0]
    assert r['portal_count'] == 2
    assert r['whatsapp_count'] == 3
    assert r['email_count'] == 1
    assert r['document_count'] == 6


def test_report_renders_channel_column(tmp_path):
    db = _mkdb_for_report(tmp_path)
    rows = ur.aggregate(
        db, firm_code='F',
        start='2026-04-01', end='2026-04-30',
    )
    html = ur.render_report_page(
        rows=rows, start='2026-04-01', end='2026-04-30',
        firm_code='F', client_code=None,
    )
    assert '<th>Channel</th>' in html
    assert '2 portal' in html
    assert '3 WhatsApp' in html
    assert '1 email' in html

"""Item 1: queue filter by uploader — dropdown options, badges,
SQL fragment composition, URL parsing."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import queue_filters as qf  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'q.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                uploader_name TEXT, uploader_email TEXT,
                uploaded_by_portal_user_id INTEGER,
                review_status TEXT, document_date TEXT
            );
        """)
        conn.execute("INSERT INTO clients VALUES ('A','FIRM')")
        conn.execute("INSERT INTO clients VALUES ('B','FIRM')")
        conn.execute("INSERT INTO clients VALUES ('X','OTHER')")
        # 12 Owner uploads, 5 Bookkeeper uploads, 3 anonymous.
        for i in range(12):
            conn.execute(
                "INSERT INTO documents (document_id, firm_code, client_code, "
                "uploader_name, uploader_email) "
                "VALUES (?, 'FIRM','A','Owner','owner@c.com')",
                (f'O{i}',),
            )
        for i in range(5):
            conn.execute(
                "INSERT INTO documents (document_id, firm_code, client_code, "
                "uploader_name, uploader_email) "
                "VALUES (?, 'FIRM','A','Bookkeeper','book@c.com')",
                (f'B{i}',),
            )
        for i in range(3):
            conn.execute(
                "INSERT INTO documents (document_id, firm_code, client_code) "
                "VALUES (?, 'FIRM','A')",
                (f'N{i}',),
            )
        # Different firm — should not leak into FIRM's filter options.
        conn.execute(
            "INSERT INTO documents (document_id, firm_code, client_code, "
            "uploader_name, uploader_email) "
            "VALUES ('X1','OTHER','X','Someone','other@x.com')",
        )
        conn.commit()
    return db


def test_dropdown_lists_all_uploaders_with_counts(tmp_path):
    db = _mkdb(tmp_path)
    opts = qf.uploader_filter_options(db, firm_code='FIRM')
    by_key = {o['uploader_key']: o for o in opts}
    assert by_key['owner@c.com']['count'] == 12
    assert by_key['book@c.com']['count'] == 5
    assert by_key[qf.ANON_KEY]['count'] == 3
    # 'Anonymous' is a nicer display name than the sentinel.
    assert by_key[qf.ANON_KEY]['display_name'] == 'Anonymous'
    # Cross-firm uploader not shown for FIRM scope.
    assert 'other@x.com' not in by_key


def test_other_firm_not_in_scoped_options(tmp_path):
    db = _mkdb(tmp_path)
    opts = qf.uploader_filter_options(db, firm_code='OTHER')
    keys = {o['uploader_key'] for o in opts}
    assert keys == {'other@x.com'}


def test_owner_scope_shows_all(tmp_path):
    db = _mkdb(tmp_path)
    # firm_code=None (owner) sees everyone
    opts = qf.uploader_filter_options(db, firm_code=None)
    keys = {o['uploader_key'] for o in opts}
    assert 'owner@c.com' in keys
    assert 'other@x.com' in keys


def test_selecting_uploader_filters_queue():
    frag, params = qf.build_uploader_where_fragment(['owner@c.com'])
    assert 'd.uploader_email' in frag
    assert params == ['owner@c.com']


def test_multiple_uploaders_combined():
    frag, params = qf.build_uploader_where_fragment(
        ['owner@c.com', 'book@c.com']
    )
    assert 'IN' in frag
    assert set(params) == {'owner@c.com', 'book@c.com'}


def test_anonymous_group_shown_separately():
    frag, params = qf.build_uploader_where_fragment([qf.ANON_KEY])
    assert 'IS NULL' in frag
    assert params == []


def test_anonymous_plus_concrete_combined():
    frag, params = qf.build_uploader_where_fragment(
        [qf.ANON_KEY, 'owner@c.com'],
    )
    assert 'IS NULL' in frag
    assert 'IN' in frag
    assert params == ['owner@c.com']


def test_no_uploaders_returns_blank_fragment():
    frag, params = qf.build_uploader_where_fragment(None)
    assert frag == ''
    assert params == []
    frag2, _ = qf.build_uploader_where_fragment([])
    assert frag2 == ''


def test_parse_uploader_filter_qs():
    assert qf.parse_uploader_filter_qs('a@b.com,c@d.com') == [
        'a@b.com', 'c@d.com',
    ]
    assert qf.parse_uploader_filter_qs('') == []
    assert qf.parse_uploader_filter_qs(None) == []
    # URL-encoded plus is a space
    assert qf.parse_uploader_filter_qs('a%40b.com') == ['a@b.com']


def test_cleared_filter_shows_all():
    """Clear link points at ?uploader= (empty value)."""
    html = qf.render_uploader_filter_dropdown(
        [{'uploader_key': 'a@b', 'display_name': 'A', 'count': 1}],
        selected_keys=['a@b'], form_action='/',
    )
    assert '?uploader=' in html
    assert 'Clear' in html


def test_uploader_badge_renders_with_name_and_color():
    b1 = qf.render_uploader_badge('Owner', 'owner@c.com')
    b2 = qf.render_uploader_badge('Bookkeeper', 'book@c.com')
    assert 'Owner' in b1
    assert 'Bookkeeper' in b2
    # Deterministic colouring — same email → same colour across calls
    again = qf.render_uploader_badge('Owner', 'owner@c.com')
    assert again == b1
    # Different emails yield different chips (html differs somewhere)
    assert b1 != b2


def test_anonymous_badge_empty_for_no_identity():
    # When the row has no uploader_name + no uploader_email, we emit
    # no chip; the caller chooses whether to render 'Anonymous'.
    assert qf.render_uploader_badge(None, None) == ''
    assert qf.render_uploader_badge('', '') == ''


def test_dropdown_preserves_other_filter_params():
    html = qf.render_uploader_filter_dropdown(
        [{'uploader_key': 'a@b', 'display_name': 'A', 'count': 1}],
        selected_keys=[], form_action='/',
        preserve_params={'status': 'Ready', 'queue_mode': 'mine',
                          'q': 'acme'},
    )
    assert 'name="status"' in html
    assert 'value="Ready"' in html
    assert 'name="q"' in html
    assert 'value="acme"' in html


def test_dropdown_selected_keys_highlighted():
    html = qf.render_uploader_filter_dropdown(
        [
            {'uploader_key': 'a@b', 'display_name': 'A', 'count': 1},
            {'uploader_key': 'c@d', 'display_name': 'C', 'count': 2},
        ],
        selected_keys=['c@d'],
    )
    assert 'value="c@d" selected' in html
    assert 'value="a@b" selected' not in html


def test_empty_options_returns_blank_dropdown():
    # When there are no uploaders, the dropdown renders empty so the
    # home page doesn't sprout an orphan form.
    assert qf.render_uploader_filter_dropdown([]) == ''

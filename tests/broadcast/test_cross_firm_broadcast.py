"""Cleanup Item 7: cross-firm broadcast primitive."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import broadcast as bc  # noqa: E402
from src.integrations import notification_sender as ns  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'bc.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (
                firm_code TEXT PRIMARY KEY, name TEXT,
                plan TEXT, subscription_status TEXT DEFAULT 'active'
            );
            CREATE TABLE dashboard_users (
                username TEXT PRIMARY KEY,
                password_hash TEXT, role TEXT, display_name TEXT,
                active INTEGER DEFAULT 1, language TEXT DEFAULT 'fr',
                firm_code TEXT
            );
        """)
        # Two firms, two owners, a firm_admin, a cancelled firm.
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan, subscription_status) "
            "VALUES ('FIRM_A', 'Firm Alpha', 'pro_monthly', 'active')"
        )
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan, subscription_status) "
            "VALUES ('FIRM_B', 'Firm Beta', 'starter_monthly', 'active')"
        )
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan, subscription_status) "
            "VALUES ('FIRM_GHOST', 'Firm Ghost', 'pro_yearly', 'cancelled')"
        )
        # Users
        conn.execute(
            "INSERT INTO dashboard_users "
            "(username, password_hash, role, display_name, language, firm_code) "
            "VALUES ('alpha@a.com','x','owner','Alpha Owner','en','FIRM_A')"
        )
        conn.execute(
            "INSERT INTO dashboard_users "
            "(username, password_hash, role, display_name, language, firm_code) "
            "VALUES ('beta@b.com','x','owner','Béta Owner','fr','FIRM_B')"
        )
        conn.execute(
            "INSERT INTO dashboard_users "
            "(username, password_hash, role, display_name, language, firm_code) "
            "VALUES ('a_admin@a.com','x','firm_admin','Admin Alpha','en','FIRM_A')"
        )
        conn.execute(
            "INSERT INTO dashboard_users "
            "(username, password_hash, role, display_name, language, firm_code) "
            "VALUES ('ghost@g.com','x','owner','Ghost','en','FIRM_GHOST')"
        )
        conn.commit()
    ns.ensure_sender_schema(db)
    return db


def _queue_rows(db):
    with sqlite3.connect(db) as c:
        return c.execute(
            "SELECT recipient_email, subject, body, title, priority, kind "
            "FROM client_notifications"
        ).fetchall()


def test_broadcast_fanout_to_all_firm_owners(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='all_firm_owners',
        subject_en='EN subj', subject_fr='FR subj',
        body_en='Hello {name}', body_fr='Bonjour {name}',
        from_user='sam@firm.com',
    )
    # Cancelled firm owner skipped → 2 recipients (alpha + beta).
    assert result['recipient_count'] == 2
    recipients = set(result['recipients'])
    assert recipients == {'alpha@a.com', 'beta@b.com'}
    rows = _queue_rows(db)
    assert len(rows) == 2
    # Each row in recipient's language
    by_email = {r[0]: r for r in rows}
    assert by_email['alpha@a.com'][1] == 'EN subj'
    assert 'Hello Alpha Owner' in by_email['alpha@a.com'][2]
    assert by_email['beta@b.com'][1] == 'FR subj'
    assert 'Bonjour Béta Owner' in by_email['beta@b.com'][2]


def test_broadcast_requires_both_subjects():
    with pytest.raises(ValueError):
        bc.broadcast(
            Path('/tmp/nope.db'), audience='all_users',
            subject_en='', subject_fr='fr',
            body_en='b', body_fr='b', from_user='s',
        )
    with pytest.raises(ValueError):
        bc.broadcast(
            Path('/tmp/nope.db'), audience='all_users',
            subject_en='en', subject_fr='',
            body_en='b', body_fr='b', from_user='s',
        )


def test_broadcast_unknown_audience_raises(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(ValueError):
        bc.broadcast(
            db, audience='all_aliens',
            subject_en='s', subject_fr='s',
            body_en='b', body_fr='b', from_user='s',
        )


def test_broadcast_specific_firms_requires_codes(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(ValueError):
        bc.broadcast(
            db, audience='specific_firms',
            subject_en='s', subject_fr='s',
            body_en='b', body_fr='b', from_user='s',
            firm_codes=[],
        )


def test_broadcast_specific_firms_scopes(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='specific_firms',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam',
        firm_codes=['FIRM_A'],
    )
    # Only the two users in FIRM_A should receive (owner + admin).
    assert set(result['recipients']) == {'alpha@a.com', 'a_admin@a.com'}


def test_broadcast_plan_tier_filter(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='plan_tier', plan_tier='pro',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam',
    )
    # Only FIRM_A is pro_monthly (active). FIRM_GHOST is pro_yearly
    # but cancelled → skipped. FIRM_B is starter → skipped.
    assert set(result['recipients']) == {'alpha@a.com', 'a_admin@a.com'}


def test_broadcast_all_firm_admins(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='all_firm_admins',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam',
    )
    assert set(result['recipients']) == {'a_admin@a.com'}


def test_broadcast_all_users_includes_every_active(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='all_users',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam',
    )
    # Excludes cancelled firm owner (ghost) and inactive users.
    assert set(result['recipients']) == {
        'alpha@a.com', 'beta@b.com', 'a_admin@a.com',
    }


def test_broadcast_respects_user_language(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='all_firm_owners',
        subject_en='EN', subject_fr='FR',
        body_en='en body', body_fr='fr body', from_user='s',
    )
    rows = _queue_rows(db)
    by_email = {r[0]: r for r in rows}
    assert by_email['alpha@a.com'][1] == 'EN'
    assert by_email['beta@b.com'][1] == 'FR'


def test_preview_recipient_count_returns_sample(tmp_path):
    db = _mkdb(tmp_path)
    preview = bc.preview_recipient_count(
        db, audience='all_firm_owners',
    )
    assert preview['count'] == 2
    assert len(preview['sample']) == 2


def test_broadcast_audit_logged(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='all_firm_owners',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam@firm.com',
    )
    hist = bc.recent_broadcasts(db, limit=5)
    assert len(hist) == 1
    assert hist[0]['batch_id'] == result['batch_id']
    assert hist[0]['recipient_count'] == 2
    assert hist[0]['from_user'] == 'sam@firm.com'


def test_broadcast_scheduled_for_future(tmp_path):
    db = _mkdb(tmp_path)
    future = '2026-12-31T00:00:00+00:00'
    result = bc.broadcast(
        db, audience='all_firm_owners',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam',
        scheduled_for=future,
    )
    with sqlite3.connect(db) as c:
        send_ats = [r[0] for r in c.execute(
            "SELECT send_at FROM client_notifications"
        ).fetchall()]
    assert all(s == future for s in send_ats)
    hist = bc.recent_broadcasts(db, limit=5)
    assert hist[0]['scheduled_for'] == future


def test_broadcast_metadata_carries_batch_id(tmp_path):
    db = _mkdb(tmp_path)
    result = bc.broadcast(
        db, audience='all_firm_owners',
        subject_en='s', subject_fr='s',
        body_en='b', body_fr='b', from_user='sam',
    )
    rows = _queue_rows(db)
    titles = [r[3] for r in rows]
    assert all(result['batch_id'] in t for t in titles)
    # Metadata contains 'cross_firm_broadcast' marker
    assert any('cross_firm_broadcast' in t for t in titles)


def test_audience_options_exported():
    assert 'all_firm_owners' in bc.BROADCAST_AUDIENCES
    assert 'specific_firms' in bc.BROADCAST_AUDIENCES
    assert 'plan_tier' in bc.BROADCAST_AUDIENCES


def test_render_broadcast_page_structure(tmp_path):
    db = _mkdb(tmp_path)
    html = bc.render_broadcast_page(
        db,
        firm_codes_available=[{'firm_code': 'FIRM_A', 'name': 'Alpha'}],
        preview=None,
    )
    assert 'Broadcast' in html
    assert 'name="subject_en"' in html
    assert 'name="subject_fr"' in html
    assert 'name="audience"' in html
    assert 'FIRM_A' in html

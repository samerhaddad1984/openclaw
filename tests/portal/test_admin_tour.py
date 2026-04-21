"""Cleanup Item 3: admins see a 4th tour screen (team management)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


_TOK = 'tok_longenough_admintourtest_0123456789abcdef'


def test_contributor_sees_3_screens():
    assert mup.portal_tour_total_for_role('contributor') == 3
    # step=4 clamps back to 3 for contributors
    html = mup.render_portal_user_tour(
        4, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='contributor',
    )
    assert 'Step 3 of 3' in html
    # No invite CTA on contributor
    assert 'tour-invite-cta' not in html


def test_admin_sees_4_screens():
    assert mup.portal_tour_total_for_role('admin') == 4
    html = mup.render_portal_user_tour(
        4, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='admin',
        firm_client_display='Construction Tremblay at Sam CPA',
    )
    assert 'Step 4 of 4' in html
    assert 'Manage your team' in html
    # Invite CTA rendered on step 4 for admin
    assert 'tour-invite-cta' in html
    assert f'/cp/{_TOK}/admin' in html


def test_admin_team_management_screen_en():
    html = mup.render_portal_user_tour(
        4, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='admin',
        firm_client_display='Construction Tremblay at Sam CPA',
    )
    assert 'As an admin' in html
    assert 'Construction Tremblay' in html
    assert 'Invite your first colleague' in html
    # FR strings must NOT leak
    assert 'admin pour' not in html


def test_admin_team_management_screen_fr():
    html = mup.render_portal_user_tour(
        4, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='fr', role='admin',
        firm_client_display='Construction Tremblay chez Sam CPA',
    )
    assert "admin pour" in html
    assert 'Inviter votre premier' in html
    # EN strings must NOT leak
    assert 'As an admin' not in html
    assert 'Invite your first colleague' not in html


def test_admin_invite_cta_links_to_admin_page():
    html = mup.render_portal_user_tour(
        4, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='admin',
    )
    assert f'href="/cp/{_TOK}/admin"' in html


def test_admin_step_4_has_finish_not_next():
    html = mup.render_portal_user_tour(
        4, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='admin',
    )
    assert 'Get started' in html
    assert 'Next &rarr;' not in html
    assert 'tour/complete' in html


def test_admin_step_3_still_has_next_pointing_to_4():
    html = mup.render_portal_user_tour(
        3, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='admin',
    )
    assert 'Step 3 of 4' in html
    # step 3 for admin must NOT show Finish; the Finish is on step 4
    assert 'Get started' not in html
    assert 'Next &rarr;' in html
    # "tour/complete" appears in the Skip button but NOT in the Next CTA
    # -- safer: Next arrow points to step 4
    assert f'/cp/{_TOK}/tour/4?lang=en' in html


def test_contributor_step_3_still_has_finish():
    html = mup.render_portal_user_tour(
        3, user_name='Alice', firm_name='Sam CPA',
        user_token=_TOK, lang='en', role='contributor',
    )
    assert 'Step 3 of 3' in html
    assert 'Get started' in html  # Finish button label
    assert 'Next &rarr;' not in html


def test_tour_completion_tracks_role_shown(tmp_path):
    # The completion helper is unchanged; this test just pins that
    # the role-argument change didn't accidentally break the DB path.
    import sqlite3
    db = tmp_path / 'roletour.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                portal_mode TEXT DEFAULT 'multi'
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                role TEXT NOT NULL, user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                invited_by TEXT, invited_at TEXT, accepted_at TEXT,
                last_active_at TEXT, upload_count INTEGER DEFAULT 0,
                suspended_at TEXT, removed_at TEXT, version INTEGER DEFAULT 1,
                first_tour_completed_at TEXT,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER, firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT, detail TEXT,
                ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute("INSERT INTO firms VALUES ('F','Sam')")
        conn.execute("INSERT INTO clients (client_code, firm_code) VALUES ('C','F')")
        conn.commit()
    u = mup.create_user_direct(
        db, firm_code='F', client_code='C',
        email='admin@c.com', full_name='Admin',
        role='admin', invited_by='cpa@firm.com', status='active',
    )
    assert mup.portal_user_tour_completed(db, user_id=u['id']) is False
    mup.mark_portal_user_tour_completed(db, user_id=u['id'])
    assert mup.portal_user_tour_completed(db, user_id=u['id']) is True


def test_role_total_admin_vs_contributor():
    assert mup.portal_tour_total_for_role('admin') == 4
    assert mup.portal_tour_total_for_role('contributor') == 3
    assert mup.portal_tour_total_for_role(None) == 3  # default → contributor


def test_role_case_insensitive():
    assert mup.portal_tour_total_for_role('ADMIN') == 4
    assert mup.portal_tour_total_for_role('Admin') == 4

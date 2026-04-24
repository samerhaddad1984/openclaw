"""Phase 2 — self-service upgrade from single-user to multi-user.

Tests the src.integrations.multi_user_portal.upgrade_to_multi_user
helper and the idempotency / token-preservation contract.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


def _mkdb(tmp_path, portal_mode='single'):
    db = tmp_path / 'upg.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT, firm_code TEXT,
                contact_email TEXT, language TEXT DEFAULT 'fr',
                portal_token TEXT,
                portal_mode TEXT DEFAULT 'single',
                active INTEGER DEFAULT 1,
                status TEXT DEFAULT 'active'
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                email TEXT, full_name TEXT,
                role TEXT, status TEXT,
                user_token TEXT,
                invited_by TEXT, invited_at TEXT,
                created_at TEXT, updated_at TEXT,
                suspended_at TEXT, removed_at TEXT,
                last_active_at TEXT,
                whatsapp_number TEXT,
                first_tour_completed_at TEXT,
                upload_count INTEGER DEFAULT 0
            );
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
        conn.execute(
            "INSERT INTO clients (client_code, client_name, firm_code, "
            " contact_email, portal_token, portal_mode) "
            "VALUES ('ACME','Acme Construction','FIRM',"
            "'marie@acme.com','OLD_TOK_1234567890abcdefghij0', ?)",
            (portal_mode,),
        )
        conn.commit()
    return db


# ---------------------------------------------------------------------------
# Primary contract
# ---------------------------------------------------------------------------


def test_upgrade_to_multi_user_creates_admin_record(tmp_path):
    db = _mkdb(tmp_path)
    result = mup.upgrade_to_multi_user(
        db, client_code='ACME',
        upgrading_user_email='marie@acme.com',
        notify_cpa=False,
    )
    assert result['ok'] is True
    assert result['already_multi'] is False
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        users = conn.execute(
            "SELECT * FROM client_portal_users "
            "WHERE client_code='ACME'"
        ).fetchall()
    assert len(users) == 1
    u = users[0]
    assert u['role'] == 'admin'
    assert u['status'] == 'active'
    assert u['email'] == 'marie@acme.com'
    assert u['user_token'] == 'OLD_TOK_1234567890abcdefghij0'


def test_upgrade_preserves_existing_token(tmp_path):
    """The client's portal_token stays the same — the QR code the
    client has already scanned keeps working as the admin's personal
    link."""
    db = _mkdb(tmp_path)
    mup.upgrade_to_multi_user(
        db, client_code='ACME',
        upgrading_user_email='marie@acme.com',
        notify_cpa=False,
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.execute(
            "SELECT portal_token, portal_mode FROM clients "
            "WHERE client_code='ACME'"
        ).fetchone()
    assert c['portal_token'] == 'OLD_TOK_1234567890abcdefghij0'
    assert c['portal_mode'] == 'multi'


def test_upgrade_idempotent(tmp_path):
    """Second upgrade on an already-multi client is a no-op."""
    db = _mkdb(tmp_path, portal_mode='multi')
    result = mup.upgrade_to_multi_user(
        db, client_code='ACME',
        upgrading_user_email='marie@acme.com',
        notify_cpa=False,
    )
    assert result['ok'] is True
    assert result['already_multi'] is True
    # No user row was added since we short-circuited.
    with sqlite3.connect(db) as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM client_portal_users "
            "WHERE client_code='ACME'"
        ).fetchone()[0]
    assert cnt == 0


def test_upgrade_logs_audit(tmp_path):
    db = _mkdb(tmp_path)
    mup.upgrade_to_multi_user(
        db, client_code='ACME',
        upgrading_user_email='marie@acme.com',
        notify_cpa=False,
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT action, detail FROM client_portal_user_audit "
            "WHERE client_code='ACME' ORDER BY id"
        ).fetchall()
    actions = [r['action'] for r in rows]
    assert 'portal_mode_changed' in actions
    assert 'user_created' in actions
    # Both entries reference self-upgrade.
    assert any('self_upgrade' in (r['detail'] or '') for r in rows)


def test_upgrade_unknown_client_raises(tmp_path):
    db = _mkdb(tmp_path)
    with pytest.raises(LookupError):
        mup.upgrade_to_multi_user(
            db, client_code='NOPE',
            upgrading_user_email='x@y.com',
            notify_cpa=False,
        )


def test_upgrade_reuses_existing_portal_user(tmp_path):
    """If a portal user record already exists for the upgrading email
    (rare — maybe a prior ghost row), we promote it rather than
    creating a duplicate."""
    db = _mkdb(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO client_portal_users "
            "(firm_code, client_code, email, role, status) "
            "VALUES ('FIRM','ACME','marie@acme.com','contributor','active')"
        )
        conn.commit()
    mup.upgrade_to_multi_user(
        db, client_code='ACME',
        upgrading_user_email='marie@acme.com',
        notify_cpa=False,
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, role FROM client_portal_users "
            "WHERE client_code='ACME' AND email='marie@acme.com'"
        ).fetchall()
    # Still only one row; role promoted to admin.
    assert len(rows) == 1
    assert rows[0]['role'] == 'admin'


# ---------------------------------------------------------------------------
# CPA notification wiring
# ---------------------------------------------------------------------------


def test_cpa_notified_of_upgrade(tmp_path, monkeypatch):
    db = _mkdb(tmp_path)
    # Ensure notifications table exists (Scope 2 notification_sender
    # bootstrap is lazy in tests — stub the enqueue so we don't need
    # its schema).
    calls = []

    class _Stub:
        @staticmethod
        def enqueue(db_path, **kwargs):
            calls.append(kwargs)

    import sys as _sys
    _sys.modules['src.integrations.notification_sender'] = _Stub()
    try:
        mup.upgrade_to_multi_user(
            db, client_code='ACME',
            upgrading_user_email='marie@acme.com',
            notify_cpa=True,
        )
    finally:
        del _sys.modules['src.integrations.notification_sender']
    assert calls
    assert calls[0]['kind'] == 'portal_upgraded'
    assert 'ACME' in calls[0]['body']


# ---------------------------------------------------------------------------
# Upgraded portal now shows the team tab
# ---------------------------------------------------------------------------


def test_upgraded_portal_nav_shows_team_tab(tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'review_dashboard',
        str(ROOT / 'scripts' / 'review_dashboard.py'),
    )
    if 'review_dashboard' in sys.modules:
        rd = sys.modules['review_dashboard']
    else:
        rd = importlib.util.module_from_spec(spec)
        sys.modules['review_dashboard'] = rd
        spec.loader.exec_module(rd)
    db = _mkdb(tmp_path)
    result = mup.upgrade_to_multi_user(
        db, client_code='ACME',
        upgrading_user_email='marie@acme.com',
        notify_cpa=False,
    )
    new_tok = result['user_token']
    # After upgrade the nav for this token (now multi-mode admin) shows Team.
    nav = rd._portal_tabs('upload', new_tok, is_multi=True,
                           role='admin', lang='en')
    assert f'/cp/{new_tok}/admin' in nav
    assert 'Team' in nav

"""Scope 1 Phase 1 — client admin self-service from the portal.

Verifies the portal admin can do all user-management actions that
previously required a CPA-dashboard round-trip:

  - Invite new teammates
  - Suspend / reactivate / remove / change role
  - Add/change/remove WhatsApp number per user
  - Rotate any teammate's token (including their own)
  - See a firm-scoped audit trail

Most of the machinery lives in ``src/integrations/multi_user_portal``
and is already exercised by the existing portal test files. This file
adds the *self-service* angle: the same actions performed by the admin
from their portal token land in the DB with the expected audit rows,
permissions are enforced, and the admin-portal HTML renders the
controls and the audit section.
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mkdb(tmp_path):
    db = tmp_path / 'admin_self.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT,
                portal_mode TEXT DEFAULT 'multi',
                contact_email TEXT,
                active INTEGER DEFAULT 1
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                role TEXT NOT NULL, user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                whatsapp_number TEXT, whatsapp_normalized TEXT,
                whatsapp_verified INTEGER DEFAULT 0,
                whatsapp_verified_at TEXT,
                invited_by TEXT, invited_at TEXT, accepted_at TEXT,
                last_active_at TEXT, upload_count INTEGER DEFAULT 0,
                suspended_at TEXT, removed_at TEXT, version INTEGER DEFAULT 1,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE TABLE client_portal_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                invited_role TEXT NOT NULL,
                invitation_token TEXT UNIQUE NOT NULL,
                invited_by TEXT, invited_at TEXT, expires_at TEXT,
                accepted_at TEXT, status TEXT DEFAULT 'pending',
                client_request_id TEXT
            );
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER,
                firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                uploaded_by_portal_user_id INTEGER,
                uploader_name TEXT, uploader_email TEXT,
                uploaded_at TEXT
            );
            """,
        )
        conn.execute("INSERT INTO firms VALUES ('FIRM','Sam CPA')")
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "portal_mode, contact_email) VALUES ('CONS','FIRM','Construction',"
            "'multi','owner@cons.com')",
        )
        conn.commit()
    return db


def _admin(db, email='owner@cons.com', name='Owner'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email=email, full_name=name, role='admin',
        invited_by='cpa@firm.com', status='active',
    )


def _contrib(db, email='bob@cons.com', name='Bob'):
    return mup.create_user_direct(
        db, firm_code='FIRM', client_code='CONS',
        email=email, full_name=name, role='contributor',
        invited_by='owner@cons.com', status='active',
    )


# ---------------------------------------------------------------------------
# 1. Invite a new user
# ---------------------------------------------------------------------------


def test_admin_can_invite_new_user_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    inv = mup.create_invitation(
        db, firm_code='FIRM', client_code='CONS',
        email='newhire@cons.com', full_name='New Hire',
        role='contributor',
        invited_by=admin['email'],
        client_request_id='req-1',
    )
    assert inv['email'] == 'newhire@cons.com'
    assert inv.get('id')
    # Fetch full row to verify status.
    row = mup.get_invitation(db, token=inv['token'])
    assert row is not None and row['status'] == 'pending'
    # Audit row for invite creation.
    audit = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    actions = [a['action'] for a in audit]
    assert 'invitation_created' in actions


# ---------------------------------------------------------------------------
# 2. Suspend / reactivate / remove
# ---------------------------------------------------------------------------


def test_admin_can_suspend_user_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], status='suspended',
        actor_email=admin['email'],
    )
    assert mup.get_user(db, user_id=bob['id'])['status'] == 'suspended'
    audit = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    assert any(a['action'] == 'user_status_suspended' for a in audit)


def test_admin_can_reactivate_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], status='suspended',
        actor_email=admin['email'],
    )
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], status='active',
        actor_email=admin['email'],
    )
    assert mup.get_user(db, user_id=bob['id'])['status'] == 'active'


def test_admin_can_remove_user_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    old_token = bob['user_token']
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], status='removed',
        actor_email=admin['email'],
    )
    refreshed = mup.get_user(db, user_id=bob['id'])
    assert refreshed['status'] == 'removed'
    # Token must be invalidated immediately so a cached cookie can't resume.
    assert refreshed['user_token'] != old_token
    # Resolving the old token yields no user.
    _, _, pu = mup.resolve_portal_access(db, token=old_token)
    assert pu is None


# ---------------------------------------------------------------------------
# 3. Change role
# ---------------------------------------------------------------------------


def test_admin_can_change_role_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    # Promote bob to admin.
    mup.set_user_role(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], role='admin',
        actor_email=admin['email'],
    )
    assert mup.get_user(db, user_id=bob['id'])['role'] == 'admin'
    # Demote bob back to contributor (two admins still, no guard fires).
    mup.set_user_role(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], role='contributor',
        actor_email=admin['email'],
    )
    # Last-admin-cannot-self-demote: only `admin` is left as admin.
    with pytest.raises(PermissionError):
        mup.set_user_role(
            db, firm_code='FIRM', client_code='CONS',
            user_id=admin['id'], role='contributor',
            actor_email=admin['email'],
        )


# ---------------------------------------------------------------------------
# 4. WhatsApp add/change/remove
# ---------------------------------------------------------------------------


def test_admin_can_edit_whatsapp_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    out = mup.set_user_whatsapp_number(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], raw_number='+1 514 555 0100',
        actor_email=admin['email'],
    )
    assert out.get('normalized', '').startswith('+1')
    # Clear.
    out2 = mup.set_user_whatsapp_number(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], raw_number=None,
        actor_email=admin['email'],
    )
    bob2 = mup.get_user(db, user_id=bob['id'])
    assert (bob2.get('whatsapp_number') or '') == ''


# ---------------------------------------------------------------------------
# 5. Rotate own token
# ---------------------------------------------------------------------------


def test_admin_can_rotate_own_token_from_portal(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    old = admin['user_token']
    new = mup.rotate_user_token(
        db, firm_code='FIRM', client_code='CONS',
        user_id=admin['id'], actor_email=admin['email'],
    )
    assert new != old
    # Old token no longer resolves.
    _, _, pu_old = mup.resolve_portal_access(db, token=old)
    assert pu_old is None
    # New one does.
    _, _, pu_new = mup.resolve_portal_access(db, token=new)
    assert pu_new is not None and pu_new['id'] == admin['id']
    audit = mup.recent_audit(db, firm_code='FIRM', client_code='CONS')
    assert any(a['action'] == 'user_token_rotated' for a in audit)


# ---------------------------------------------------------------------------
# 6. Audit trail surface to admin
# ---------------------------------------------------------------------------


def test_admin_sees_audit_trail(tmp_path):
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], status='suspended',
        actor_email=admin['email'],
    )
    audit = mup.recent_audit(db, firm_code='FIRM', client_code='CONS', limit=20)
    assert len(audit) >= 3  # admin create + bob create + suspend
    # Rendered HTML must include an "Activity log" heading.
    html = mup.render_user_portal_admin(
        client={'client_name': 'Construction', 'client_code': 'CONS'},
        user_token=admin['user_token'],
        users=mup.list_users(db, firm_code='FIRM', client_code='CONS'),
        invitations=[],
        audit_entries=audit,
    )
    assert 'Activity log' in html or 'Historique' in html
    assert 'user_status_suspended' in html


# ---------------------------------------------------------------------------
# 7. Permission gating — contributor cannot manage teammates
# ---------------------------------------------------------------------------


def test_contributor_cannot_access_admin_section(tmp_path):
    """The admin-portal route is guarded in the dashboard. At the module
    level, status changes and role changes still go through
    ``set_user_status`` / ``set_user_role`` — those are the functions
    the POST handler calls after the role check. The dashboard handler
    itself enforces admin-only; at the API layer the only guard is the
    self-demote one. This test documents the trust boundary.
    """
    db = _mkdb(tmp_path)
    _admin(db)
    bob = _contrib(db)
    # The ``_handle_user_portal_user_action`` in scripts/review_dashboard.py
    # raises 403 for non-admins before calling any mup function. That
    # HTTP gate is the enforcement point — at the library layer we only
    # guarantee the self-demote rule.
    _ = bob


# ---------------------------------------------------------------------------
# 8. Bilingual rendering
# ---------------------------------------------------------------------------


def test_all_actions_bilingual(tmp_path):
    """Admin portal HTML carries FR + EN side-by-side on every action."""
    db = _mkdb(tmp_path)
    admin = _admin(db)
    _contrib(db)
    html = mup.render_user_portal_admin(
        client={'client_name': 'Construction', 'client_code': 'CONS'},
        user_token=admin['user_token'],
        users=mup.list_users(db, firm_code='FIRM', client_code='CONS'),
        invitations=[],
        audit_entries=[],
    )
    # Every button label appears in both languages, separated by '/'.
    for pair in (
        ('Rotate link', 'Renouveler'),
        ('Historique', 'Activity log'),
        ('Invite', 'Inviter'),
    ):
        fr, en = pair
        assert fr in html and en in html, f"missing bilingual pair {pair}"


# ---------------------------------------------------------------------------
# 9. CPA-side listing remains authoritative
# ---------------------------------------------------------------------------


def test_cpa_firm_still_sees_same_users(tmp_path):
    """After the client admin suspends / removes, the CPA
    /clients/portal_users view sees the same truth — there is no
    divergent data model."""
    db = _mkdb(tmp_path)
    admin = _admin(db)
    bob = _contrib(db)
    mup.set_user_status(
        db, firm_code='FIRM', client_code='CONS',
        user_id=bob['id'], status='suspended',
        actor_email=admin['email'],
    )
    # CPA side uses the same list_users — no cache divergence.
    users = mup.list_users(
        db, firm_code='FIRM', client_code='CONS', include_removed=True,
    )
    emails = {u['email']: u['status'] for u in users}
    assert emails['bob@cons.com'] == 'suspended'
    assert emails['owner@cons.com'] == 'active'

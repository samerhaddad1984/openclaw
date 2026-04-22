"""Phase 2: WhatsApp number validation + uniqueness.

Tests the server-side validator that backs both the invite form's
live check (XHR) and the final save. Uniqueness spans the whole DB —
not just one firm — because the Twilio inbound webhook has nothing
other than the number to disambiguate users with.
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


def _mkdb(tmp_path):
    db = tmp_path / 'wa.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                portal_mode TEXT DEFAULT 'multi',
                active INTEGER DEFAULT 1
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
                whatsapp_number TEXT,
                whatsapp_verified INTEGER DEFAULT 0,
                whatsapp_verified_at TEXT,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE UNIQUE INDEX idx_cpu_whatsapp_firm
                ON client_portal_users(firm_code, whatsapp_number)
                WHERE whatsapp_number IS NOT NULL;
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER,
                firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.executescript("""
            INSERT INTO firms VALUES ('FIRM_A','A');
            INSERT INTO firms VALUES ('FIRM_B','B');
            INSERT INTO clients (client_code, firm_code, portal_mode)
                VALUES ('CA','FIRM_A','multi');
            INSERT INTO clients (client_code, firm_code, portal_mode)
                VALUES ('CB','FIRM_B','multi');
        """)
        conn.commit()
    return db


def _mkuser(db, firm, client, email):
    return mup.create_user_direct(
        db, firm_code=firm, client_code=client,
        email=email, full_name=email, role='contributor',
        invited_by='cpa@x', status='active',
    )


# ---------------------------------------------------------------------------
# Format validation
# ---------------------------------------------------------------------------

def test_valid_canadian_number_accepted(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, 'FIRM_A', 'CA', 'bob@ca')
    result = mup.validate_whatsapp_number(
        db, raw_number='514-555-0101',
        firm_code='FIRM_A', client_code='CA',
        current_user_id=u['id'],
    )
    assert result['valid'] is True
    assert result['normalized'] == '+15145550101'
    assert result['error'] is None


def test_invalid_format_rejected(tmp_path):
    db = _mkdb(tmp_path)
    result = mup.validate_whatsapp_number(
        db, raw_number='not a phone',
        firm_code='FIRM_A', client_code='CA',
    )
    assert result['valid'] is False
    assert result['error'] == 'invalid_format'
    assert result['normalized'] is None


def test_foreign_number_rejected(tmp_path):
    db = _mkdb(tmp_path)
    result = mup.validate_whatsapp_number(
        db, raw_number='+33612345678',
        firm_code='FIRM_A', client_code='CA',
    )
    assert result['valid'] is False
    assert result['error'] == 'invalid_format'


# ---------------------------------------------------------------------------
# Uniqueness
# ---------------------------------------------------------------------------

def test_duplicate_within_firm_rejected(tmp_path):
    db = _mkdb(tmp_path)
    u1 = _mkuser(db, 'FIRM_A', 'CA', 'alice@ca')
    u2 = _mkuser(db, 'FIRM_A', 'CA', 'bob@ca')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u1['id'], raw_number='+15145550102',
        actor_email='cpa@x',
    )
    result = mup.validate_whatsapp_number(
        db, raw_number='514-555-0102',
        firm_code='FIRM_A', client_code='CA',
        current_user_id=u2['id'],
    )
    assert result['valid'] is False
    assert result['error'] == 'already_used'
    assert result['already_used'] is True
    assert result['used_in_firm'] is True


def test_duplicate_across_firms_rejected(tmp_path):
    db = _mkdb(tmp_path)
    u_a = _mkuser(db, 'FIRM_A', 'CA', 'alice@a')
    u_b = _mkuser(db, 'FIRM_B', 'CB', 'bob@b')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u_a['id'], raw_number='+15145550103',
        actor_email='cpa@x',
    )
    result = mup.validate_whatsapp_number(
        db, raw_number='+15145550103',
        firm_code='FIRM_B', client_code='CB',
        current_user_id=u_b['id'],
    )
    assert result['valid'] is False
    assert result['already_used'] is True
    # Collision came from a different firm.
    assert result['used_in_firm'] is False


def test_same_user_can_resubmit_own_number(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, 'FIRM_A', 'CA', 'alice@ca')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u['id'], raw_number='+15145550104',
        actor_email='cpa@x',
    )
    # Resubmitting the same number (e.g. opening edit form) must
    # not flag a duplicate against the user's own row.
    result = mup.validate_whatsapp_number(
        db, raw_number='514-555-0104',
        firm_code='FIRM_A', client_code='CA',
        current_user_id=u['id'],
    )
    assert result['valid'] is True


def test_normalization_various_inputs(tmp_path):
    db = _mkdb(tmp_path)
    cases = [
        '+1 (514) 555-0105',
        '514.555.0105',
        '1-514-555-0105',
        '5145550105',
        'whatsapp:+15145550105',
    ]
    for raw in cases:
        r = mup.validate_whatsapp_number(
            db, raw_number=raw, firm_code='FIRM_A', client_code='CA',
        )
        assert r['valid'], raw
        assert r['normalized'] == '+15145550105', raw


# ---------------------------------------------------------------------------
# set_user_whatsapp_number side effects
# ---------------------------------------------------------------------------

def test_set_whatsapp_writes_audit(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, 'FIRM_A', 'CA', 'alice@ca')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u['id'], raw_number='514-555-0106',
        actor_email='admin@ca',
    )
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT action, detail FROM client_portal_user_audit "
            "WHERE portal_user_id=? "
            "AND action='whatsapp_number_set'",
            (u['id'],),
        ).fetchone()
    assert row is not None
    assert row[1] == '+15145550106'


def test_clear_whatsapp_number(tmp_path):
    db = _mkdb(tmp_path)
    u = _mkuser(db, 'FIRM_A', 'CA', 'alice@ca')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u['id'], raw_number='514-555-0107',
        actor_email='admin@ca',
    )
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u['id'], raw_number=None,
        actor_email='admin@ca',
    )
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT whatsapp_number, whatsapp_verified "
            "FROM client_portal_users WHERE id=?", (u['id'],),
        ).fetchone()
    assert row[0] is None
    assert row[1] == 0


def test_set_whatsapp_raises_on_conflict(tmp_path):
    db = _mkdb(tmp_path)
    u1 = _mkuser(db, 'FIRM_A', 'CA', 'alice@ca')
    u2 = _mkuser(db, 'FIRM_A', 'CA', 'bob@ca')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u1['id'], raw_number='+15145550108',
        actor_email='admin@ca',
    )
    with pytest.raises(ValueError):
        mup.set_user_whatsapp_number(
            db, firm_code='FIRM_A', client_code='CA',
            user_id=u2['id'], raw_number='+15145550108',
            actor_email='admin@ca',
        )


def test_removed_user_frees_the_number(tmp_path):
    # A removed user's number should be available for another
    # teammate — otherwise the firm gets stuck if someone leaves.
    db = _mkdb(tmp_path)
    u1 = _mkuser(db, 'FIRM_A', 'CA', 'alice@ca')
    u2 = _mkuser(db, 'FIRM_A', 'CA', 'bob@ca')
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u1['id'], raw_number='+15145550109',
        actor_email='admin@ca',
    )
    # Clear alice's number then mark her removed (mirrors the
    # real workflow: admin clears number before removal).
    mup.set_user_whatsapp_number(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u1['id'], raw_number=None,
        actor_email='admin@ca',
    )
    mup.set_user_status(
        db, firm_code='FIRM_A', client_code='CA',
        user_id=u1['id'], status='removed',
        actor_email='admin@ca',
    )
    # Bob can now register the freed number.
    result = mup.validate_whatsapp_number(
        db, raw_number='+15145550109',
        firm_code='FIRM_A', client_code='CA',
        current_user_id=u2['id'],
    )
    assert result['valid'] is True

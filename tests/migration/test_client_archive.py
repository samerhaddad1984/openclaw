"""Scope 3.1 — client archive / deactivation with retention."""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import client_archive as ca  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'archive.db'
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT,
                firm_code TEXT,
                status TEXT DEFAULT 'active',
                archive_reason TEXT,
                archive_notes TEXT,
                archived_at TEXT,
                archived_by TEXT,
                retention_expires_at TEXT,
                portal_token TEXT
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT,
                email TEXT,
                token TEXT,
                status TEXT DEFAULT 'active'
            );
            INSERT INTO clients (client_code, client_name, firm_code,
                                 status, portal_token)
            VALUES ('CLI1','Client One','FIRM','active','TOK_CLI1'),
                   ('CLI2','Client Two','FIRM','active','TOK_CLI2'),
                   ('OTHR','Other firm','OTHERFIRM','active','TOK_OTHR');
            INSERT INTO client_portal_users (client_code, email, token, status)
            VALUES ('CLI1','alice@c.com','USR_ALICE','active'),
                   ('CLI1','bob@c.com','USR_BOB','active');
            """
        )
        conn.commit()
    ca.ensure_schema(db_path)
    return db_path


def _status(db, code):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return dict(conn.execute(
            "SELECT * FROM clients WHERE client_code=?", (code,)
        ).fetchone())


# ---------------------------------------------------------------------------
# Basic archive flow
# ---------------------------------------------------------------------------


def test_archive_marks_client_archived(db):
    r = ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                          reason=ca.REASON_LEFT_FIRM, actor='owner@firm.com')
    assert r['ok'] is True
    assert r['status'] == ca.STATUS_ARCHIVED
    row = _status(db, 'CLI1')
    assert row['status'] == ca.STATUS_ARCHIVED
    assert row['archive_reason'] == ca.REASON_LEFT_FIRM
    assert row['archived_by'] == 'owner@firm.com'
    assert row['retention_expires_at'] is not None


def test_archive_invalidates_portal_tokens(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_DORMANT, actor='o@f.com')
    row = _status(db, 'CLI1')
    assert row['portal_token'] is None
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        users = conn.execute(
            "SELECT token, status FROM client_portal_users "
            "WHERE client_code='CLI1'"
        ).fetchall()
    assert all(u['token'] is None for u in users)
    assert all(u['status'] == 'revoked' for u in users)


def test_archive_sets_7_year_retention(db):
    before = datetime.now(timezone.utc)
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    row = _status(db, 'CLI1')
    expires = datetime.fromisoformat(row['retention_expires_at'])
    years = (expires - before).days / 365.25
    assert 6.9 < years < 7.1


def test_archive_rejects_invalid_reason(db):
    r = ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                          reason='mystery', actor='o@f.com')
    assert r == {'ok': False, 'reason': 'invalid_reason'}


def test_archive_rejects_wrong_firm(db):
    r = ca.archive_client(db, firm_code='OTHERFIRM', client_code='CLI1',
                          reason=ca.REASON_OTHER, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'wrong_firm'}


def test_archive_rejects_unknown_client(db):
    r = ca.archive_client(db, firm_code='FIRM', client_code='NOPE',
                          reason=ca.REASON_OTHER, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'unknown_client'}


def test_archive_twice_rejected(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    r = ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                          reason=ca.REASON_OTHER, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'already_archived'}


# ---------------------------------------------------------------------------
# Engagement guard
# ---------------------------------------------------------------------------


def _add_period_close(db, client, status='open'):
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS period_close_checklists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, period TEXT, status TEXT
            )
        """)
        conn.execute(
            "INSERT INTO period_close_checklists (client_code, period, status) "
            "VALUES (?,?,?)", (client, '2025-03', status),
        )
        conn.commit()


def test_archive_blocked_when_engagement_open(db):
    _add_period_close(db, 'CLI1', status='open')
    r = ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                          reason=ca.REASON_LEFT_FIRM, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'has_active_engagements'}
    # Still active
    assert _status(db, 'CLI1')['status'] == 'active'


def test_archive_force_bypasses_engagement_guard(db):
    _add_period_close(db, 'CLI1', status='in_progress')
    r = ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                          reason=ca.REASON_LEFT_FIRM, actor='o@f.com',
                          force=True)
    assert r['ok'] is True
    assert r['forced'] is True
    trail = ca.get_audit_trail(db, 'FIRM', 'CLI1')
    assert 'FORCED' in trail[0]['notes']


def test_audit_working_papers_block_archive(db):
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE audit_working_papers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, status TEXT
            )
        """)
        conn.execute(
            "INSERT INTO audit_working_papers (client_code, status) "
            "VALUES ('CLI1','in_progress')"
        )
        conn.commit()
    r = ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                          reason=ca.REASON_LEFT_FIRM, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'has_active_engagements'}


# ---------------------------------------------------------------------------
# Gating uploads
# ---------------------------------------------------------------------------


def test_archived_client_refuses_uploads(db):
    assert ca.can_accept_uploads(db, 'CLI1') is True
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    assert ca.can_accept_uploads(db, 'CLI1') is False


def test_can_accept_uploads_unknown_client_returns_false(db):
    assert ca.can_accept_uploads(db, 'NOPE') is False


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


def test_list_clients_hides_archived_by_default(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    active = ca.list_clients(db, 'FIRM')
    assert [c['client_code'] for c in active] == ['CLI2']


def test_list_clients_include_archived_flag(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    all_ = ca.list_clients(db, 'FIRM', include_archived=True)
    assert sorted(c['client_code'] for c in all_) == ['CLI1', 'CLI2']


def test_list_archived_firm_scoped(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    ca.archive_client(db, firm_code='OTHERFIRM', client_code='OTHR',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    assert [c['client_code']
            for c in ca.list_archived(db, 'FIRM')] == ['CLI1']
    assert [c['client_code']
            for c in ca.list_archived(db, 'OTHERFIRM')] == ['OTHR']


# ---------------------------------------------------------------------------
# Reactivation
# ---------------------------------------------------------------------------


def test_reactivate_restores_active_status(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_DORMANT, actor='o@f.com')
    r = ca.reactivate_client(db, firm_code='FIRM', client_code='CLI1',
                             actor='o@f.com')
    assert r['ok'] is True
    row = _status(db, 'CLI1')
    assert row['status'] == 'active'
    assert row['archive_reason'] is None
    assert row['retention_expires_at'] is None


def test_reactivate_not_archived_rejected(db):
    r = ca.reactivate_client(db, firm_code='FIRM', client_code='CLI1',
                             actor='o@f.com')
    assert r == {'ok': False, 'reason': 'not_archived'}


# ---------------------------------------------------------------------------
# Retention / purge
# ---------------------------------------------------------------------------


def test_retention_window_respected_by_purge_eligible(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    # Fresh archive → not eligible
    assert ca.purge_eligible(db, 'FIRM') == []


def test_purge_eligible_after_7_years(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE clients SET retention_expires_at=? WHERE client_code=?",
            (past, 'CLI1'),
        )
        conn.commit()
    eligible = ca.purge_eligible(db, 'FIRM')
    assert [c['client_code'] for c in eligible] == ['CLI1']


def test_purge_requires_valid_confirm_token(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE clients SET retention_expires_at='2020-01-01T00:00:00+00:00' "
            "WHERE client_code='CLI1'"
        )
        conn.commit()
    r = ca.purge_client(db, firm_code='FIRM', client_code='CLI1',
                        actor='owner@f.com', confirm_token='bogus')
    assert r == {'ok': False, 'reason': 'bad_confirm_token'}


def test_purge_succeeds_after_retention_with_token(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE clients SET retention_expires_at='2020-01-01T00:00:00+00:00' "
            "WHERE client_code='CLI1'"
        )
        conn.commit()
    tok = ca.deterministic_confirm_token('FIRM', 'CLI1')
    r = ca.purge_client(db, firm_code='FIRM', client_code='CLI1',
                        actor='owner@f.com', confirm_token=tok)
    assert r == {'ok': True, 'status': 'purged'}
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        remaining = conn.execute(
            "SELECT 1 FROM clients WHERE client_code=?", ('CLI1',)
        ).fetchone()
    assert remaining is None


def test_purge_blocked_before_retention(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    tok = ca.deterministic_confirm_token('FIRM', 'CLI1')
    r = ca.purge_client(db, firm_code='FIRM', client_code='CLI1',
                        actor='owner@f.com', confirm_token=tok)
    assert r == {'ok': False, 'reason': 'retention_still_active'}


# ---------------------------------------------------------------------------
# Audit trail
# ---------------------------------------------------------------------------


def test_archive_and_reactivate_logged(db):
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_DORMANT, actor='o@f.com',
                      notes='client went quiet')
    ca.reactivate_client(db, firm_code='FIRM', client_code='CLI1',
                         actor='o@f.com', notes='came back')
    trail = ca.get_audit_trail(db, 'FIRM', 'CLI1')
    actions = [e['action'] for e in trail]
    assert 'archived' in actions
    assert 'reactivated' in actions


# ---------------------------------------------------------------------------
# Cross-client reports tolerate archived
# ---------------------------------------------------------------------------


def test_archived_client_data_preserved(db):
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE documents (id INTEGER PRIMARY KEY, client_code TEXT);
            INSERT INTO documents (client_code) VALUES ('CLI1'),('CLI1'),('CLI2');
            """
        )
        conn.commit()
    ca.archive_client(db, firm_code='FIRM', client_code='CLI1',
                      reason=ca.REASON_OTHER, actor='o@f.com')
    # Documents are still there even after archive
    with sqlite3.connect(db) as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE client_code='CLI1'"
        ).fetchone()[0]
    assert cnt == 2

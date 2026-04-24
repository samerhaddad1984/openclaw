"""Scope 3.1 — interaction tests.

Confirms the archive flow doesn't break other features:

  - /clients listings handle archived rows without crashing
  - team workload / uploader reports tolerate archived clients
  - archive requires engagements closed or force
  - active audit blocks archive without force
  - reactivation restores full access
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import client_archive as ca  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'inter.db'
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
                portal_token TEXT,
                primary_employee_email TEXT,
                secondary_employee_email TEXT
            );
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT UNIQUE,
                client_code TEXT,
                review_status TEXT DEFAULT 'Queued',
                uploader_name TEXT
            );
            INSERT INTO clients (client_code, client_name, firm_code,
                                 primary_employee_email)
            VALUES ('ACTIVE','Active Co','FIRM','sam@f.com'),
                   ('ARCH','Archived Co','FIRM','sam@f.com'),
                   ('OTHER','Other','OTHERFIRM',NULL);
            INSERT INTO documents (document_id, client_code, review_status)
            VALUES ('D1','ARCH','Approved'),
                   ('D2','ARCH','Rejected'),
                   ('D3','ACTIVE','Queued');
            """
        )
        conn.commit()
    ca.ensure_schema(db_path)
    ca.archive_client(db_path, firm_code='FIRM', client_code='ARCH',
                      reason=ca.REASON_LEFT_FIRM, actor='o@f.com')
    return db_path


# ---------------------------------------------------------------------------
# Admin dashboard / listings
# ---------------------------------------------------------------------------


def test_admin_dashboard_handles_archived_clients(db):
    # list_clients without include_archived hides ARCH.
    active_only = ca.list_clients(db, 'FIRM')
    codes = [c['client_code'] for c in active_only]
    assert 'ARCH' not in codes
    assert 'ACTIVE' in codes
    # With the flag both show.
    both = ca.list_clients(db, 'FIRM', include_archived=True)
    assert {'ACTIVE', 'ARCH'} <= {c['client_code'] for c in both}


def test_owner_workload_report_handles_archived(db):
    # Simulate the team_workload query shape: it groups per employee
    # and counts clients.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        active = conn.execute(
            "SELECT COUNT(*) FROM clients WHERE firm_code='FIRM' "
            "AND primary_employee_email='sam@f.com' "
            "AND COALESCE(status,'active')='active'"
        ).fetchone()[0]
    # Archive removed ARCH from the default list — workload shouldn't
    # double-count archived clients.
    assert active == 1


def test_documents_uploaded_before_archive_remain_readable(db):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT document_id FROM documents WHERE client_code='ARCH'"
        ).fetchall()
    # Two rows survive archive. This is the audit-trail preservation
    # the retention rule demands.
    assert {r['document_id'] for r in rows} == {'D1', 'D2'}


# ---------------------------------------------------------------------------
# Engagement guard
# ---------------------------------------------------------------------------


def test_archive_requires_engagements_closed_or_force(db):
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE period_close_checklists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, period TEXT, status TEXT
            );
            INSERT INTO period_close_checklists
              (client_code, period, status)
            VALUES ('ACTIVE','2025-03','open');
            """
        )
        conn.commit()
    r = ca.archive_client(db, firm_code='FIRM', client_code='ACTIVE',
                          reason=ca.REASON_LEFT_FIRM, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'has_active_engagements'}
    # Close the engagement
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE period_close_checklists SET status='closed' "
            "WHERE client_code='ACTIVE'"
        )
        conn.commit()
    r2 = ca.archive_client(db, firm_code='FIRM', client_code='ACTIVE',
                           reason=ca.REASON_LEFT_FIRM, actor='o@f.com')
    assert r2['ok'] is True


def test_active_audit_blocks_archive_without_force(db):
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE audit_working_papers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, status TEXT
            );
            INSERT INTO audit_working_papers (client_code, status)
            VALUES ('ACTIVE','in_progress');
            """
        )
        conn.commit()
    r = ca.archive_client(db, firm_code='FIRM', client_code='ACTIVE',
                          reason=ca.REASON_LEFT_FIRM, actor='o@f.com')
    assert r == {'ok': False, 'reason': 'has_active_engagements'}
    # Force bypass — but the trail flags the override.
    r2 = ca.archive_client(db, firm_code='FIRM', client_code='ACTIVE',
                           reason=ca.REASON_LEFT_FIRM, actor='o@f.com',
                           force=True)
    assert r2['ok'] is True
    assert r2['forced'] is True


# ---------------------------------------------------------------------------
# Reactivation restores access
# ---------------------------------------------------------------------------


def test_reactivation_restores_full_access(db):
    # Starting state: ARCH is archived (fixture did it).
    assert ca.can_accept_uploads(db, 'ARCH') is False
    r = ca.reactivate_client(db, firm_code='FIRM', client_code='ARCH',
                             actor='o@f.com')
    assert r['ok'] is True
    assert ca.can_accept_uploads(db, 'ARCH') is True
    # The archive reason is cleared so listings treat it as normal.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, archive_reason, retention_expires_at "
            "FROM clients WHERE client_code='ARCH'"
        ).fetchone()
    assert row['status'] == 'active'
    assert row['archive_reason'] is None
    assert row['retention_expires_at'] is None


# ---------------------------------------------------------------------------
# Upload gate
# ---------------------------------------------------------------------------


def test_archive_blocks_new_uploads_via_can_accept_uploads(db):
    # This is the hook upload_queue consults.
    assert ca.can_accept_uploads(db, 'ACTIVE') is True
    assert ca.can_accept_uploads(db, 'ARCH') is False


def test_upload_queue_refuses_archived_client(db, tmp_path, monkeypatch):
    # Arrange — build a minimal queue-friendly DB shape.
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS documents_tmp (dummy INTEGER);
            """
        )
        conn.commit()
    from src.engines import upload_queue as uq
    upload_dir = tmp_path / 'uploads'
    upload_dir.mkdir()
    # Call into the real save_and_queue_document — the archive gate
    # raises PermissionError before touching disk.
    with pytest.raises(PermissionError, match='archived'):
        uq.save_and_queue_document(
            b'fake', 'test.pdf',
            client_code='ARCH',
            db_path=db, upload_dir=upload_dir,
        )

"""Scope 3.2 interaction tests.

Confirms the OOO flow doesn't break hybrid assignment and that
existing assignments are not altered by OOO activation.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import employee_ooo as ooo  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'inter.db'
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE dashboard_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT, firm_code TEXT, role TEXT,
                active INTEGER DEFAULT 1
            );
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT, firm_code TEXT,
                primary_employee_email TEXT,
                secondary_employee_email TEXT,
                status TEXT DEFAULT 'active'
            );
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT UNIQUE, client_code TEXT,
                review_status TEXT DEFAULT 'Queued'
            );
            CREATE TABLE review_workflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, entity_type TEXT, entity_id TEXT,
                status TEXT, assigned_to_email TEXT,
                priority TEXT DEFAULT 'normal', assigned_at TEXT
            );
            CREATE TABLE review_workflow_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id INTEGER, actor_email TEXT,
                actor_role TEXT, action TEXT,
                from_status TEXT, to_status TEXT, notes TEXT
            );
            INSERT INTO dashboard_users (email, firm_code, role, active)
            VALUES
              ('sam@f.com','FIRM','employee',1),
              ('jean@f.com','FIRM','employee',1),
              ('admin@f.com','FIRM','firm_admin',1);
            INSERT INTO clients (client_code, client_name, firm_code,
                                 primary_employee_email,
                                 secondary_employee_email)
            VALUES ('ACME','Acme','FIRM','sam@f.com','jean@f.com'),
                   ('BETA','Beta','FIRM','sam@f.com','admin@f.com');
            """
        )
        conn.commit()
    ooo.ensure_schema(db_path)
    return db_path


def _insert_doc(db, client, doc_id):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status) "
            "VALUES (?,?,'Queued')",
            (doc_id, client),
        )
        conn.commit()


def test_existing_assignments_preserved_when_ooo_activated(db):
    # Pre-existing workflow — sam owns it.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, assigned_at) "
            "VALUES ('FIRM','document','EX1','in_review','sam@f.com',"
            " '2026-04-20')",
        )
        conn.commit()
    # Turn on OOO; existing assignment should NOT move.
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-25', end_date='2026-05-03',
                created_by='admin@f.com',
                require_coverage_permission=False)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT assigned_to_email FROM review_workflow "
            "WHERE entity_id='EX1'"
        ).fetchone()
    assert row['assigned_to_email'] == 'sam@f.com'


def test_ooo_doesnt_break_existing_hybrid_assignments(db):
    # Force-assign an existing workflow; ensure OOO doesn't rewrite it.
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, assigned_at) VALUES (?,?,?,?,?,?)",
            [
                ('FIRM', 'document', 'EX1', 'assigned', 'sam@f.com',
                 '2026-04-20'),
                ('FIRM', 'document', 'EX2', 'assigned', 'jean@f.com',
                 '2026-04-21'),
            ],
        )
        conn.commit()
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-25', end_date='2026-05-03',
                created_by='admin@f.com',
                require_coverage_permission=False)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        assignments = dict(
            (r['entity_id'], r['assigned_to_email'])
            for r in conn.execute(
                "SELECT entity_id, assigned_to_email FROM review_workflow"
            ).fetchall()
        )
    assert assignments == {'EX1': 'sam@f.com', 'EX2': 'jean@f.com'}


def test_ooo_routes_new_docs_to_coverage(db):
    """auto_assign_new_document consults OOO and routes to coverage."""
    from src.integrations import auto_assign as aa
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-01', end_date='2026-05-31',
                created_by='admin@f.com',
                require_coverage_permission=False)
    _insert_doc(db, 'ACME', 'NEW-1')
    # Bootstrap missing tables auto_assign needs.
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS review_workflow_statuses (
                status TEXT PRIMARY KEY
            );
            """
        )
        conn.commit()
    result = aa.auto_assign_new_document(document_id='NEW-1', db_path=db)
    assert result is not None
    assert result['assigned_to_email'] == 'admin@f.com'
    assert result['reason'] == 'ooo_coverage'


def test_ooo_deactivation_reverts_new_doc_routing(db):
    from src.integrations import auto_assign as aa
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-01', end_date='2026-05-31',
                created_by='admin@f.com',
                require_coverage_permission=False)
    ooo.end_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                ended_by='admin@f.com', reason='cancelled')
    _insert_doc(db, 'ACME', 'NEW-2')
    result = aa.auto_assign_new_document(document_id='NEW-2', db_path=db)
    assert result is not None
    # After cancel, new docs go back to the client's primary.
    assert result['assigned_to_email'] == 'sam@f.com'


def test_bulk_reassignment_separate_from_ooo(db):
    """Explicit bulk_reassign moves existing work; OOO does not."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, assigned_at) "
            "VALUES ('FIRM','document','EX1','in_review','sam@f.com','x')",
        )
        conn.commit()
    r = ooo.bulk_reassign(db, firm_code='FIRM',
                          from_email='sam@f.com',
                          to_email='jean@f.com',
                          actor='admin@f.com')
    assert r == {'ok': True, 'reassigned': 1}
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT assigned_to_email FROM review_workflow "
            "WHERE entity_id='EX1'"
        ).fetchone()
    assert row['assigned_to_email'] == 'jean@f.com'


def test_coverage_can_decline_via_admin(db):
    # Admin "declines" by cancelling the OOO window for sam and
    # re-setting with a different coverage.
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-01', end_date='2026-05-31',
                created_by='admin@f.com',
                require_coverage_permission=False)
    ooo.end_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                ended_by='admin@f.com', reason='cancelled')
    r = ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                    coverage_email='admin@f.com',
                    start_date='2026-04-01', end_date='2026-05-31',
                    created_by='admin@f.com')
    assert r['ok'] is True

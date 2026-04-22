"""Hybrid assignment phase 5: per-document reassignment override.

Covers reassign_document() — the helper the /document/reassign POST
endpoint sits on top of:

- Document-level reassignment overrides the client-level default.
- Owner / firm_admin can reassign any document.
- Currently-assigned employee can hand off to a colleague.
- An employee who is NOT the current assignee cannot reassign.
- review_workflow_audit captures action='reassign' + the reason +
  the previous and new assignees.
- After reassign, the new assignee sees the doc in /my_tasks (the
  primary already-tested visibility surface) and the old assignee
  no longer does.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.review_workflow import (  # noqa: E402
    STATUS_ASSIGNED,
    WorkflowPermissionError,
    assign,
    ensure_review_schema,
    get_workflow,
    my_tasks,
    reassign_document,
)


def _seed(db_path: Path) -> None:
    """Seed a workflow row for D-1 assigned to sophie@f1.com."""
    ensure_review_schema(db_path)
    assign(
        db_path,
        firm_code='F1', entity_type='document', entity_id='D-1',
        assignee_email='sophie@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
    )


@pytest.fixture
def db(tmp_path):
    p = tmp_path / 'd.db'
    _seed(p)
    return p


# ---------------------------------------------------------------------------
# Permission gating
# ---------------------------------------------------------------------------


def test_owner_can_reassign_any_document(db):
    res = reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email='jean@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
        reason='specialization: tax expert',
    )
    assert res['assigned_to_email'] == 'jean@f1.com'


def test_firm_admin_can_reassign_any_document(db):
    res = reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email='jean@f1.com',
        actor_email='marie@f1.com', actor_role='firm_admin',
        reason='balance_workload',
    )
    assert res['assigned_to_email'] == 'jean@f1.com'


def test_current_assignee_employee_can_hand_off(db):
    res = reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email='jean@f1.com',
        actor_email='sophie@f1.com', actor_role='employee',
        reason='help: complex tax credit',
    )
    assert res['assigned_to_email'] == 'jean@f1.com'


def test_employee_cannot_reassign_others_documents(db):
    with pytest.raises(WorkflowPermissionError):
        reassign_document(
            db, firm_code='F1', document_id='D-1',
            new_assignee_email='jean@f1.com',
            actor_email='jean@f1.com', actor_role='employee',  # not current
            reason='trying to grab',
        )


def test_can_return_document_to_pool(db):
    """Empty/None new_assignee unsets the assignment."""
    reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email=None,
        actor_email='sam@f1.com', actor_role='owner',
        reason='no longer relevant',
    )
    wf = get_workflow(db, firm_code='F1', entity_type='document',
                       entity_id='D-1')
    assert wf['assigned_to_email'] is None


# ---------------------------------------------------------------------------
# Audit trail
# ---------------------------------------------------------------------------


def test_reassignment_reason_logged(db):
    reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email='jean@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
        reason='specialization: tax credit eligibility',
    )
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT actor_email, actor_role, action, notes "
            "FROM review_workflow_audit ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    actor_email, actor_role, action, notes = row
    assert action == 'reassign'
    assert actor_email == 'sam@f1.com'
    assert actor_role == 'owner'
    # Notes should carry both previous and new assignees + reason.
    assert 'reason=specialization' in notes
    assert 'prev=sophie@f1.com' in notes
    assert 'new=jean@f1.com' in notes


# ---------------------------------------------------------------------------
# Visibility flow
# ---------------------------------------------------------------------------


def test_document_goes_to_new_assignee_queue(db):
    reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email='jean@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
    )
    jean_tasks = my_tasks(db, assignee_email='jean@f1.com')
    assert any(t['entity_id'] == 'D-1' for t in jean_tasks)


def test_document_leaves_original_assignee_queue(db):
    reassign_document(
        db, firm_code='F1', document_id='D-1',
        new_assignee_email='jean@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
    )
    sophie_tasks = my_tasks(db, assignee_email='sophie@f1.com')
    assert not any(t['entity_id'] == 'D-1' for t in sophie_tasks)


# ---------------------------------------------------------------------------
# Override beats client-default
# ---------------------------------------------------------------------------


def test_document_reassign_overrides_client_default(tmp_path):
    """End-to-end: client primary auto-routes to Sophie; admin
    overrides to Jean; the override sticks even though Sophie remains
    the client primary."""
    db = tmp_path / 'a.db'
    ensure_review_schema(db)
    # Use the higher-level integration: write into clients + run
    # auto_assign, then call reassign.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("""
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                firm_code   TEXT,
                primary_employee_email TEXT,
                secondary_employee_email TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE dashboard_users (
                username TEXT PRIMARY KEY,
                email TEXT, firm_code TEXT, active INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                client_code TEXT,
                file_name   TEXT,
                created_at  TEXT
            )
        """)
        conn.execute("INSERT INTO clients VALUES "
                      "('TREMBLAY','F1','sophie@f1.com',NULL)")
        conn.executemany(
            "INSERT INTO dashboard_users (username,email,firm_code,active) "
            "VALUES (?,?,?,1)",
            [('sophie@f1.com','sophie@f1.com','F1'),
             ('jean@f1.com',  'jean@f1.com',  'F1')],
        )
        conn.execute("INSERT INTO documents VALUES ('D-X','TREMBLAY','x','t')")
        conn.commit()
    finally:
        conn.close()

    from src.integrations.auto_assign import auto_assign_new_document
    auto_assign_new_document(document_id='D-X', db_path=db)
    wf = get_workflow(db, firm_code='F1', entity_type='document',
                       entity_id='D-X')
    assert wf['assigned_to_email'] == 'sophie@f1.com'

    reassign_document(
        db, firm_code='F1', document_id='D-X',
        new_assignee_email='jean@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
        reason='help: tax review',
    )
    wf2 = get_workflow(db, firm_code='F1', entity_type='document',
                        entity_id='D-X')
    assert wf2['assigned_to_email'] == 'jean@f1.com'

    # Re-running auto_assign on the same doc must NOT clobber the
    # override (idempotency tested in Phase 2 too — re-validated here
    # for the override scenario).
    auto_assign_new_document(document_id='D-X', db_path=db)
    wf3 = get_workflow(db, firm_code='F1', entity_type='document',
                        entity_id='D-X')
    assert wf3['assigned_to_email'] == 'jean@f1.com'


def test_reassign_creates_workflow_row_when_missing(tmp_path):
    """If a document was never auto-assigned (no primary on client),
    an admin can still reassign it explicitly."""
    db = tmp_path / 'd.db'
    ensure_review_schema(db)
    res = reassign_document(
        db, firm_code='F1', document_id='D-NEW',
        new_assignee_email='jean@f1.com',
        actor_email='sam@f1.com', actor_role='owner',
        reason='manual pickup',
    )
    assert res['assigned_to_email'] == 'jean@f1.com'
    assert res['status'] == STATUS_ASSIGNED

"""Gap 2 — review queue workflow state machine + role gating."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.review_workflow import (  # noqa: E402
    STATUS_APPROVED,
    STATUS_ESCALATED,
    STATUS_REJECTED,
    STATUS_SUBMITTED,
    WorkflowPermissionError,
    WorkflowStateError,
    approve,
    assign,
    bulk_approve,
    ensure_review_schema,
    escalate,
    get_workflow,
    my_tasks,
    pending_reviews,
    reject,
    requires_review,
    submit_for_review,
)


def _mk(tmp_path):
    db = tmp_path / 'w.db'
    ensure_review_schema(db)
    return db


# --- role gating ---

def test_employee_cannot_post_directly():
    # Policy check: employees must submit-for-review, never post directly.
    assert requires_review(role='employee', entity_type='document') is True
    assert requires_review(role='employee',
                             entity_type='journal_entry',
                             amount=10.0) is True


def test_firm_admin_can_post_below_threshold():
    assert requires_review(role='firm_admin', entity_type='document') is False
    assert requires_review(role='firm_admin',
                             entity_type='journal_entry',
                             amount=100.0) is False


def test_high_value_je_requires_review_even_for_admin():
    # $5,000+ JEs always require review (4-eyes control).
    assert requires_review(role='firm_admin',
                             entity_type='journal_entry',
                             amount=5000.0) is True
    assert requires_review(role='owner',
                             entity_type='journal_entry',
                             amount=-10_000.0) is True


def test_employee_cannot_assign(tmp_path):
    db = _mk(tmp_path)
    with pytest.raises(WorkflowPermissionError):
        assign(db, firm_code='F1', entity_type='document', entity_id='D1',
                assignee_email='junior@f.com',
                actor_email='junior@f.com', actor_role='employee')


def test_employee_cannot_approve(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='junior@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='junior@f.com',
                        actor_role='employee')
    with pytest.raises(WorkflowPermissionError):
        approve(db, firm_code='F1', entity_type='document',
                 entity_id='D1', actor_email='junior@f.com',
                 actor_role='employee')


# --- happy path ---

def test_assign_creates_row_in_assigned_state(tmp_path):
    db = _mk(tmp_path)
    wf = assign(db, firm_code='F1', entity_type='document', entity_id='D1',
                 assignee_email='junior@f.com',
                 actor_email='boss@f.com', actor_role='owner',
                 priority='high')
    assert wf['status'] == 'assigned'
    assert wf['assigned_to_email'] == 'junior@f.com'
    assert wf['priority'] == 'high'


def test_employee_submits_then_owner_approves(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='junior@f.com',
            actor_email='boss@f.com', actor_role='owner')
    wf = submit_for_review(db, firm_code='F1', entity_type='document',
                              entity_id='D1', actor_email='junior@f.com',
                              actor_role='employee', notes='LGTM')
    assert wf['status'] == STATUS_SUBMITTED
    assert wf['submitted_by_email'] == 'junior@f.com'

    wf2 = approve(db, firm_code='F1', entity_type='document', entity_id='D1',
                   actor_email='boss@f.com', actor_role='owner')
    assert wf2['status'] == STATUS_APPROVED
    assert wf2['reviewed_by_email'] == 'boss@f.com'


def test_reject_returns_to_rejected_state_with_reason(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    wf = reject(db, firm_code='F1', entity_type='document', entity_id='D1',
                 actor_email='boss@f.com', actor_role='owner',
                 reason='Wrong GL account')
    assert wf['status'] == STATUS_REJECTED
    assert wf['rejection_reason'] == 'Wrong GL account'


def test_reject_empty_reason_raises(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    with pytest.raises(ValueError):
        reject(db, firm_code='F1', entity_type='document', entity_id='D1',
                actor_email='boss@f.com', actor_role='owner', reason='')


def test_employee_can_revise_after_reject(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    reject(db, firm_code='F1', entity_type='document', entity_id='D1',
            actor_email='boss@f.com', actor_role='owner',
            reason='fix it')
    # Employee re-submits the revision.
    wf = submit_for_review(db, firm_code='F1', entity_type='document',
                              entity_id='D1', actor_email='jr@f.com',
                              actor_role='employee', notes='revised')
    assert wf['status'] == STATUS_SUBMITTED


# --- escalation ---

def test_employee_escalates_sets_urgent(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner',
            priority='normal')
    wf = escalate(db, firm_code='F1', entity_type='document',
                    entity_id='D1', actor_email='jr@f.com',
                    actor_role='employee', notes='need help')
    assert wf['status'] == STATUS_ESCALATED
    assert wf['priority'] == 'urgent'


def test_cannot_escalate_approved(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    approve(db, firm_code='F1', entity_type='document', entity_id='D1',
             actor_email='boss@f.com', actor_role='owner')
    with pytest.raises(WorkflowStateError):
        escalate(db, firm_code='F1', entity_type='document', entity_id='D1',
                   actor_email='jr@f.com', actor_role='employee')


# --- queries ---

def test_my_tasks_returns_assigned_rows(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner',
            priority='high')
    assign(db, firm_code='F1', entity_type='document', entity_id='D2',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner',
            priority='urgent')
    rows = my_tasks(db, assignee_email='jr@f.com')
    # Urgent comes first
    assert rows[0]['entity_id'] == 'D2'
    assert rows[1]['entity_id'] == 'D1'


def test_my_tasks_excludes_approved(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    approve(db, firm_code='F1', entity_type='document', entity_id='D1',
             actor_email='boss@f.com', actor_role='owner')
    assert my_tasks(db, assignee_email='jr@f.com') == []


def test_pending_reviews_shows_submitted_only(tmp_path):
    db = _mk(tmp_path)
    # one submitted, one still assigned
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    assign(db, firm_code='F1', entity_type='document', entity_id='D2',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D2', actor_email='jr@f.com',
                        actor_role='employee')
    pending = pending_reviews(db, firm_code='F1')
    assert [p['entity_id'] for p in pending] == ['D2']


def test_pending_reviews_firm_scoped(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    assign(db, firm_code='F2', entity_type='document', entity_id='D9',
            assignee_email='someone@g.com',
            actor_email='owner@g.com', actor_role='owner')
    submit_for_review(db, firm_code='F2', entity_type='document',
                        entity_id='D9', actor_email='someone@g.com',
                        actor_role='employee')
    assert [p['entity_id'] for p in pending_reviews(db, firm_code='F1')] == ['D1']


# --- bulk + audit ---

def test_bulk_approve_handles_mixed_states(tmp_path):
    db = _mk(tmp_path)
    for eid in ('D1', 'D2', 'D3'):
        assign(db, firm_code='F1', entity_type='document', entity_id=eid,
                assignee_email='jr@f.com',
                actor_email='boss@f.com', actor_role='owner')
    # Only D1 and D3 get submitted — D2 stays assigned.
    for eid in ('D1', 'D3'):
        submit_for_review(db, firm_code='F1', entity_type='document',
                            entity_id=eid, actor_email='jr@f.com',
                            actor_role='employee')

    out = bulk_approve(db, firm_code='F1', entity_type='document',
                        entity_ids=['D1', 'D2', 'D3'],
                        actor_email='boss@f.com', actor_role='owner')
    assert sorted(out['approved']) == ['D1', 'D3']
    assert out['skipped'] == ['D2']


def test_audit_trail_complete(tmp_path):
    db = _mk(tmp_path)
    assign(db, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='jr@f.com',
            actor_email='boss@f.com', actor_role='owner')
    submit_for_review(db, firm_code='F1', entity_type='document',
                        entity_id='D1', actor_email='jr@f.com',
                        actor_role='employee')
    reject(db, firm_code='F1', entity_type='document', entity_id='D1',
            actor_email='boss@f.com', actor_role='owner',
            reason='fix it')
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT action, actor_email, from_status, to_status "
            "FROM review_workflow_audit ORDER BY id"
        ).fetchall()
    assert len(rows) == 3
    assert rows[0] == ('assign', 'boss@f.com', None, 'assigned')
    assert rows[1][0] == 'submit'
    assert rows[2][0] == 'reject'


def test_bootstrap_wiring_grep():
    """Dashboard bootstrap writes review_workflow + audit tables."""
    src = (ROOT / 'scripts' / 'review_dashboard.py').read_text()
    assert 'CREATE TABLE IF NOT EXISTS review_workflow' in src
    assert 'review_workflow_audit' in src


def test_invalid_entity_type_rejected(tmp_path):
    db = _mk(tmp_path)
    with pytest.raises(ValueError):
        assign(db, firm_code='F1', entity_type='weird', entity_id='D1',
                assignee_email='jr@f.com',
                actor_email='boss@f.com', actor_role='owner')

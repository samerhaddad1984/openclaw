"""Review-queue workflow: assign -> in_progress -> submit -> approve /
reject / escalate.

Backing table ``review_workflow`` tracks one row per (entity_type,
entity_id) with a state machine. The module exposes pure Python
functions so HTTP routes + tests call the same surface.

State machine::

    assigned ──► in_progress ──► submitted ──► approved
                                    │   │
                                    │   └──► rejected ─► (back to in_progress)
                                    │
                                    └──► escalated (firm_admin + owner notified)

Role gating (enforced here, not just at route level):

- ``submit_for_review``  — employee, firm_admin, owner
- ``approve`` / ``reject`` — firm_admin, owner
- ``assign`` — firm_admin, owner
- ``escalate`` — employee (raises to firm_admin + owner)
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


STATUS_ASSIGNED = 'assigned'
STATUS_IN_PROGRESS = 'in_progress'
STATUS_SUBMITTED = 'submitted'
STATUS_APPROVED = 'approved'
STATUS_REJECTED = 'rejected'
STATUS_ESCALATED = 'escalated'
STATUS_RETURNED = 'returned'

PRIORITIES = ('low', 'normal', 'high', 'urgent')
VALID_ENTITY_TYPES = ('document', 'journal_entry', 'reconciliation',
                        'tax_return')


class WorkflowPermissionError(Exception):
    """Raised when the caller's role can't perform the requested action."""


class WorkflowStateError(Exception):
    """Raised on invalid state transitions (e.g. approve already-approved)."""


def ensure_review_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_workflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                status TEXT NOT NULL,
                assigned_to_email TEXT,
                submitted_by_email TEXT,
                reviewed_by_email TEXT,
                priority TEXT DEFAULT 'normal',
                assigned_at TEXT,
                submitted_at TEXT,
                reviewed_at TEXT,
                review_notes TEXT,
                rejection_reason TEXT,
                version INTEGER DEFAULT 1,
                UNIQUE(firm_code, entity_type, entity_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_review_workflow_assignee "
            "ON review_workflow(assigned_to_email, status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_review_workflow_reviewer "
            "ON review_workflow(firm_code, status)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_workflow_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id INTEGER,
                actor_email TEXT,
                actor_role TEXT,
                action TEXT,
                from_status TEXT,
                to_status TEXT,
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


def _require_role(role: str, allowed: Iterable[str]) -> None:
    if role not in allowed:
        raise WorkflowPermissionError(
            f"role={role!r} cannot perform this action; needs one of "
            f"{sorted(allowed)}"
        )


def _log_audit(conn: sqlite3.Connection, *, workflow_id: int,
                actor_email: str, actor_role: str, action: str,
                from_status: str | None, to_status: str,
                notes: str | None = None) -> None:
    conn.execute(
        "INSERT INTO review_workflow_audit "
        "(workflow_id, actor_email, actor_role, action, from_status, "
        " to_status, notes) VALUES (?,?,?,?,?,?,?)",
        (workflow_id, actor_email, actor_role, action,
         from_status, to_status, notes),
    )


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------


def get_workflow(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
) -> dict[str, Any] | None:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM review_workflow "
            "WHERE firm_code=? AND entity_type=? AND entity_id=?",
            (firm_code, entity_type, entity_id),
        ).fetchone()
    return dict(row) if row else None


def my_tasks(
    db_path: Path | str,
    *,
    assignee_email: str,
) -> list[dict[str, Any]]:
    """Rows currently assigned to this user and not in a terminal
    state."""
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM review_workflow "
            "WHERE assigned_to_email=? "
            "AND status IN (?, ?, ?, ?) "
            "ORDER BY priority = 'urgent' DESC, priority = 'high' DESC, "
            "         assigned_at ASC",
            (assignee_email, STATUS_ASSIGNED, STATUS_IN_PROGRESS,
             STATUS_REJECTED, STATUS_RETURNED),
        ).fetchall()
    return [dict(r) for r in rows]


def pending_reviews(
    db_path: Path | str,
    *,
    firm_code: str,
) -> list[dict[str, Any]]:
    """Rows submitted for review within a firm."""
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM review_workflow "
            "WHERE firm_code=? AND status IN (?, ?) "
            "ORDER BY priority = 'urgent' DESC, priority = 'high' DESC, "
            "         submitted_at ASC",
            (firm_code, STATUS_SUBMITTED, STATUS_ESCALATED),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Transitions
# ---------------------------------------------------------------------------


def assign(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
    assignee_email: str,
    actor_email: str,
    actor_role: str,
    priority: str = 'normal',
) -> dict[str, Any]:
    _require_role(actor_role, {'firm_admin', 'owner'})
    if entity_type not in VALID_ENTITY_TYPES:
        raise ValueError(f"unknown entity_type {entity_type!r}")
    if priority not in PRIORITIES:
        raise ValueError(f"unknown priority {priority!r}")
    now = _iso_now()
    with _open(db_path) as conn:
        conn.execute(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, priority, assigned_at) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(firm_code, entity_type, entity_id) DO UPDATE SET "
            " assigned_to_email=excluded.assigned_to_email, "
            " priority=excluded.priority, "
            " status=CASE WHEN review_workflow.status='submitted' "
            "             THEN review_workflow.status "
            "             ELSE excluded.status END, "
            " assigned_at=COALESCE(review_workflow.assigned_at, excluded.assigned_at)",
            (firm_code, entity_type, entity_id, STATUS_ASSIGNED,
             assignee_email, priority, now),
        )
        row = conn.execute(
            "SELECT id, status FROM review_workflow "
            "WHERE firm_code=? AND entity_type=? AND entity_id=?",
            (firm_code, entity_type, entity_id),
        ).fetchone()
        _log_audit(conn, workflow_id=row['id'],
                    actor_email=actor_email, actor_role=actor_role,
                    action='assign',
                    from_status=None, to_status=STATUS_ASSIGNED,
                    notes=f'assignee={assignee_email} priority={priority}')
        conn.commit()
    return get_workflow(db_path, firm_code=firm_code,
                          entity_type=entity_type, entity_id=entity_id) or {}


def reassign_document(
    db_path: Path | str,
    *,
    firm_code: str,
    document_id: str,
    new_assignee_email: Optional[str],
    actor_email: str,
    actor_role: str,
    reason: str = '',
) -> dict[str, Any]:
    """Document-level assignment override (Phase 5).

    Allowed when:
      - actor is owner / firm_admin (full power), OR
      - actor is the currently-assigned employee handing the doc off
        to a colleague (ask-for-help flow).

    Empty / None new_assignee_email moves the document back to the
    pool. Always upserts the review_workflow row + writes an audit
    entry with action='reassign' and the reason.
    """
    new_email = (new_assignee_email or '').strip() or None
    actor_email = (actor_email or '').strip()
    if not actor_email:
        raise WorkflowPermissionError('actor_email is required to reassign')
    now = _iso_now()
    with _open(db_path) as conn:
        wf = conn.execute(
            "SELECT id, status, assigned_to_email "
            "FROM review_workflow "
            "WHERE firm_code=? AND entity_type='document' AND entity_id=?",
            (firm_code, document_id),
        ).fetchone()
        prev_email = (wf['assigned_to_email'] or '').strip() if wf else ''
        # Permission: admins always; otherwise actor must be the
        # current assignee. Employees handing off to themselves is a
        # no-op but allowed (no-op short-circuits below).
        if actor_role not in ('owner', 'firm_admin'):
            if not prev_email or prev_email.lower() != actor_email.lower():
                raise WorkflowPermissionError(
                    f"role={actor_role!r} can only reassign documents "
                    f"currently assigned to them"
                )
        if wf is None:
            conn.execute(
                "INSERT INTO review_workflow "
                "(firm_code, entity_type, entity_id, status, "
                " assigned_to_email, priority, assigned_at) "
                "VALUES (?, 'document', ?, ?, ?, 'normal', ?)",
                (firm_code, document_id,
                 STATUS_ASSIGNED if new_email else STATUS_ASSIGNED,
                 new_email, now),
            )
            wf_row = conn.execute(
                "SELECT id FROM review_workflow "
                "WHERE firm_code=? AND entity_type='document' AND entity_id=?",
                (firm_code, document_id),
            ).fetchone()
            wf_id = wf_row['id']
        else:
            wf_id = wf['id']
            conn.execute(
                "UPDATE review_workflow "
                "SET assigned_to_email=?, assigned_at=? "
                "WHERE id=?",
                (new_email, now, wf_id),
            )
        notes_parts = [f"reason={reason}"] if reason else []
        notes_parts.append(
            f"prev={prev_email or 'pool'} new={new_email or 'pool'}"
        )
        _log_audit(
            conn, workflow_id=wf_id, actor_email=actor_email,
            actor_role=actor_role, action='reassign',
            from_status=None, to_status=STATUS_ASSIGNED,
            notes='; '.join(notes_parts),
        )
        conn.commit()
    return get_workflow(db_path, firm_code=firm_code,
                          entity_type='document',
                          entity_id=document_id) or {}


def submit_for_review(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
    actor_email: str,
    actor_role: str,
    notes: str | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
    _require_role(actor_role, {'employee', 'firm_admin', 'owner'})
    now = _iso_now()
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT id, status FROM review_workflow "
            "WHERE firm_code=? AND entity_type=? AND entity_id=?",
            (firm_code, entity_type, entity_id),
        ).fetchone()
        if row is None:
            # Auto-create on first submit (e.g. when employee picks
            # up an unassigned document and submits it).
            conn.execute(
                "INSERT INTO review_workflow "
                "(firm_code, entity_type, entity_id, status, "
                " submitted_by_email, priority, assigned_at, submitted_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (firm_code, entity_type, entity_id, STATUS_SUBMITTED,
                 actor_email, priority or 'normal', now, now),
            )
            wf_id = conn.execute(
                "SELECT id FROM review_workflow "
                "WHERE firm_code=? AND entity_type=? AND entity_id=?",
                (firm_code, entity_type, entity_id),
            ).fetchone()['id']
            _log_audit(conn, workflow_id=wf_id, actor_email=actor_email,
                        actor_role=actor_role, action='submit',
                        from_status=None, to_status=STATUS_SUBMITTED,
                        notes=notes)
            conn.commit()
        else:
            if row['status'] in (STATUS_SUBMITTED, STATUS_APPROVED):
                raise WorkflowStateError(
                    f"cannot submit when status={row['status']}"
                )
            prev = row['status']
            conn.execute(
                "UPDATE review_workflow SET status=?, submitted_by_email=?, "
                " submitted_at=?, review_notes=? WHERE id=?",
                (STATUS_SUBMITTED, actor_email, now, notes, row['id']),
            )
            if priority and priority in PRIORITIES:
                conn.execute(
                    "UPDATE review_workflow SET priority=? WHERE id=?",
                    (priority, row['id']),
                )
            _log_audit(conn, workflow_id=row['id'], actor_email=actor_email,
                        actor_role=actor_role, action='submit',
                        from_status=prev, to_status=STATUS_SUBMITTED,
                        notes=notes)
            conn.commit()
    return get_workflow(db_path, firm_code=firm_code,
                          entity_type=entity_type, entity_id=entity_id) or {}


def approve(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
    actor_email: str,
    actor_role: str,
    notes: str | None = None,
) -> dict[str, Any]:
    _require_role(actor_role, {'firm_admin', 'owner'})
    return _reviewer_transition(
        db_path, firm_code=firm_code, entity_type=entity_type,
        entity_id=entity_id, actor_email=actor_email,
        actor_role=actor_role, to_status=STATUS_APPROVED,
        action='approve', notes=notes,
    )


def reject(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
    actor_email: str,
    actor_role: str,
    reason: str,
) -> dict[str, Any]:
    _require_role(actor_role, {'firm_admin', 'owner'})
    if not reason:
        raise ValueError("rejection requires a non-empty reason")
    return _reviewer_transition(
        db_path, firm_code=firm_code, entity_type=entity_type,
        entity_id=entity_id, actor_email=actor_email,
        actor_role=actor_role, to_status=STATUS_REJECTED,
        action='reject', rejection_reason=reason,
    )


def escalate(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
    actor_email: str,
    actor_role: str,
    notes: str | None = None,
) -> dict[str, Any]:
    _require_role(actor_role, {'employee', 'firm_admin'})
    now = _iso_now()
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT id, status FROM review_workflow "
            "WHERE firm_code=? AND entity_type=? AND entity_id=?",
            (firm_code, entity_type, entity_id),
        ).fetchone()
        if row is None:
            raise WorkflowStateError("cannot escalate: no workflow row exists")
        prev = row['status']
        if prev in (STATUS_APPROVED,):
            raise WorkflowStateError(
                f"cannot escalate when status={prev}"
            )
        conn.execute(
            "UPDATE review_workflow SET status=?, priority='urgent', "
            " review_notes=? WHERE id=?",
            (STATUS_ESCALATED, notes, row['id']),
        )
        _log_audit(conn, workflow_id=row['id'], actor_email=actor_email,
                    actor_role=actor_role, action='escalate',
                    from_status=prev, to_status=STATUS_ESCALATED,
                    notes=notes)
        conn.commit()
    return get_workflow(db_path, firm_code=firm_code,
                          entity_type=entity_type, entity_id=entity_id) or {}


def _reviewer_transition(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_id: str,
    actor_email: str,
    actor_role: str,
    to_status: str,
    action: str,
    notes: str | None = None,
    rejection_reason: str | None = None,
) -> dict[str, Any]:
    now = _iso_now()
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT id, status FROM review_workflow "
            "WHERE firm_code=? AND entity_type=? AND entity_id=?",
            (firm_code, entity_type, entity_id),
        ).fetchone()
        if row is None:
            raise WorkflowStateError("no workflow row to transition")
        prev = row['status']
        if prev not in (STATUS_SUBMITTED, STATUS_ESCALATED):
            raise WorkflowStateError(
                f"cannot {action} from status={prev}"
            )
        conn.execute(
            "UPDATE review_workflow SET status=?, reviewed_by_email=?, "
            " reviewed_at=?, review_notes=COALESCE(?, review_notes), "
            " rejection_reason=? WHERE id=?",
            (to_status, actor_email, now, notes, rejection_reason,
             row['id']),
        )
        _log_audit(conn, workflow_id=row['id'], actor_email=actor_email,
                    actor_role=actor_role, action=action,
                    from_status=prev, to_status=to_status,
                    notes=notes or rejection_reason)
        conn.commit()
    return get_workflow(db_path, firm_code=firm_code,
                          entity_type=entity_type, entity_id=entity_id) or {}


# ---------------------------------------------------------------------------
# Bulk operation
# ---------------------------------------------------------------------------


def bulk_approve(
    db_path: Path | str,
    *,
    firm_code: str,
    entity_type: str,
    entity_ids: Iterable[str],
    actor_email: str,
    actor_role: str,
) -> dict[str, Any]:
    _require_role(actor_role, {'firm_admin', 'owner'})
    approved, skipped = [], []
    for eid in entity_ids:
        try:
            approve(db_path, firm_code=firm_code, entity_type=entity_type,
                     entity_id=eid, actor_email=actor_email,
                     actor_role=actor_role)
            approved.append(eid)
        except (WorkflowStateError, LookupError):
            skipped.append(eid)
    return {'approved': approved, 'skipped': skipped,
             'total': len(approved) + len(skipped)}


# ---------------------------------------------------------------------------
# Policy: require review for high-value JEs
# ---------------------------------------------------------------------------


HIGH_VALUE_REVIEW_THRESHOLD_CAD = 5_000.0


def requires_review(
    *, role: str, entity_type: str, amount: float | None = None,
) -> bool:
    """Returns True when this caller must submit-for-review rather than
    posting directly. Used by post_journal_entry / post_bill routes."""
    if role == 'employee':
        return True
    # firm_admin / owner may post directly UNLESS the amount is above
    # the high-value threshold for a journal entry.
    if entity_type == 'journal_entry' and amount is not None:
        if abs(float(amount)) >= HIGH_VALUE_REVIEW_THRESHOLD_CAD:
            return True
    return False

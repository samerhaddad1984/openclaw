"""Auto-assignment of newly ingested documents.

Hybrid model: each client may have a primary and secondary employee
(``clients.primary_employee_email`` / ``secondary_employee_email``).
When a new document is created — by web upload, public portal, multi-
user portal, WhatsApp, OpenClaw bridge, or any other ingest path —
this module routes it to the client's primary employee (or secondary
if primary is missing/inactive). If neither is set or active the
document stays unassigned and visible in the firm pool.

Hooks:
- ``src/engines/upload_queue.save_and_queue_document`` calls
  ``auto_assign_new_document`` after the placeholder row is written.
- ``src/integrations/whatsapp`` and ``src/integrations/openclaw_bridge``
  call it after ``process_file`` returns a document_id.
- New direct ingest paths should also call it. The function is
  idempotent: if a workflow row already exists for the document,
  it is left untouched (preserves any explicit override).
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional

from src.engines.ocr_engine import DB_PATH as _DEFAULT_DB_PATH
from src.integrations.review_workflow import (
    STATUS_ASSIGNED,
    ensure_review_schema,
)

log = logging.getLogger(__name__)


REASON_AUTO_PRIMARY = "auto_primary"
REASON_AUTO_SECONDARY = "auto_secondary"
REASON_PRIMARY_INACTIVE = "primary_inactive"


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _employee_active(
    conn: sqlite3.Connection, *, firm_code: str, email: Optional[str],
) -> bool:
    """True iff the email belongs to an active dashboard user in the firm.

    Returns False on missing/empty email or if the user row is missing
    or has active=0. Matches by email within firm_code so cross-firm
    impersonation can't slip through.
    """
    if not email:
        return False
    try:
        row = conn.execute(
            "SELECT active FROM dashboard_users "
            "WHERE LOWER(email)=LOWER(?) AND firm_code=?",
            (email, firm_code),
        ).fetchone()
    except sqlite3.OperationalError:
        # dashboard_users might not exist in stripped-down test DBs.
        return False
    if row is None:
        return False
    # active is stored as INTEGER (1/0) in dashboard_users.
    return bool(row[0])


def _existing_workflow(
    conn: sqlite3.Connection, *, firm_code: str, document_id: str,
) -> bool:
    row = conn.execute(
        "SELECT id FROM review_workflow "
        "WHERE firm_code=? AND entity_type='document' AND entity_id=?",
        (firm_code, document_id),
    ).fetchone()
    return row is not None


def auto_assign_new_document(
    *,
    document_id: str,
    db_path: Path | str | None = None,
) -> Optional[dict[str, Any]]:
    """Assign a freshly-ingested document based on its client's primary.

    Returns the chosen assignment row (assigned_to_email + reason) or
    None when the document stays in the firm pool. Safe to call from
    any ingest path; idempotent on re-ingest.

    The function never raises on routine "missing clients table" or
    "missing review_workflow row" cases — those are normal in minimal
    test bootstraps. Caller logs are noise for those paths.
    """
    if not document_id:
        return None
    if db_path is None:
        db_path = _DEFAULT_DB_PATH
    try:
        ensure_review_schema(db_path)
    except sqlite3.OperationalError:
        # If we can't even set up the workflow schema, the DB is in a
        # broken state — let the caller continue without an assignment.
        return None

    with _open(db_path) as conn:
        try:
            doc = conn.execute(
                "SELECT d.client_code, c.firm_code, "
                "       c.primary_employee_email, "
                "       c.secondary_employee_email "
                "FROM documents d "
                "LEFT JOIN clients c ON c.client_code = d.client_code "
                "WHERE d.document_id=?",
                (document_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            return None

        if doc is None:
            return None
        firm_code = doc["firm_code"]
        if not firm_code:
            # Document with no resolvable firm (e.g. UNASSIGNED client
            # with no clients row): nothing to auto-assign against.
            return None

        # Don't clobber an existing assignment / explicit override.
        if _existing_workflow(conn, firm_code=firm_code,
                              document_id=document_id):
            return None

        primary = doc["primary_employee_email"]
        secondary = doc["secondary_employee_email"]

        chosen_email: Optional[str] = None
        reason = REASON_AUTO_PRIMARY
        if _employee_active(conn, firm_code=firm_code, email=primary):
            chosen_email = primary
            reason = REASON_AUTO_PRIMARY
        elif _employee_active(conn, firm_code=firm_code, email=secondary):
            chosen_email = secondary
            reason = (REASON_PRIMARY_INACTIVE
                      if primary else REASON_AUTO_SECONDARY)
        else:
            # No active assignee — leave in pool.
            return None

        from datetime import datetime, timezone
        now = (datetime.now(timezone.utc)
               .replace(microsecond=0).isoformat())
        try:
            conn.execute(
                "INSERT INTO review_workflow "
                "(firm_code, entity_type, entity_id, status, "
                " assigned_to_email, priority, assigned_at) "
                "VALUES (?, 'document', ?, ?, ?, 'normal', ?)",
                (firm_code, document_id, STATUS_ASSIGNED,
                 chosen_email, now),
            )
            wf = conn.execute(
                "SELECT id FROM review_workflow "
                "WHERE firm_code=? AND entity_type='document' AND entity_id=?",
                (firm_code, document_id),
            ).fetchone()
            if wf is not None:
                conn.execute(
                    "INSERT INTO review_workflow_audit "
                    "(workflow_id, actor_email, actor_role, action, "
                    " from_status, to_status, notes) "
                    "VALUES (?, 'system', 'system', 'auto_assign', "
                    " NULL, ?, ?)",
                    (wf["id"], STATUS_ASSIGNED,
                     f"reason={reason} assignee={chosen_email}"),
                )
            conn.commit()
        except sqlite3.IntegrityError:
            # Concurrent ingest already created the row — fine, leave
            # whatever they wrote alone.
            return None
        except sqlite3.OperationalError:
            log.exception("auto_assign_new_document failed for %s",
                          document_id)
            return None

    return {
        "document_id": document_id,
        "firm_code": firm_code,
        "assigned_to_email": chosen_email,
        "reason": reason,
    }

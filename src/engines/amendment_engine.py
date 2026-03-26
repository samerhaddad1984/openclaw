"""
src/engines/amendment_engine.py — Filed-period amendment lifecycle engine.

Handles Traps 1, 4, and 9:
  Trap 1: Filed period remains historically intact. Corrections go to the
           correction period (May), and an amendment-needed flag is raised
           for the filed period (April).
  Trap 4: Recognition timing respects activation dates, not invoice dates.
           Prior treatment contradictions are visible, not silently rewritten.
  Trap 9: Full audit lineage — what was believed, what changed, who approved,
           which version was filed, what contradicted it, whether amendment
           was required, whether rollback happened, current state.

Public interface
----------------
flag_amendment_needed          — raise amendment flag for a filed period
resolve_amendment_flag         — mark flag as resolved after amended filing
take_filing_snapshot           — freeze all docs/postings at filing time
snapshot_document              — capture single doc state at a point in time
get_belief_at_time             — "what was believed about doc X at time T?"
get_amendment_timeline         — full lineage for a client+period
build_period_correction_entry  — create May correction for April error
validate_recognition_timing    — check activation date vs invoice date
update_recognition_period      — set correct recognition period from activation
is_period_filed                — check if a period has been filed
get_filed_period_for_date      — find which filed period a date falls into
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=False)


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row else {}


# =========================================================================
# TRAP 1 — Amendment flag lifecycle
# =========================================================================

def is_period_filed(
    conn: sqlite3.Connection,
    client_code: str,
    period_label: str,
) -> bool:
    """Check whether a period has been filed (gst_filings has a filed_at)."""
    if not _table_exists(conn, "gst_filings"):
        return False
    row = conn.execute(
        """SELECT filed_at FROM gst_filings
           WHERE client_code = ? AND period_label = ?
             AND filed_at IS NOT NULL AND filed_at != ''""",
        (client_code, period_label),
    ).fetchone()
    return row is not None


def get_filed_period_for_date(
    conn: sqlite3.Connection,
    client_code: str,
    document_date: str,
) -> str | None:
    """Return the period_label of the filed period containing document_date.

    Period labels are expected as YYYY-MM.  document_date is YYYY-MM-DD.
    """
    if not document_date or len(document_date) < 7:
        return None
    period_label = document_date[:7]  # YYYY-MM
    if is_period_filed(conn, client_code, period_label):
        return period_label
    return None


def flag_amendment_needed(
    conn: sqlite3.Connection,
    *,
    client_code: str,
    filed_period: str,
    trigger_document_id: str,
    trigger_type: str = "credit_memo",
    reason_en: str = "",
    reason_fr: str = "",
    original_filing_id: str | None = None,
    created_by: str = "system",
) -> dict[str, Any]:
    """Raise an amendment-needed flag for a filed period.

    TRAP 1: The original filing stays intact.  This flag signals that
    a later document contradicts what was filed and an amended return
    may be required.
    """
    if not _table_exists(conn, "amendment_flags"):
        raise RuntimeError("amendment_flags table missing — run migrate_db.py")

    now = _utc_now()
    conn.execute(
        """INSERT INTO amendment_flags
               (client_code, filed_period, trigger_document_id, trigger_type,
                reason_en, reason_fr, original_filing_id, status,
                created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
           ON CONFLICT(client_code, filed_period, trigger_document_id)
           DO UPDATE SET
               reason_en  = excluded.reason_en,
               reason_fr  = excluded.reason_fr,
               updated_at = excluded.updated_at
        """,
        (client_code, filed_period, trigger_document_id, trigger_type,
         reason_en, reason_fr, original_filing_id, now, now),
    )
    conn.commit()

    # Log to audit trail
    if _table_exists(conn, "audit_log"):
        conn.execute(
            """INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
               VALUES (?, ?, ?, ?)""",
            (
                "amendment_flag_raised",
                trigger_document_id,
                _json_dumps({
                    "client_code": client_code,
                    "filed_period": filed_period,
                    "trigger_type": trigger_type,
                    "reason_en": reason_en,
                }),
                now,
            ),
        )
        conn.commit()

    return {
        "status": "amendment_flag_raised",
        "client_code": client_code,
        "filed_period": filed_period,
        "trigger_document_id": trigger_document_id,
        "trigger_type": trigger_type,
    }


def resolve_amendment_flag(
    conn: sqlite3.Connection,
    *,
    client_code: str,
    filed_period: str,
    trigger_document_id: str,
    resolved_by: str,
    amendment_filing_id: str | None = None,
    resolution: str = "amended",
) -> dict[str, Any]:
    """Mark an amendment flag as resolved after the amended return is filed."""
    now = _utc_now()
    conn.execute(
        """UPDATE amendment_flags
           SET status = ?, resolved_by = ?, resolved_at = ?,
               amendment_filing_id = ?, updated_at = ?
           WHERE client_code = ? AND filed_period = ?
             AND trigger_document_id = ?""",
        (resolution, resolved_by, now, amendment_filing_id, now,
         client_code, filed_period, trigger_document_id),
    )
    conn.commit()
    return {
        "status": resolution,
        "resolved_by": resolved_by,
        "amendment_filing_id": amendment_filing_id,
    }


def get_open_amendment_flags(
    conn: sqlite3.Connection,
    client_code: str,
    filed_period: str | None = None,
) -> list[dict[str, Any]]:
    """List open amendment flags for a client, optionally filtered by period."""
    if not _table_exists(conn, "amendment_flags"):
        return []
    if filed_period:
        rows = conn.execute(
            """SELECT * FROM amendment_flags
               WHERE client_code = ? AND filed_period = ? AND status = 'open'
               ORDER BY created_at""",
            (client_code, filed_period),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM amendment_flags
               WHERE client_code = ? AND status = 'open'
               ORDER BY filed_period, created_at""",
            (client_code,),
        ).fetchall()
    return [_row_dict(r) for r in rows]


# =========================================================================
# TRAP 9 — Audit lineage: snapshots & temporal queries
# =========================================================================

def snapshot_document(
    conn: sqlite3.Connection,
    document_id: str,
    *,
    snapshot_type: str = "correction",
    snapshot_reason: str = "",
    taken_by: str = "system",
) -> int:
    """Capture point-in-time state of a document.  Returns snapshot_id."""
    if not _table_exists(conn, "document_snapshots"):
        raise RuntimeError("document_snapshots table missing — run migrate_db.py")

    row = conn.execute(
        "SELECT * FROM documents WHERE document_id = ?", (document_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Document not found: {document_id}")

    state = _row_dict(row)
    now = _utc_now()

    cursor = conn.execute(
        """INSERT INTO document_snapshots
               (document_id, snapshot_type, snapshot_reason, state_json, taken_by, taken_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (document_id, snapshot_type, snapshot_reason, _json_dumps(state), taken_by, now),
    )
    conn.commit()
    return cursor.lastrowid


def snapshot_posting(
    conn: sqlite3.Connection,
    document_id: str,
    *,
    snapshot_type: str = "correction",
    snapshot_reason: str = "",
    taken_by: str = "system",
) -> int | None:
    """Capture point-in-time state of posting job for a document."""
    if not _table_exists(conn, "posting_snapshots"):
        return None
    if not _table_exists(conn, "posting_jobs"):
        return None

    row = conn.execute(
        "SELECT * FROM posting_jobs WHERE document_id = ?", (document_id,)
    ).fetchone()
    if not row:
        return None

    state = _row_dict(row)
    posting_id = state.get("posting_id", "")
    now = _utc_now()

    cursor = conn.execute(
        """INSERT INTO posting_snapshots
               (posting_id, document_id, snapshot_type, snapshot_reason,
                state_json, taken_by, taken_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (posting_id, document_id, snapshot_type, snapshot_reason,
         _json_dumps(state), taken_by, now),
    )
    conn.commit()
    return cursor.lastrowid


def take_filing_snapshot(
    conn: sqlite3.Connection,
    client_code: str,
    period_label: str,
    *,
    filed_by: str = "system",
) -> dict[str, Any]:
    """Freeze the state of all documents and postings in a period at filing time.

    TRAP 9: This creates the "what was believed when we filed" baseline.
    """
    if not _table_exists(conn, "documents"):
        return {"snapshot_count": 0}

    # Find all documents in this period
    rows = conn.execute(
        """SELECT document_id FROM documents
           WHERE client_code = ?
             AND document_date LIKE ?
           ORDER BY document_id""",
        (client_code, f"{period_label}%"),
    ).fetchall()

    snapshot_ids: list[int] = []
    for row in rows:
        doc_id = row["document_id"]
        sid = snapshot_document(
            conn, doc_id,
            snapshot_type="filing",
            snapshot_reason=f"Period {period_label} filed by {filed_by}",
            taken_by=filed_by,
        )
        snapshot_ids.append(sid)
        snapshot_posting(
            conn, doc_id,
            snapshot_type="filing",
            snapshot_reason=f"Period {period_label} filed by {filed_by}",
            taken_by=filed_by,
        )

    return {
        "client_code": client_code,
        "period_label": period_label,
        "snapshot_count": len(snapshot_ids),
        "snapshot_ids": snapshot_ids,
    }


def get_belief_at_time(
    conn: sqlite3.Connection,
    document_id: str,
    as_of: str,
) -> dict[str, Any]:
    """Return what the system believed about a document at a given time.

    TRAP 9: Answers "what was believed about document X on date Y?"
    by finding the most recent snapshot taken before as_of.
    """
    if not _table_exists(conn, "document_snapshots"):
        return {"error": "no snapshot history available"}

    row = conn.execute(
        """SELECT state_json, snapshot_type, snapshot_reason, taken_at
           FROM document_snapshots
           WHERE document_id = ? AND taken_at <= ?
           ORDER BY taken_at DESC
           LIMIT 1""",
        (document_id, as_of),
    ).fetchone()

    if not row:
        return {
            "document_id": document_id,
            "as_of": as_of,
            "belief": None,
            "note": "No snapshot exists before this date",
        }

    return {
        "document_id": document_id,
        "as_of": as_of,
        "belief": json.loads(row["state_json"]),
        "snapshot_type": row["snapshot_type"],
        "snapshot_reason": row["snapshot_reason"],
        "snapshot_taken_at": row["taken_at"],
    }


def get_amendment_timeline(
    conn: sqlite3.Connection,
    client_code: str,
    period_label: str,
) -> dict[str, Any]:
    """Build full audit lineage for a client+period.

    TRAP 9: Answers all the audit questions:
    - what was believed at filing?
    - what changed after filing?
    - who approved what?
    - which version was filed?
    - what later evidence contradicted it?
    - whether amendment was required?
    - whether rollback happened?
    - current state?
    """
    timeline: dict[str, Any] = {
        "client_code": client_code,
        "period_label": period_label,
        "events": [],
    }

    # 1. Filing record
    if _table_exists(conn, "gst_filings"):
        filing = conn.execute(
            """SELECT * FROM gst_filings
               WHERE client_code = ? AND period_label = ?""",
            (client_code, period_label),
        ).fetchone()
        if filing:
            timeline["filing"] = _row_dict(filing)

    # 2. Filing snapshots
    if _table_exists(conn, "document_snapshots"):
        snaps = conn.execute(
            """SELECT ds.* FROM document_snapshots ds
               JOIN documents d ON ds.document_id = d.document_id
               WHERE d.client_code = ?
                 AND d.document_date LIKE ?
                 AND ds.snapshot_type = 'filing'
               ORDER BY ds.taken_at""",
            (client_code, f"{period_label}%"),
        ).fetchall()
        timeline["filing_snapshots"] = [_row_dict(s) for s in snaps]

    # 3. Amendment flags
    if _table_exists(conn, "amendment_flags"):
        flags = conn.execute(
            """SELECT * FROM amendment_flags
               WHERE client_code = ? AND filed_period = ?
               ORDER BY created_at""",
            (client_code, period_label),
        ).fetchall()
        timeline["amendment_flags"] = [_row_dict(f) for f in flags]

    # 4. Correction snapshots (post-filing changes)
    if _table_exists(conn, "document_snapshots"):
        corrections = conn.execute(
            """SELECT ds.* FROM document_snapshots ds
               JOIN documents d ON ds.document_id = d.document_id
               WHERE d.client_code = ?
                 AND d.document_date LIKE ?
                 AND ds.snapshot_type != 'filing'
               ORDER BY ds.taken_at""",
            (client_code, f"{period_label}%"),
        ).fetchall()
        timeline["correction_snapshots"] = [_row_dict(c) for c in corrections]

    # 5. Correction chains involving this period
    if _table_exists(conn, "correction_chains"):
        chains = conn.execute(
            """SELECT cc.* FROM correction_chains cc
               JOIN documents d ON cc.source_document_id = d.document_id
               WHERE cc.client_code = ?
                 AND d.document_date LIKE ?
               ORDER BY cc.created_at""",
            (client_code, f"{period_label}%"),
        ).fetchall()
        timeline["correction_chains"] = [_row_dict(c) for c in chains]

    # 6. Rollback events
    if _table_exists(conn, "rollback_log"):
        rollbacks = conn.execute(
            """SELECT * FROM rollback_log
               WHERE client_code = ?
               ORDER BY created_at""",
            (client_code,),
        ).fetchall()
        timeline["rollbacks"] = [_row_dict(r) for r in rollbacks]

    # 7. Audit log events for this period's documents
    if _table_exists(conn, "audit_log") and _table_exists(conn, "documents"):
        events = conn.execute(
            """SELECT al.* FROM audit_log al
               JOIN documents d ON al.document_id = d.document_id
               WHERE d.client_code = ?
                 AND d.document_date LIKE ?
               ORDER BY al.created_at""",
            (client_code, f"{period_label}%"),
        ).fetchall()
        timeline["audit_events"] = [_row_dict(e) for e in events]

    # 8. Current state of all documents in this period
    if _table_exists(conn, "documents"):
        current = conn.execute(
            """SELECT * FROM documents
               WHERE client_code = ? AND document_date LIKE ?
               ORDER BY document_id""",
            (client_code, f"{period_label}%"),
        ).fetchall()
        timeline["current_state"] = [_row_dict(c) for c in current]

    return timeline


# =========================================================================
# TRAP 1 — Period correction entries (May corrects April)
# =========================================================================

def build_period_correction_entry(
    conn: sqlite3.Connection,
    *,
    original_document_id: str,
    correction_document_id: str,
    client_code: str,
    correction_period: str,
    correction_amount: float,
    correction_gst: float | None = None,
    correction_qst: float | None = None,
    reason_en: str = "",
    reason_fr: str = "",
    created_by: str = "system",
) -> dict[str, Any]:
    """Create correction entries in the correction period (e.g., May) that
    reverse the impact of an error discovered in a filed period (e.g., April).

    TRAP 1: April remains untouched.  The correction entries live in May.
    An amendment flag is raised for April.
    """
    # 1. Snapshot the original document before any correction
    snapshot_document(
        conn, original_document_id,
        snapshot_type="pre_correction",
        snapshot_reason=f"Before correction by {correction_document_id}",
        taken_by=created_by,
    )

    # 2. Look up original document's period
    orig_doc = conn.execute(
        "SELECT document_date, client_code FROM documents WHERE document_id = ?",
        (original_document_id,),
    ).fetchone()
    if not orig_doc:
        raise ValueError(f"Original document not found: {original_document_id}")

    original_date = orig_doc["document_date"] or ""
    original_period = original_date[:7] if len(original_date) >= 7 else ""

    # 3. If original period is filed, raise amendment flag
    if original_period and is_period_filed(conn, client_code, original_period):
        flag_amendment_needed(
            conn,
            client_code=client_code,
            filed_period=original_period,
            trigger_document_id=correction_document_id,
            trigger_type="correction_entry",
            reason_en=reason_en or f"Correction in {correction_period} affects filed period {original_period}",
            reason_fr=reason_fr or f"Correction en {correction_period} affecte la période déclarée {original_period}",
            created_by=created_by,
        )

    # 4. Record correction chain link
    if _table_exists(conn, "correction_chains"):
        now = _utc_now()
        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, link_type, economic_effect,
                    amount, tax_impact_gst, tax_impact_qst,
                    created_by, created_at)
               VALUES (?, ?, ?, ?, 'correction_entry', 'reduction',
                       ?, ?, ?, ?, ?)""",
            (original_document_id, client_code, original_document_id,
             correction_document_id, correction_amount,
             correction_gst, correction_qst, created_by, now),
        )
        conn.commit()

    return {
        "status": "correction_entry_created",
        "original_document_id": original_document_id,
        "original_period": original_period,
        "correction_document_id": correction_document_id,
        "correction_period": correction_period,
        "correction_amount": correction_amount,
        "amendment_flag_raised": bool(
            original_period and is_period_filed(conn, client_code, original_period)
        ),
    }


# =========================================================================
# TRAP 4 — Recognition timing
# =========================================================================

def validate_recognition_timing(
    conn: sqlite3.Connection,
    document_id: str,
) -> dict[str, Any]:
    """Check if a document's recognition period matches its activation date.

    TRAP 4: If monitoring starts in June, it should not be recognized in
    April or May, even if the invoice or email arrived in those months.
    """
    doc = conn.execute(
        "SELECT * FROM documents WHERE document_id = ?", (document_id,)
    ).fetchone()
    if not doc:
        return {"error": f"Document not found: {document_id}"}

    doc = _row_dict(doc)
    document_date = doc.get("document_date", "")
    activation_date = doc.get("activation_date", "")
    recognition_period = doc.get("recognition_period", "")
    recognition_status = doc.get("recognition_status", "immediate")

    result: dict[str, Any] = {
        "document_id": document_id,
        "document_date": document_date,
        "activation_date": activation_date,
        "recognition_period": recognition_period,
        "recognition_status": recognition_status,
        "issues": [],
    }

    if not activation_date:
        # No activation date — default recognition is at document date
        return result

    # Activation date is later than document date → deferred recognition
    if activation_date > document_date:
        result["issues"].append({
            "issue": "deferred_recognition_required",
            "description_en": (
                f"Activation date ({activation_date}) is after document date "
                f"({document_date}). Recognition should be deferred to "
                f"{activation_date[:7]}."
            ),
            "description_fr": (
                f"La date d'activation ({activation_date}) est après la date "
                f"du document ({document_date}). La comptabilisation devrait "
                f"être reportée à {activation_date[:7]}."
            ),
            "correct_recognition_period": activation_date[:7],
        })

        # Check if current recognition contradicts activation
        doc_period = document_date[:7] if len(document_date) >= 7 else ""
        activation_period = activation_date[:7] if len(activation_date) >= 7 else ""

        if recognition_period and recognition_period != activation_period:
            result["issues"].append({
                "issue": "recognition_period_mismatch",
                "description_en": (
                    f"Current recognition period ({recognition_period}) does "
                    f"not match activation period ({activation_period})."
                ),
                "description_fr": (
                    f"La période de comptabilisation actuelle ({recognition_period}) "
                    f"ne correspond pas à la période d'activation ({activation_period})."
                ),
            })

        if doc_period and doc_period != activation_period:
            result["issues"].append({
                "issue": "prior_treatment_contradiction",
                "description_en": (
                    f"Document was dated {doc_period} but activation is {activation_period}. "
                    f"Any recognition in {doc_period} needs correction."
                ),
                "description_fr": (
                    f"Le document est daté {doc_period} mais l'activation est {activation_period}. "
                    f"Toute comptabilisation en {doc_period} nécessite une correction."
                ),
            })

    return result


def update_recognition_period(
    conn: sqlite3.Connection,
    document_id: str,
    activation_date: str,
    *,
    updated_by: str = "system",
) -> dict[str, Any]:
    """Set correct recognition period based on activation date.

    TRAP 4: Does NOT rewrite history.  Sets recognition_status to 'deferred'
    and recognition_period to the activation month.  If the document was
    already posted in a prior period, a correction entry is flagged, not
    a silent rewrite.
    """
    recognition_period = activation_date[:7] if len(activation_date) >= 7 else ""

    # Snapshot before change
    snapshot_document(
        conn, document_id,
        snapshot_type="recognition_update",
        snapshot_reason=f"Activation date set to {activation_date}",
        taken_by=updated_by,
    )

    conn.execute(
        """UPDATE documents
           SET activation_date = ?,
               recognition_period = ?,
               recognition_status = 'deferred'
           WHERE document_id = ?""",
        (activation_date, recognition_period, document_id),
    )
    conn.commit()

    # Check if prior posting exists in a different period
    doc = conn.execute(
        "SELECT document_date, client_code FROM documents WHERE document_id = ?",
        (document_id,),
    ).fetchone()

    result: dict[str, Any] = {
        "document_id": document_id,
        "activation_date": activation_date,
        "recognition_period": recognition_period,
        "recognition_status": "deferred",
        "prior_period_impact": False,
    }

    if doc:
        doc_period = (doc["document_date"] or "")[:7]
        client_code = doc["client_code"] or ""
        if doc_period and doc_period != recognition_period:
            result["prior_period_impact"] = True
            result["impacted_period"] = doc_period
            if is_period_filed(conn, client_code, doc_period):
                result["amendment_needed"] = True
                flag_amendment_needed(
                    conn,
                    client_code=client_code,
                    filed_period=doc_period,
                    trigger_document_id=document_id,
                    trigger_type="recognition_timing",
                    reason_en=f"Recognition deferred to {recognition_period} (activation: {activation_date})",
                    reason_fr=f"Comptabilisation reportée à {recognition_period} (activation: {activation_date})",
                    created_by=updated_by,
                )

    return result

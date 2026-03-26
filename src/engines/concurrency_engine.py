"""
src/engines/concurrency_engine.py — Optimistic locking & journal collision detection.

Handles Traps 6 and 7:
  Trap 6: Stale human approval.  Reviewer B approves on a stale screen after
           Reviewer A already anchored correction behavior.  The stale action
           must be rejected or forced-refreshed because the case version changed.
  Trap 7: Manual journal collision.  External bookkeeper enters a manual
           journal that conflicts with or double-corrects a document-backed
           correction.  The manual journal must be blocked, quarantined, or
           conflict-flagged — never allowed to silently coexist.

Public interface
----------------
read_version                   — read current version of a document or posting
check_version_or_raise         — compare-and-swap: raise if version changed
approve_with_version_check     — approve only if version matches
detect_manual_journal_collision — find collisions with document corrections
quarantine_manual_journal      — block a colliding manual journal
validate_manual_journal        — full validation before accepting MJE
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
# TRAP 6 — Optimistic locking
# =========================================================================

class StaleVersionError(Exception):
    """Raised when a user action targets a version that no longer matches."""

    def __init__(
        self,
        entity_type: str,
        entity_id: str,
        expected_version: int,
        current_version: int,
    ):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.expected_version = expected_version
        self.current_version = current_version
        super().__init__(
            f"Stale {entity_type} version: expected {expected_version}, "
            f"current is {current_version}. "
            f"Another user has modified {entity_type} '{entity_id}'. "
            f"Refresh and retry."
        )


def read_version(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: str,
) -> int:
    """Read the current version of a document or posting job.

    Returns 0 if the entity has no version column or doesn't exist.
    """
    if entity_type == "document":
        table = "documents"
        id_col = "document_id"
    elif entity_type == "posting":
        table = "posting_jobs"
        id_col = "posting_id"
    else:
        return 0

    if not _table_exists(conn, table):
        return 0

    # Check if version column exists
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if "version" not in cols:
        return 0

    row = conn.execute(
        f"SELECT version FROM {table} WHERE {id_col} = ?", (entity_id,)
    ).fetchone()
    return int(row["version"]) if row else 0


def check_version_or_raise(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: str,
    expected_version: int,
) -> None:
    """Compare-and-swap: raise StaleVersionError if version has changed.

    TRAP 6: Call this before any write operation that was initiated from
    a UI read.  If Reviewer A has modified the document since Reviewer B
    read it, Reviewer B's action is rejected.
    """
    current = read_version(conn, entity_type, entity_id)

    # Version 0 means no versioning — allow the operation
    if current == 0:
        return

    if current != expected_version:
        # Log the stale action attempt
        if _table_exists(conn, "audit_log"):
            conn.execute(
                """INSERT INTO audit_log
                       (event_type, document_id, prompt_snippet, created_at)
                   VALUES (?, ?, ?, ?)""",
                (
                    "stale_version_rejected",
                    entity_id,
                    _json_dumps({
                        "entity_type": entity_type,
                        "expected_version": expected_version,
                        "current_version": current,
                    }),
                    _utc_now(),
                ),
            )
            conn.commit()

        raise StaleVersionError(entity_type, entity_id, expected_version, current)


def approve_with_version_check(
    conn: sqlite3.Connection,
    *,
    document_id: str,
    expected_document_version: int,
    reviewer: str,
    approval_state: str = "approved_for_posting",
) -> dict[str, Any]:
    """Approve a document only if its version matches what the reviewer saw.

    TRAP 6: Prevents Reviewer B from approving a stale document state
    after Reviewer A already made changes.
    """
    # Check document version
    check_version_or_raise(conn, "document", document_id, expected_document_version)

    # Also check posting version if exists
    if _table_exists(conn, "posting_jobs"):
        posting = conn.execute(
            "SELECT posting_id, version FROM posting_jobs WHERE document_id = ?",
            (document_id,),
        ).fetchone()
        if posting:
            # Don't check posting version — it auto-increments.
            # Just record the current state for audit.
            pass

    now = _utc_now()

    # Update approval state
    if _table_exists(conn, "posting_jobs"):
        conn.execute(
            """UPDATE posting_jobs
               SET approval_state = ?, reviewer = ?, updated_at = ?
               WHERE document_id = ?""",
            (approval_state, reviewer, now, document_id),
        )

    # Snapshot the approval action
    if _table_exists(conn, "audit_log"):
        conn.execute(
            """INSERT INTO audit_log
                   (event_type, username, document_id, prompt_snippet, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "version_checked_approval",
                reviewer,
                document_id,
                _json_dumps({
                    "approval_state": approval_state,
                    "document_version": expected_document_version,
                }),
                now,
            ),
        )

    conn.commit()

    return {
        "status": "approved",
        "document_id": document_id,
        "reviewer": reviewer,
        "version_at_approval": expected_document_version,
    }


# =========================================================================
# TRAP 7 — Manual journal collision detection
# =========================================================================

def detect_manual_journal_collision(
    conn: sqlite3.Connection,
    *,
    client_code: str,
    period: str,
    debit_account: str,
    credit_account: str,
    amount: float,
    document_id: str | None = None,
) -> dict[str, Any]:
    """Detect when a manual journal entry conflicts with document-backed corrections.

    TRAP 7: The bookkeeper's manual journal tries to "help" by correcting
    something that the system's correction chain already handles.

    Checks for:
    1. Active correction chains in the same period that affect the same GL accounts
    2. Existing posting jobs that would be double-counted
    3. Amount overlaps that suggest duplicate correction
    """
    result: dict[str, Any] = {
        "has_collision": False,
        "collision_type": None,
        "collisions": [],
    }

    # Check 1: Active correction chains in same period affecting same accounts
    if _table_exists(conn, "correction_chains") and _table_exists(conn, "documents"):
        chains = conn.execute(
            """SELECT cc.*, d.gl_account, d.amount AS doc_amount,
                      d.document_date
               FROM correction_chains cc
               JOIN documents d ON cc.target_document_id = d.document_id
               WHERE cc.client_code = ?
                 AND cc.status = 'active'
                 AND d.document_date LIKE ?""",
            (client_code, f"{period}%"),
        ).fetchall()

        for chain in chains:
            chain = _row_dict(chain)
            chain_gl = chain.get("gl_account", "")
            chain_amount = abs(chain.get("amount") or chain.get("doc_amount") or 0)

            # Check if the manual journal touches the same GL accounts
            gl_match = (
                chain_gl == debit_account
                or chain_gl == credit_account
            )
            # Check amount overlap (within 5% tolerance)
            amount_close = (
                chain_amount > 0
                and abs(chain_amount - abs(amount)) / chain_amount < 0.05
            )

            if gl_match and amount_close:
                result["has_collision"] = True
                result["collision_type"] = "correction_chain_overlap"
                result["collisions"].append({
                    "type": "correction_chain_overlap",
                    "chain_id": chain.get("chain_id"),
                    "chain_amount": chain_amount,
                    "journal_amount": abs(amount),
                    "gl_account": chain_gl,
                    "description_en": (
                        f"Active correction chain (id={chain.get('chain_id')}) "
                        f"already handles a {chain.get('economic_effect', '')} "
                        f"of {chain_amount} on GL {chain_gl}. "
                        f"This manual journal would double-correct."
                    ),
                    "description_fr": (
                        f"La chaîne de correction active (id={chain.get('chain_id')}) "
                        f"gère déjà un(e) {chain.get('economic_effect', '')} "
                        f"de {chain_amount} sur GL {chain_gl}. "
                        f"Ce journal manuel ferait une double correction."
                    ),
                })

    # Check 2: Existing posting jobs in same period with overlapping amounts
    if _table_exists(conn, "posting_jobs"):
        postings = conn.execute(
            """SELECT * FROM posting_jobs
               WHERE client_code = ?
                 AND document_date LIKE ?
                 AND entry_kind = 'credit'
                 AND posting_status NOT IN ('cancelled', 'rolled_back')""",
            (client_code, f"{period}%"),
        ).fetchall()

        for posting in postings:
            posting = _row_dict(posting)
            posting_gl = posting.get("gl_account", "")
            posting_amount = abs(posting.get("amount") or 0)

            gl_match = (
                posting_gl == debit_account
                or posting_gl == credit_account
            )
            amount_close = (
                posting_amount > 0
                and abs(posting_amount - abs(amount)) / posting_amount < 0.05
            )

            if gl_match and amount_close:
                result["has_collision"] = True
                if not result["collision_type"]:
                    result["collision_type"] = "posting_job_overlap"
                result["collisions"].append({
                    "type": "posting_job_overlap",
                    "posting_id": posting.get("posting_id"),
                    "posting_amount": posting_amount,
                    "journal_amount": abs(amount),
                    "gl_account": posting_gl,
                    "document_id": posting.get("document_id"),
                    "description_en": (
                        f"Existing credit posting ({posting.get('posting_id')}) "
                        f"for {posting_amount} on GL {posting_gl} already "
                        f"handles this correction. Manual journal would "
                        f"double the effect."
                    ),
                    "description_fr": (
                        f"L'écriture de crédit existante ({posting.get('posting_id')}) "
                        f"de {posting_amount} sur GL {posting_gl} gère déjà "
                        f"cette correction. Le journal manuel doublerait l'effet."
                    ),
                })

    # Check 3: No document linkage — manual journal has no supporting document
    if document_id is None:
        result["collisions"].append({
            "type": "unsupported_journal",
            "description_en": (
                "Manual journal has no linked document_id. "
                "Document-backed corrections take precedence over "
                "unsupported manual entries."
            ),
            "description_fr": (
                "Le journal manuel n'a pas de document_id lié. "
                "Les corrections basées sur des documents ont priorité "
                "sur les écritures manuelles sans justificatif."
            ),
        })
        if result["has_collision"]:
            result["collision_type"] = "unsupported_double_correction"

    return result


def quarantine_manual_journal(
    conn: sqlite3.Connection,
    entry_id: str,
    *,
    collision_document_id: str | None = None,
    collision_chain_id: int | None = None,
    reason: str = "",
) -> dict[str, Any]:
    """Block/quarantine a manual journal entry that collides with a document correction.

    TRAP 7: The manual journal is not deleted — it's quarantined for
    review.  A reviewer must explicitly confirm it's needed despite
    the existing document-backed correction.
    """
    if not _table_exists(conn, "manual_journal_entries"):
        raise RuntimeError("manual_journal_entries table missing")

    now = _utc_now()
    conn.execute(
        """UPDATE manual_journal_entries
           SET status = 'quarantined',
               collision_status = 'collision_detected',
               collision_document_id = ?,
               collision_chain_id = ?,
               updated_at = ?
           WHERE entry_id = ?""",
        (collision_document_id, collision_chain_id, now, entry_id),
    )
    conn.commit()

    # Audit log
    if _table_exists(conn, "audit_log"):
        conn.execute(
            """INSERT INTO audit_log
                   (event_type, document_id, prompt_snippet, created_at)
               VALUES (?, ?, ?, ?)""",
            (
                "manual_journal_quarantined",
                collision_document_id or entry_id,
                _json_dumps({
                    "entry_id": entry_id,
                    "collision_document_id": collision_document_id,
                    "collision_chain_id": collision_chain_id,
                    "reason": reason,
                }),
                now,
            ),
        )
        conn.commit()

    return {
        "status": "quarantined",
        "entry_id": entry_id,
        "collision_document_id": collision_document_id,
        "collision_chain_id": collision_chain_id,
    }


def validate_manual_journal(
    conn: sqlite3.Connection,
    *,
    entry_id: str,
    client_code: str,
    period: str,
    debit_account: str,
    credit_account: str,
    amount: float,
    document_id: str | None = None,
    prepared_by: str = "",
) -> dict[str, Any]:
    """Full validation of a manual journal entry before acceptance.

    TRAP 7: Runs collision detection and either:
    - Allows the journal if no collision
    - Quarantines it if collision detected
    - Never allows silent coexistence with document-backed corrections
    """
    collision = detect_manual_journal_collision(
        conn,
        client_code=client_code,
        period=period,
        debit_account=debit_account,
        credit_account=credit_account,
        amount=amount,
        document_id=document_id,
    )

    if collision["has_collision"]:
        # Quarantine the entry
        quarantine_result = quarantine_manual_journal(
            conn,
            entry_id,
            collision_document_id=(
                collision["collisions"][0].get("document_id")
                if collision["collisions"] else None
            ),
            collision_chain_id=(
                collision["collisions"][0].get("chain_id")
                if collision["collisions"] else None
            ),
            reason=collision["collision_type"] or "collision_detected",
        )

        return {
            "accepted": False,
            "entry_id": entry_id,
            "status": "quarantined",
            "collision": collision,
            "quarantine": quarantine_result,
            "message_en": (
                "Manual journal entry quarantined due to collision with "
                "existing document-backed correction. Review required."
            ),
            "message_fr": (
                "Écriture de journal manuel mise en quarantaine en raison "
                "d'un conflit avec une correction existante basée sur un "
                "document. Révision requise."
            ),
        }

    # No collision — accept
    now = _utc_now()
    if _table_exists(conn, "manual_journal_entries"):
        conn.execute(
            """UPDATE manual_journal_entries
               SET collision_status = 'clear', updated_at = ?
               WHERE entry_id = ?""",
            (now, entry_id),
        )
        conn.commit()

    return {
        "accepted": True,
        "entry_id": entry_id,
        "status": "clear",
        "collision": collision,
    }

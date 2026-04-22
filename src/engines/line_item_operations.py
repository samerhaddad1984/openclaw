"""src/engines/line_item_operations.py — CPA line-item corrections.

Three operations CPAs perform on OCR-extracted invoice lines:

  * ``split_line``     — 1 line → N lines on the same document. Sum of the
                         new pretax amounts must equal the original pretax
                         amount.
  * ``merge_lines``    — N lines → 1 line on the same document. Pretax,
                         GST, QST, HST are aggregated. Tax code must be
                         consistent across sources.
  * ``allocate_line``  — 1 line → N lines split across GL accounts by
                         percentage or exact amount. Sum of allocations
                         equals the original pretax amount.

Each operation:

  - runs inside a single transaction
  - uses optimistic concurrency on the source line(s) and parent document
    (raises ``OptimisticConcurrencyError`` on stale reads)
  - is idempotent via ``client_request_id`` — replaying the same id
    returns the prior result
  - writes an ``invoice_line_audit`` row capturing before/after state,
    performed_by, timestamp, and reason
  - marks source lines soft-deleted (``deleted_at`` timestamp) rather
    than removing them, so the audit trail can reconstruct history
  - tags the new line(s) with ``modification_type`` (``split`` / ``merged``
    / ``allocated``) and ``parent_line_id`` for UI badges

Decimal is used throughout — never float — to avoid drift.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Iterable

from src.db.optimistic import OptimisticConcurrencyError, version_check_update

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
SUM_TOLERANCE = Decimal("0.01")  # $0.01 tolerance on sum-equality checks
MAX_SPLITS = 20
MAX_MERGE_SOURCES = 20

# ---------------------------------------------------------------------------
# Public errors
# ---------------------------------------------------------------------------


class LineItemOperationError(ValueError):
    """Raised for invariant violations inside split/merge/allocate."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _ensure_operations_schema(conn: sqlite3.Connection) -> None:
    """Add the soft-delete / modification-type columns + the audit table.

    Idempotent — ALTER TABLE failures for existing columns are swallowed.
    """
    # Additive columns on invoice_lines (may not exist on older DBs).
    for col_def in (
        "modification_type TEXT",  # null, 'split', 'merged', 'allocated'
        "parent_line_ids TEXT",    # JSON array of source line_ids (null for OCR lines)
        "deleted_at TEXT",         # soft delete timestamp (null = active)
    ):
        try:
            conn.execute(f"ALTER TABLE invoice_lines ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass

    # Audit table — one row per operation, not per affected line.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_line_audit (
            audit_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            operation        TEXT NOT NULL,          -- 'split' / 'merge' / 'allocate'
            client_request_id TEXT,                   -- idempotency key (unique per doc+op when present)
            performed_by     TEXT NOT NULL,
            performed_at     TEXT NOT NULL,
            reason           TEXT,
            before_json      TEXT NOT NULL,           -- snapshot of source line(s)
            after_json       TEXT NOT NULL,           -- snapshot of resulting line(s)
            source_line_ids  TEXT NOT NULL,           -- JSON array
            result_line_ids  TEXT NOT NULL            -- JSON array
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_line_audit_doc ON invoice_line_audit(document_id)",
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_line_audit_idem "
        "ON invoice_line_audit(document_id, client_request_id) "
        "WHERE client_request_id IS NOT NULL",
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dec(v: Any) -> Decimal:
    if v is None or v == "":
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def _q(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if isinstance(row, sqlite3.Row):
        return dict(row)
    raise TypeError(f"expected Row/dict, got {type(row).__name__}")


def _fetch_line(conn: sqlite3.Connection, line_id: int) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM invoice_lines WHERE line_id = ?",
        (int(line_id),),
    ).fetchone()
    if row is None:
        raise LineItemOperationError("line_not_found", f"line_id={line_id} not found")
    return dict(row)


def _doc_version(conn: sqlite3.Connection, document_id: str) -> int:
    row = conn.execute(
        "SELECT version FROM documents WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    if row is None:
        raise LineItemOperationError("document_not_found", f"document_id={document_id}")
    v = row[0] if not isinstance(row, sqlite3.Row) else row["version"]
    return int(v or 1)


def _bump_doc_version(conn: sqlite3.Connection, document_id: str, expected_version: int) -> int:
    """Bump the parent document's version under optimistic concurrency.

    Raises ``OptimisticConcurrencyError`` if the caller's version is stale.
    """
    cur = conn.execute(
        "UPDATE documents SET version = version + 1 "
        "WHERE document_id = ? AND version = ?",
        (document_id, int(expected_version)),
    )
    if cur.rowcount == 0:
        raise OptimisticConcurrencyError("documents", document_id, int(expected_version))
    return int(expected_version) + 1


def _soft_delete_line(
    conn: sqlite3.Connection, line_id: int, expected_version: int, deleted_at: str,
) -> None:
    """Mark a line deleted_at = now under optimistic concurrency."""
    version_check_update(
        conn,
        table="invoice_lines",
        pk_column="line_id",
        pk_value=int(line_id),
        expected_version=int(expected_version),
        fields={"deleted_at": deleted_at},
    )


def _insert_line(
    conn: sqlite3.Connection,
    *,
    document_id: str,
    line_number: int,
    description: str,
    pretax: Decimal,
    gl_account: str | None,
    tax_code: str | None,
    gst_amount: Decimal | None = None,
    qst_amount: Decimal | None = None,
    hst_amount: Decimal | None = None,
    modification_type: str,
    parent_line_ids: list[int],
    line_notes: str | None = None,
) -> int:
    cur = conn.execute(
        """INSERT INTO invoice_lines
           (document_id, line_number, description, line_total_pretax,
            gl_account, tax_code, tax_regime,
            gst_amount, qst_amount, hst_amount,
            modification_type, parent_line_ids, line_notes, created_at,
            version)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
        (
            document_id,
            int(line_number),
            description,
            float(_q(pretax)),
            gl_account,
            tax_code,
            tax_code,  # mirror tax_regime for existing queries
            float(_q(gst_amount)) if gst_amount is not None else None,
            float(_q(qst_amount)) if qst_amount is not None else None,
            float(_q(hst_amount)) if hst_amount is not None else None,
            modification_type,
            json.dumps(parent_line_ids),
            line_notes,
            _now_iso(),
        ),
    )
    return int(cur.lastrowid)


def _active_lines(conn: sqlite3.Connection, document_id: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [
        dict(r)
        for r in conn.execute(
            """SELECT * FROM invoice_lines
               WHERE document_id = ? AND (deleted_at IS NULL OR deleted_at = '')
               ORDER BY line_number""",
            (document_id,),
        ).fetchall()
    ]


def _next_line_number(conn: sqlite3.Connection, document_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(line_number), 0) FROM invoice_lines WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    n = row[0] if row else 0
    return int(n or 0) + 1


def _idempotency_hit(
    conn: sqlite3.Connection,
    document_id: str,
    client_request_id: str | None,
) -> dict[str, Any] | None:
    """If a prior audit row exists for this client_request_id, return its
    result. Callers MUST return early without retrying the operation."""
    if not client_request_id:
        return None
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """SELECT * FROM invoice_line_audit
           WHERE document_id = ? AND client_request_id = ?""",
        (document_id, client_request_id),
    ).fetchone()
    if row is None:
        return None
    d = dict(row)
    return {
        "ok": True,
        "idempotent_replay": True,
        "audit_id": d["audit_id"],
        "result_line_ids": json.loads(d["result_line_ids"]),
    }


def _write_audit(
    conn: sqlite3.Connection,
    *,
    document_id: str,
    operation: str,
    performed_by: str,
    reason: str | None,
    before: Any,
    after: Any,
    source_line_ids: list[int],
    result_line_ids: list[int],
    client_request_id: str | None,
) -> int:
    cur = conn.execute(
        """INSERT INTO invoice_line_audit
           (document_id, operation, client_request_id, performed_by,
            performed_at, reason, before_json, after_json,
            source_line_ids, result_line_ids)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            document_id,
            operation,
            client_request_id,
            performed_by,
            _now_iso(),
            reason,
            json.dumps(before, default=str),
            json.dumps(after, default=str),
            json.dumps(source_line_ids),
            json.dumps(result_line_ids),
        ),
    )
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# SPLIT
# ---------------------------------------------------------------------------


def split_line(
    *,
    document_id: str,
    line_id: int,
    splits: list[dict[str, Any]],
    expected_version: int,
    expected_doc_version: int | None = None,
    performed_by: str,
    reason: str | None = None,
    client_request_id: str | None = None,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Split one line into N.

    ``splits`` is a list of dicts with at minimum:
        description (str), amount (pretax, Decimal/number),
        gl_account (str|None), tax_code (str|None).
    Sum of amounts must equal the original line's pretax within $0.01.
    """
    _ensure_operations_schema(conn)

    hit = _idempotency_hit(conn, document_id, client_request_id)
    if hit is not None:
        return hit

    if not splits or len(splits) < 2:
        raise LineItemOperationError(
            "splits_too_few", "split requires at least 2 target lines",
        )
    if len(splits) > MAX_SPLITS:
        raise LineItemOperationError(
            "splits_too_many", f"split allows at most {MAX_SPLITS} targets",
        )

    src = _fetch_line(conn, line_id)
    if src["document_id"] != document_id:
        raise LineItemOperationError(
            "document_mismatch", "line does not belong to this document",
        )
    if src.get("deleted_at"):
        raise LineItemOperationError(
            "line_already_deleted", "line was already modified",
        )

    original_pretax = _dec(src["line_total_pretax"])
    total_new = sum((_dec(s.get("amount", 0)) for s in splits), Decimal("0"))
    if abs(_q(total_new) - _q(original_pretax)) > SUM_TOLERANCE:
        raise LineItemOperationError(
            "sum_mismatch",
            f"split amounts sum to {_q(total_new)} but original is {_q(original_pretax)}",
        )

    if expected_doc_version is None:
        expected_doc_version = _doc_version(conn, document_id)

    now = _now_iso()
    # Soft-delete source first (enforces version check on the line).
    _soft_delete_line(conn, line_id, int(expected_version), now)

    # Insert N new lines.
    result_ids: list[int] = []
    next_n = _next_line_number(conn, document_id)
    for i, s in enumerate(splits):
        new_id = _insert_line(
            conn,
            document_id=document_id,
            line_number=next_n + i,
            description=str(s.get("description", "")).strip() or f"Split {i + 1}",
            pretax=_dec(s.get("amount", 0)),
            gl_account=s.get("gl_account"),
            tax_code=s.get("tax_code"),
            modification_type="split",
            parent_line_ids=[int(line_id)],
            line_notes=f"Split from line {line_id}",
        )
        result_ids.append(new_id)

    # Bump parent doc.
    _bump_doc_version(conn, document_id, int(expected_doc_version))

    # Audit.
    audit_id = _write_audit(
        conn,
        document_id=document_id,
        operation="split",
        performed_by=performed_by,
        reason=reason,
        before={
            "line_id": line_id,
            "description": src.get("description"),
            "pretax": float(_q(original_pretax)),
            "gl_account": src.get("gl_account"),
            "tax_code": src.get("tax_code"),
        },
        after=[
            {
                "line_id": nid,
                "description": s.get("description"),
                "pretax": float(_q(_dec(s.get("amount", 0)))),
                "gl_account": s.get("gl_account"),
                "tax_code": s.get("tax_code"),
            }
            for nid, s in zip(result_ids, splits)
        ],
        source_line_ids=[int(line_id)],
        result_line_ids=result_ids,
        client_request_id=client_request_id,
    )
    conn.commit()

    return {
        "ok": True,
        "audit_id": audit_id,
        "result_line_ids": result_ids,
        "new_doc_version": int(expected_doc_version) + 1,
    }


# ---------------------------------------------------------------------------
# MERGE
# ---------------------------------------------------------------------------


def merge_lines(
    *,
    document_id: str,
    line_ids: Iterable[int],
    merged_description: str,
    expected_versions: dict[int, int] | None = None,
    expected_doc_version: int | None = None,
    gl_account: str | None = None,
    tax_code: str | None = None,
    performed_by: str,
    reason: str | None = None,
    client_request_id: str | None = None,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Merge N lines into 1.

    All sources must share the same tax_code unless ``tax_code`` is given
    explicitly to override. Pretax, GST, QST, HST are summed. The caller
    may specify the target ``gl_account``; otherwise the first source's
    GL is used (and only if every source agrees).
    """
    _ensure_operations_schema(conn)

    hit = _idempotency_hit(conn, document_id, client_request_id)
    if hit is not None:
        return hit

    ids = [int(i) for i in line_ids]
    if len(ids) < 2:
        raise LineItemOperationError(
            "too_few_sources", "merge requires at least 2 lines",
        )
    if len(ids) > MAX_MERGE_SOURCES:
        raise LineItemOperationError(
            "too_many_sources",
            f"merge allows at most {MAX_MERGE_SOURCES} sources",
        )
    if len(set(ids)) != len(ids):
        raise LineItemOperationError(
            "duplicate_sources", "merge line_ids contained duplicates",
        )

    sources = [_fetch_line(conn, i) for i in ids]
    for s in sources:
        if s["document_id"] != document_id:
            raise LineItemOperationError(
                "document_mismatch", "merge requires lines on the same document",
            )
        if s.get("deleted_at"):
            raise LineItemOperationError(
                "line_already_deleted", f"line {s['line_id']} already modified",
            )

    # Resolve tax code.
    src_tax_codes = {str(s.get("tax_code") or "").upper() for s in sources}
    if tax_code is None:
        if len(src_tax_codes) > 1:
            raise LineItemOperationError(
                "tax_code_mismatch",
                "sources have different tax codes — pass tax_code explicitly",
            )
        tax_code = next(iter(src_tax_codes)) or None

    # Resolve GL account.
    if gl_account is None:
        gls = {str(s.get("gl_account") or "") for s in sources}
        if len(gls) > 1:
            raise LineItemOperationError(
                "gl_mismatch",
                "sources have different GL accounts — pass gl_account explicitly",
            )
        gl_account = next(iter(gls)) or None

    pretax = sum((_dec(s["line_total_pretax"]) for s in sources), Decimal("0"))
    gst = sum((_dec(s.get("gst_amount")) for s in sources), Decimal("0"))
    qst = sum((_dec(s.get("qst_amount")) for s in sources), Decimal("0"))
    hst = sum((_dec(s.get("hst_amount")) for s in sources), Decimal("0"))

    if expected_doc_version is None:
        expected_doc_version = _doc_version(conn, document_id)

    now = _now_iso()
    # Soft-delete every source.
    for s in sources:
        expected = (expected_versions or {}).get(int(s["line_id"]))
        if expected is None:
            expected = int(s.get("version") or 1)
        _soft_delete_line(conn, int(s["line_id"]), expected, now)

    next_n = _next_line_number(conn, document_id)
    new_id = _insert_line(
        conn,
        document_id=document_id,
        line_number=next_n,
        description=merged_description.strip() or "Merged",
        pretax=pretax,
        gl_account=gl_account,
        tax_code=tax_code,
        gst_amount=gst if gst > 0 else None,
        qst_amount=qst if qst > 0 else None,
        hst_amount=hst if hst > 0 else None,
        modification_type="merged",
        parent_line_ids=ids,
        line_notes=f"Merged from lines {', '.join(str(i) for i in ids)}",
    )

    _bump_doc_version(conn, document_id, int(expected_doc_version))

    audit_id = _write_audit(
        conn,
        document_id=document_id,
        operation="merge",
        performed_by=performed_by,
        reason=reason,
        before=[
            {
                "line_id": s["line_id"],
                "description": s.get("description"),
                "pretax": float(_q(_dec(s["line_total_pretax"]))),
                "gl_account": s.get("gl_account"),
                "tax_code": s.get("tax_code"),
            }
            for s in sources
        ],
        after={
            "line_id": new_id,
            "description": merged_description,
            "pretax": float(_q(pretax)),
            "gl_account": gl_account,
            "tax_code": tax_code,
        },
        source_line_ids=ids,
        result_line_ids=[new_id],
        client_request_id=client_request_id,
    )
    conn.commit()

    return {
        "ok": True,
        "audit_id": audit_id,
        "result_line_ids": [new_id],
        "new_doc_version": int(expected_doc_version) + 1,
    }


# ---------------------------------------------------------------------------
# ALLOCATE
# ---------------------------------------------------------------------------


def allocate_line(
    *,
    document_id: str,
    line_id: int,
    allocations: list[dict[str, Any]],
    mode: str = "amount",
    expected_version: int,
    expected_doc_version: int | None = None,
    performed_by: str,
    reason: str | None = None,
    client_request_id: str | None = None,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Allocate one line across multiple GL accounts / tax treatments.

    ``mode`` is either ``amount`` (each allocation has ``amount`` in the
    original line's currency) or ``percentage`` (each has ``percentage``
    in 0–100). In percentage mode, percentages must sum to 100 exactly
    and the resulting amounts are derived by multiplying the original
    pretax by each percentage. The final allocation absorbs the rounding
    remainder so the amounts always sum to the original exactly.
    """
    _ensure_operations_schema(conn)

    hit = _idempotency_hit(conn, document_id, client_request_id)
    if hit is not None:
        return hit

    if mode not in ("amount", "percentage"):
        raise LineItemOperationError(
            "bad_mode", f"mode must be 'amount' or 'percentage', got {mode!r}",
        )
    if not allocations or len(allocations) < 2:
        raise LineItemOperationError(
            "too_few_allocations", "allocate requires at least 2 allocations",
        )
    if len(allocations) > MAX_SPLITS:
        raise LineItemOperationError(
            "too_many_allocations", f"allocate allows at most {MAX_SPLITS}",
        )

    src = _fetch_line(conn, line_id)
    if src["document_id"] != document_id:
        raise LineItemOperationError(
            "document_mismatch", "line does not belong to this document",
        )
    if src.get("deleted_at"):
        raise LineItemOperationError(
            "line_already_deleted", "line was already modified",
        )

    original_pretax = _dec(src["line_total_pretax"])

    amounts: list[Decimal] = []
    if mode == "percentage":
        pct_sum = Decimal("0")
        for a in allocations:
            pct = _dec(a.get("percentage", 0))
            if pct < 0:
                raise LineItemOperationError(
                    "negative_percentage", "percentages must be non-negative",
                )
            pct_sum += pct
        if pct_sum != Decimal("100"):
            raise LineItemOperationError(
                "percentage_sum_mismatch",
                f"percentages sum to {pct_sum}, need exactly 100",
            )
        # Derive amounts — last allocation absorbs remainder so sum is exact.
        remaining = original_pretax
        for i, a in enumerate(allocations):
            if i == len(allocations) - 1:
                amounts.append(_q(remaining))
            else:
                pct = _dec(a.get("percentage", 0))
                amt = _q(original_pretax * pct / Decimal("100"))
                amounts.append(amt)
                remaining -= amt
    else:  # mode == 'amount'
        for a in allocations:
            amt = _dec(a.get("amount", 0))
            if amt < 0:
                raise LineItemOperationError(
                    "negative_amount", "amounts must be non-negative",
                )
            amounts.append(_q(amt))
        total = sum(amounts, Decimal("0"))
        if abs(_q(total) - _q(original_pretax)) > SUM_TOLERANCE:
            raise LineItemOperationError(
                "sum_mismatch",
                f"allocated amounts sum to {_q(total)} but original is {_q(original_pretax)}",
            )

    # Every allocation must have a gl_account (the whole point).
    for i, a in enumerate(allocations):
        if not a.get("gl_account"):
            raise LineItemOperationError(
                "missing_gl_account",
                f"allocation #{i + 1} is missing gl_account",
            )

    if expected_doc_version is None:
        expected_doc_version = _doc_version(conn, document_id)

    now = _now_iso()
    _soft_delete_line(conn, line_id, int(expected_version), now)

    result_ids: list[int] = []
    next_n = _next_line_number(conn, document_id)
    for i, (a, amt) in enumerate(zip(allocations, amounts)):
        desc = str(a.get("description") or src.get("description") or "").strip()
        new_id = _insert_line(
            conn,
            document_id=document_id,
            line_number=next_n + i,
            description=desc or f"Allocation {i + 1}",
            pretax=amt,
            gl_account=a.get("gl_account"),
            tax_code=a.get("tax_code") or src.get("tax_code"),
            modification_type="allocated",
            parent_line_ids=[int(line_id)],
            line_notes=f"Allocated from line {line_id}",
        )
        result_ids.append(new_id)

    _bump_doc_version(conn, document_id, int(expected_doc_version))

    audit_id = _write_audit(
        conn,
        document_id=document_id,
        operation="allocate",
        performed_by=performed_by,
        reason=reason,
        before={
            "line_id": line_id,
            "description": src.get("description"),
            "pretax": float(_q(original_pretax)),
            "gl_account": src.get("gl_account"),
            "tax_code": src.get("tax_code"),
        },
        after=[
            {
                "line_id": nid,
                "description": a.get("description"),
                "pretax": float(amt),
                "gl_account": a.get("gl_account"),
                "tax_code": a.get("tax_code") or src.get("tax_code"),
                "percentage": float(_dec(a.get("percentage", 0))) if mode == "percentage" else None,
            }
            for nid, a, amt in zip(result_ids, allocations, amounts)
        ],
        source_line_ids=[int(line_id)],
        result_line_ids=result_ids,
        client_request_id=client_request_id,
    )
    conn.commit()

    return {
        "ok": True,
        "audit_id": audit_id,
        "result_line_ids": result_ids,
        "new_doc_version": int(expected_doc_version) + 1,
    }


# ---------------------------------------------------------------------------
# QUERIES
# ---------------------------------------------------------------------------


def get_active_lines(conn: sqlite3.Connection, document_id: str) -> list[dict[str, Any]]:
    """Return non-deleted lines for a document ordered by line_number."""
    _ensure_operations_schema(conn)
    return _active_lines(conn, document_id)


def get_audit_trail(
    conn: sqlite3.Connection, document_id: str,
) -> list[dict[str, Any]]:
    """Return all audit rows for a document, newest first."""
    _ensure_operations_schema(conn)
    conn.row_factory = sqlite3.Row
    rows = [
        dict(r)
        for r in conn.execute(
            """SELECT * FROM invoice_line_audit
               WHERE document_id = ?
               ORDER BY performed_at DESC, audit_id DESC""",
            (document_id,),
        ).fetchall()
    ]
    # Decode JSON fields for convenient consumption.
    for r in rows:
        for key in ("before_json", "after_json", "source_line_ids", "result_line_ids"):
            try:
                r[key[:-5] if key.endswith("_json") else key] = json.loads(r[key])
            except Exception:
                pass
    return rows


def has_cpa_modifications(conn: sqlite3.Connection, document_id: str) -> bool:
    """True if any audit row exists for this document."""
    _ensure_operations_schema(conn)
    row = conn.execute(
        "SELECT 1 FROM invoice_line_audit WHERE document_id = ? LIMIT 1",
        (document_id,),
    ).fetchone()
    return row is not None

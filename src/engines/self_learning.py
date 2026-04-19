"""Sprint A — self-learning layer.

Every CPA correction teaches the system:

- Vendor aliases: "unprix" once corrected to "Uniprix" by a CPA will be
  auto-canonicalised on the next extraction.
- GL-account learning: once a vendor's GL is corrected N times, we can
  suggest it for new line items where the engine didn't already assign one.
- Correction log: every /document/update and /document/line_item/save goes
  through here so we have a full audit trail keyed by document_id.

The tables are scoped by firm_code so Firm A's corrections don't leak into
Firm B's recommendations.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.engines.ocr_engine import DB_PATH

log = logging.getLogger(__name__)

# A vendor alias is applied automatically only when we've seen it corrected
# at least this many times. Below the floor the original string is kept so
# one-off typos can't hijack a vendor globally.
VENDOR_ALIAS_MIN_CORRECTIONS = 2

# GL / tax_code suggestions are attached (not applied) until this many
# corrections for the same vendor agree.
GL_SUGGEST_MIN_CORRECTIONS = 2


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS vendor_learning (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        extracted_vendor    TEXT NOT NULL,
        canonical_vendor    TEXT NOT NULL,
        firm_code           TEXT,
        confidence          REAL DEFAULT 1.0,
        correction_count    INTEGER DEFAULT 1,
        first_seen          TEXT DEFAULT (datetime('now')),
        last_seen           TEXT DEFAULT (datetime('now')),
        UNIQUE(extracted_vendor, canonical_vendor, firm_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vendor_learning_extract "
    "ON vendor_learning(extracted_vendor, firm_code)",
    """
    CREATE TABLE IF NOT EXISTS vendor_gl_learning (
        id                        INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_vendor          TEXT NOT NULL,
        gl_account                TEXT NOT NULL,
        tax_code                  TEXT,
        firm_code                 TEXT,
        line_description_pattern  TEXT,
        confidence                REAL DEFAULT 1.0,
        correction_count          INTEGER DEFAULT 1,
        first_seen                TEXT DEFAULT (datetime('now')),
        last_seen                 TEXT DEFAULT (datetime('now')),
        UNIQUE(canonical_vendor, gl_account, tax_code, firm_code, line_description_pattern)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vendor_gl "
    "ON vendor_gl_learning(canonical_vendor, firm_code)",
    """
    CREATE TABLE IF NOT EXISTS correction_log (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        document_id   TEXT NOT NULL,
        field         TEXT NOT NULL,
        old_value     TEXT,
        new_value     TEXT,
        corrected_by  TEXT,
        firm_code     TEXT,
        created_at    TEXT DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_correction_log_doc "
    "ON correction_log(document_id)",
    "CREATE INDEX IF NOT EXISTS idx_correction_log_field "
    "ON correction_log(field, firm_code, created_at)",
]


def ensure_schema(db_path: Optional[Path] = None) -> None:
    """Idempotent — call before any read/write."""
    target = str(db_path) if db_path is not None else str(DB_PATH)
    conn = sqlite3.connect(target, timeout=10)
    try:
        for stmt in _DDL:
            conn.execute(stmt)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Record a correction
# ---------------------------------------------------------------------------

_VENDOR_FIELDS = {"vendor"}
_GL_FIELDS = {"gl_account", "tax_code", "category"}


def _norm(s: Any) -> Optional[str]:
    if s is None:
        return None
    v = str(s).strip()
    return v or None


def record_correction(
    *,
    document_id: str,
    field: str,
    old_value: Any,
    new_value: Any,
    corrected_by: str = "",
    firm_code: str = "",
    vendor_hint: Optional[str] = None,
    line_description_pattern: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Persist a correction and update the learned tables.

    Returns a small summary dict that includes which tables were touched —
    useful in tests and logs.
    """
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    old_s = _norm(old_value)
    new_s = _norm(new_value)
    summary = {"correction_logged": False, "vendor_learning": False, "gl_learning": False}
    if old_s == new_s or not field:
        return summary

    conn = sqlite3.connect(target, timeout=10)
    try:
        # 1. Always log.
        conn.execute(
            "INSERT INTO correction_log (document_id, field, old_value, new_value, "
            "corrected_by, firm_code) VALUES (?,?,?,?,?,?)",
            (document_id, field, old_s, new_s, corrected_by or None, firm_code or None),
        )
        summary["correction_logged"] = True

        # 2. Vendor alias learning.
        if field in _VENDOR_FIELDS and old_s and new_s:
            # firm_code stored as '' rather than NULL — see _bump_gl_learning
            # for the SQLite-NULL-distinct-under-UNIQUE reasoning.
            conn.execute(
                "INSERT INTO vendor_learning (extracted_vendor, canonical_vendor, firm_code) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(extracted_vendor, canonical_vendor, firm_code) DO UPDATE SET "
                "  correction_count = correction_count + 1, "
                "  confidence = MIN(1.0, confidence + 0.05), "
                "  last_seen = datetime('now')",
                (old_s, new_s, firm_code or ""),
            )
            summary["vendor_learning"] = True

        # 3. GL / tax_code learning is keyed on the canonical vendor — the
        #    vendor the document ended up tagged with — not the raw OCR
        #    string. When the caller knows the vendor they pass it via
        #    vendor_hint; otherwise look it up from the documents row.
        if field in _GL_FIELDS and new_s:
            canonical = _norm(vendor_hint)
            if canonical is None:
                row = conn.execute(
                    "SELECT vendor FROM documents WHERE document_id=?",
                    (document_id,),
                ).fetchone()
                canonical = _norm(row[0]) if row else None
            if canonical:
                _bump_gl_learning(
                    conn,
                    canonical_vendor=canonical,
                    gl_account=new_s if field == "gl_account" else None,
                    tax_code=new_s if field == "tax_code" else None,
                    firm_code=firm_code,
                    pattern=line_description_pattern,
                )
                summary["gl_learning"] = True

        conn.commit()
    finally:
        conn.close()
    return summary


def _bump_gl_learning(
    conn: sqlite3.Connection,
    *,
    canonical_vendor: str,
    gl_account: Optional[str],
    tax_code: Optional[str],
    firm_code: str,
    pattern: Optional[str],
) -> None:
    """Upsert into vendor_gl_learning. We store one row per (vendor, gl, tax)
    tuple so tax_code changes don't overwrite the GL history and vice versa.

    Keys are normalised to empty string rather than NULL because SQLite
    treats two NULLs as distinct under UNIQUE, which would otherwise create
    a new row for every correction with a missing tax_code / firm_code
    instead of incrementing the existing one.
    """
    fc = firm_code or ""
    pat = pattern or ""
    tc = tax_code or ""
    if gl_account is None:
        row = conn.execute(
            "SELECT gl_account FROM vendor_gl_learning "
            "WHERE canonical_vendor=? AND COALESCE(firm_code,'') = ? "
            "ORDER BY last_seen DESC LIMIT 1",
            (canonical_vendor, fc),
        ).fetchone()
        if row is None:
            # No prior GL for this vendor — store the tax_code correction
            # against a synthetic unknown GL so it still counts as history.
            gl_account = "__unknown__"
        else:
            gl_account = row[0]

    conn.execute(
        "INSERT INTO vendor_gl_learning "
        "(canonical_vendor, gl_account, tax_code, firm_code, line_description_pattern) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(canonical_vendor, gl_account, tax_code, firm_code, line_description_pattern) "
        "DO UPDATE SET "
        "  correction_count = correction_count + 1, "
        "  confidence = MIN(1.0, confidence + 0.05), "
        "  last_seen = datetime('now')",
        (canonical_vendor, gl_account, tc, fc, pat),
    )


# ---------------------------------------------------------------------------
# Apply learning at extraction time
# ---------------------------------------------------------------------------

def apply_vendor_learning(
    extracted_vendor: Optional[str],
    firm_code: str = "",
    *,
    db_path: Optional[Path] = None,
    min_corrections: int = VENDOR_ALIAS_MIN_CORRECTIONS,
) -> Tuple[Optional[str], Optional[str]]:
    """Return (canonical_vendor, original_vendor_if_changed).

    When no alias has enough support we pass the vendor through unchanged.
    """
    if not extracted_vendor:
        return extracted_vendor, None
    target = str(db_path) if db_path is not None else str(DB_PATH)
    try:
        ensure_schema(db_path)
        conn = sqlite3.connect(target, timeout=5)
        try:
            row = conn.execute(
                "SELECT canonical_vendor, correction_count FROM vendor_learning "
                "WHERE extracted_vendor = ? "
                "  AND COALESCE(firm_code,'') = ? "
                "ORDER BY correction_count DESC LIMIT 1",
                (extracted_vendor, firm_code or ""),
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        log.exception("apply_vendor_learning lookup failed")
        return extracted_vendor, None
    if not row:
        return extracted_vendor, None
    canonical, count = row
    if (count or 0) < min_corrections:
        return extracted_vendor, None
    if canonical == extracted_vendor:
        return extracted_vendor, None
    return canonical, extracted_vendor


def suggest_gl_for_vendor(
    canonical_vendor: Optional[str],
    firm_code: str = "",
    *,
    db_path: Optional[Path] = None,
    min_corrections: int = GL_SUGGEST_MIN_CORRECTIONS,
) -> Optional[Dict[str, Optional[str]]]:
    """Return {'gl_account': ..., 'tax_code': ...} once we have enough support."""
    if not canonical_vendor:
        return None
    target = str(db_path) if db_path is not None else str(DB_PATH)
    try:
        ensure_schema(db_path)
        conn = sqlite3.connect(target, timeout=5)
        try:
            row = conn.execute(
                "SELECT gl_account, tax_code, correction_count FROM vendor_gl_learning "
                "WHERE canonical_vendor = ? "
                "  AND COALESCE(firm_code,'') = ? "
                "  AND gl_account != '__unknown__' "
                "ORDER BY correction_count DESC, last_seen DESC LIMIT 1",
                (canonical_vendor, firm_code or ""),
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        log.exception("suggest_gl_for_vendor lookup failed")
        return None
    if not row:
        return None
    gl, tax, count = row
    if (count or 0) < min_corrections:
        return None
    return {"gl_account": gl, "tax_code": tax or None}


def suggest_line_item_gl(
    lines: Iterable[Dict[str, Any]],
    canonical_vendor: Optional[str],
    firm_code: str = "",
    *,
    db_path: Optional[Path] = None,
) -> None:
    """Annotate each line without a gl_account with a learned suggestion.

    Mutates the line dicts in place. Only suggests when the vendor is
    known and we have enough corrections; never overwrites an engine-set
    gl_account.
    """
    s = suggest_gl_for_vendor(canonical_vendor, firm_code, db_path=db_path)
    if not s:
        return
    for line in lines:
        if line.get("gl_account"):
            continue
        line["gl_account_suggested"] = s["gl_account"]
        if s.get("tax_code"):
            line["tax_code_suggested"] = s["tax_code"]
        line["gl_source"] = "learned"


# ---------------------------------------------------------------------------
# Dashboard metrics
# ---------------------------------------------------------------------------

def top_vendor_corrections(
    limit: int = 50,
    firm_code: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=5)
    try:
        if firm_code:
            rows = conn.execute(
                "SELECT extracted_vendor, canonical_vendor, correction_count, "
                "confidence, first_seen, last_seen FROM vendor_learning "
                "WHERE firm_code = ? "
                "ORDER BY correction_count DESC, last_seen DESC LIMIT ?",
                (firm_code, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT extracted_vendor, canonical_vendor, correction_count, "
                "confidence, first_seen, last_seen FROM vendor_learning "
                "ORDER BY correction_count DESC, last_seen DESC LIMIT ?",
                (limit,),
            ).fetchall()
    finally:
        conn.close()
    return [
        {
            "extracted_vendor": r[0], "canonical_vendor": r[1],
            "correction_count": r[2], "confidence": r[3],
            "first_seen": r[4], "last_seen": r[5],
        }
        for r in rows
    ]


def top_gl_corrections(
    limit: int = 50,
    firm_code: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=5)
    try:
        if firm_code:
            rows = conn.execute(
                "SELECT canonical_vendor, gl_account, tax_code, correction_count, "
                "confidence, last_seen FROM vendor_gl_learning "
                "WHERE firm_code = ? AND gl_account != '__unknown__' "
                "ORDER BY correction_count DESC, last_seen DESC LIMIT ?",
                (firm_code, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT canonical_vendor, gl_account, tax_code, correction_count, "
                "confidence, last_seen FROM vendor_gl_learning "
                "WHERE gl_account != '__unknown__' "
                "ORDER BY correction_count DESC, last_seen DESC LIMIT ?",
                (limit,),
            ).fetchall()
    finally:
        conn.close()
    return [
        {
            "canonical_vendor": r[0], "gl_account": r[1], "tax_code": r[2],
            "correction_count": r[3], "confidence": r[4], "last_seen": r[5],
        }
        for r in rows
    ]


def learning_summary(db_path: Optional[Path] = None) -> Dict[str, Any]:
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=5)
    try:
        r1 = conn.execute("SELECT COUNT(*), COALESCE(SUM(correction_count),0) FROM vendor_learning").fetchone()
        r2 = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(correction_count),0) FROM vendor_gl_learning "
            "WHERE gl_account != '__unknown__'"
        ).fetchone()
        r3 = conn.execute("SELECT COUNT(*) FROM correction_log").fetchone()
        recent = conn.execute(
            "SELECT field, COUNT(*) FROM correction_log "
            "WHERE created_at > datetime('now', '-7 days') GROUP BY field"
        ).fetchall()
    finally:
        conn.close()
    return {
        "vendor_aliases":  {"distinct": r1[0], "total_corrections": r1[1]},
        "vendor_gl":       {"distinct": r2[0], "total_corrections": r2[1]},
        "corrections_all": r3[0],
        "recent_by_field": {f: n for f, n in recent},
    }

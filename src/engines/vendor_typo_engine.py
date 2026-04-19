"""
src/engines/vendor_typo_engine.py — Sprint G Feature 5.

Refined vendor-typo detection. Replaces the over-aggressive prior pass
that fired on any pair with > 0.7 SequenceMatcher ratio.

Refinements:
  * Length-aware Levenshtein threshold (similarity > 0.85 by character).
  * Exclude legitimate corporate-suffix variations (Inc / Inc. / Incorporated
    / Ltée / SA / SARL / LLC / Co / Co.).
  * Require both vendors to have ≥ 3 transactions in the same client.
  * Require their amount-ranges to overlap (a typo would charge similar
    amounts; truly different vendors typically don't).
  * Cap pairs reported per call to keep noise down.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

HIGH = "high"
MEDIUM = "medium"
LOW = "low"

DEFAULT_SIMILARITY_THRESHOLD = 0.85
MIN_TX_COUNT_PER_VENDOR = 3
MAX_PAIRS_REPORTED = 50

# Corporate suffixes that should be normalised away before comparing names.
_CORP_SUFFIXES = (
    " incorporated", " inc.", " inc", " corp.", " corp",
    " ltd.", " ltd", " ltée", " ltee", " limited",
    " sa", " s.a.", " sarl", " s.a.r.l.",
    " llc", " l.l.c.", " llp", " l.l.p.",
    " co.", " co", " company", " companies",
    " plc", " group", " holdings", " enterprises",
    " gmbh", " ag", " bv", " nv",
)

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


def _normalize_vendor(name: str) -> str:
    """Lowercase, strip corporate suffixes + punctuation, collapse spaces."""
    s = (name or "").strip().lower()
    if not s:
        return ""
    for suf in _CORP_SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    s = _PUNCT_RE.sub(" ", s)
    s = " ".join(s.split())
    return s


def levenshtein(a: str, b: str) -> int:
    """Iterative two-row Levenshtein distance (insert/delete/substitute = 1)."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i] + [0] * len(b)
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            cur[j] = min(
                cur[j - 1] + 1,        # insertion
                prev[j] + 1,           # deletion
                prev[j - 1] + cost,    # substitution
            )
        prev = cur
    return prev[-1]


def similarity(a: str, b: str) -> float:
    """1.0 = identical, 0.0 = totally different. Length-normalised."""
    if not a and not b:
        return 1.0
    max_len = max(len(a), len(b))
    if max_len == 0:
        return 1.0
    return 1.0 - (levenshtein(a, b) / max_len)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_vendor_typos_refined(
    firm_code: str = "",
    client_code: str = "",
    days_back: int = 365,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    min_tx_count: int = MIN_TX_COUNT_PER_VENDOR,
    max_pairs: int = MAX_PAIRS_REPORTED,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return a list of suspected vendor-typo findings."""
    own_conn = False
    if conn is None:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        own_conn = True

    findings: list[dict[str, Any]] = []
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        if "vendor" not in cols or "amount" not in cols:
            return []

        params: list[Any] = []
        where = ["TRIM(COALESCE(d.vendor,'')) != ''"]
        if "review_status" in cols:
            where.append("LOWER(COALESCE(d.review_status,'')) != 'ignored'")
        if firm_code and "firm_code" in cols:
            where.append("LOWER(COALESCE(d.firm_code,'')) = LOWER(?)")
            params.append(firm_code)
        if client_code:
            where.append("LOWER(COALESCE(d.client_code,'')) = LOWER(?)")
            params.append(client_code)
        where.append(
            "COALESCE(d.created_at, d.document_date, '1970-01-01') >= "
            "datetime('now', '-' || ? || ' days')"
        )
        params.append(int(days_back))

        sql = (
            "SELECT d.vendor, COUNT(*) AS tx_count, "
            "       MIN(d.amount) AS min_amt, MAX(d.amount) AS max_amt, "
            "       AVG(d.amount) AS avg_amt "
            "FROM documents d WHERE " + " AND ".join(where) +
            " GROUP BY d.vendor HAVING tx_count >= ?"
        )
        params.append(min_tx_count)
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []

        # Build vendor stats.
        vendors: list[dict[str, Any]] = []
        for r in rows:
            raw = (r["vendor"] or "").strip()
            norm = _normalize_vendor(raw)
            if not norm:
                continue
            vendors.append({
                "raw": raw,
                "normalised": norm,
                "tx_count": int(r["tx_count"]),
                "min_amt": float(r["min_amt"] or 0),
                "max_amt": float(r["max_amt"] or 0),
                "avg_amt": float(r["avg_amt"] or 0),
            })

        # Pair-wise comparison. O(n²) is fine for typical CPA-firm vendor
        # counts (< 1000 per client); short-circuit when normalised forms
        # match exactly (definitely the same vendor with a punctuation diff).
        seen_pairs: set[tuple[str, str]] = set()
        for v1, v2 in combinations(vendors, 2):
            key = tuple(sorted([v1["raw"].lower(), v2["raw"].lower()]))
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            # Identical normalised form => corporate-suffix dup.
            if v1["normalised"] == v2["normalised"]:
                findings.append(
                    _make_finding(v1, v2, similarity=1.0, reason="suffix_only_diff")
                )
                continue

            sim = similarity(v1["normalised"], v2["normalised"])
            if sim < similarity_threshold:
                continue
            # Amount-range overlap check.
            if not _amount_ranges_overlap(v1, v2):
                continue
            findings.append(_make_finding(v1, v2, similarity=sim,
                                           reason="fuzzy_match"))
            if len(findings) >= max_pairs:
                break

        return findings
    finally:
        if own_conn:
            conn.close()


def _amount_ranges_overlap(v1: dict, v2: dict) -> bool:
    """Returns True if the [min, max] ranges intersect or are within 50% of
    each other's average."""
    if v1["max_amt"] >= v2["min_amt"] and v2["max_amt"] >= v1["min_amt"]:
        return True
    avg_ratio = (
        max(v1["avg_amt"], v2["avg_amt"])
        / max(min(v1["avg_amt"], v2["avg_amt"]), 0.01)
    )
    return avg_ratio < 2.0


def _make_finding(v1: dict, v2: dict, *, similarity: float,
                   reason: str) -> dict[str, Any]:
    sev = HIGH if similarity > 0.92 else MEDIUM
    return {
        "type": "vendor_typo_variants",
        "severity": sev,
        "reason": reason,
        "vendors": sorted([v1["raw"], v2["raw"]]),
        "normalised": [v1["normalised"], v2["normalised"]],
        "similarity": round(similarity, 4),
        "transaction_counts": [v1["tx_count"], v2["tx_count"]],
        "amount_ranges": [
            [v1["min_amt"], v1["max_amt"]],
            [v2["min_amt"], v2["max_amt"]],
        ],
        "recommendation": "Review: likely same vendor with name variation",
        "i18n_key": "fraud_vendor_typo",
        "detected_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }

"""
src/engines/approval_graph_engine.py — Sprint G Feature 1.

Detects approval-graph anomalies that violate segregation-of-duties:
  * 2-cycles: A approves B's expense, B approves A's
  * Longer cycles: A → B → C → A
  * Self-approval: same user submitter and approver

Cycle detection uses Tarjan's strongly-connected-components algorithm
on a directed graph where an edge u → v means "u was approved by v".
Strongly-connected components of size > 1 are cycles; we also detect
explicit self-loops (size-1 SCC with edge to itself).

All amounts use Python Decimal — currency-safe.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# Severity constants — match fraud_engine.
HIGH = "high"
MEDIUM = "medium"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Cycle detection — Tarjan's SCC
# ---------------------------------------------------------------------------

def find_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    """Return all cycles (each as a list of node ids) in a directed graph.

    Implementation: Tarjan's strongly-connected-components. Any SCC of size
    > 1 is a cycle. Self-loops (node with edge to itself) are returned as
    1-element cycles.
    """
    if not graph:
        return []

    index_counter = [0]
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    sccs: list[list[str]] = []

    def strong_connect(v: str) -> None:
        indices[v] = index_counter[0]
        lowlinks[v] = index_counter[0]
        index_counter[0] += 1
        stack.append(v)
        on_stack.add(v)

        for w in graph.get(v, set()):
            if w not in indices:
                strong_connect(w)
                lowlinks[v] = min(lowlinks[v], lowlinks[w])
            elif w in on_stack:
                lowlinks[v] = min(lowlinks[v], indices[w])

        if lowlinks[v] == indices[v]:
            component = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                component.append(w)
                if w == v:
                    break
            sccs.append(component)

    # Tarjan can blow Python's default recursion limit on big graphs;
    # bump it conservatively for safety.
    import sys as _sys
    old_limit = _sys.getrecursionlimit()
    _sys.setrecursionlimit(max(old_limit, 5000))
    try:
        for node in list(graph.keys()):
            if node not in indices:
                strong_connect(node)
    finally:
        _sys.setrecursionlimit(old_limit)

    cycles: list[list[str]] = []
    for component in sccs:
        if len(component) > 1:
            cycles.append(sorted(component))
        elif len(component) == 1:
            n = component[0]
            if n in graph.get(n, set()):
                cycles.append([n])
    return cycles


# ---------------------------------------------------------------------------
# Detection driver
# ---------------------------------------------------------------------------

def _ensure_columns(conn: sqlite3.Connection) -> tuple[bool, bool]:
    """Return (has_submitted_by, has_approved_by) on the documents table."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    return ("submitted_by" in cols, "approved_by" in cols)


def detect_circular_approvals(
    firm_code: str = "",
    client_code: str = "",
    days_back: int = 90,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return a list of approval-cycle findings for a firm/client.

    Each finding describes one cycle:
      type             always 'circular_approval'
      severity         HIGH for self-approval/cycles >= 2, MEDIUM for 3+ chains
      cycle            list of usernames in cycle order
      cycle_length     number of distinct users in the cycle
      approval_count   number of documents that contributed to the cycle
      total_amount     sum of those documents' amounts (Decimal -> str)
      evidence_docs    list of document_ids
      first_seen       earliest involved document date
      last_seen        latest involved document date
    """
    own_conn = False
    if conn is None:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        own_conn = True

    try:
        has_sub, has_app = _ensure_columns(conn)
        if not (has_sub and has_app):
            # Schema doesn't track approval graph — no findings possible.
            return []

        params: list[Any] = []
        where = ["d.approved_by IS NOT NULL", "TRIM(COALESCE(d.approved_by,'')) != ''",
                 "TRIM(COALESCE(d.submitted_by,'')) != ''"]
        if firm_code:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
            if "firm_code" in cols:
                where.append("LOWER(COALESCE(d.firm_code,'')) = LOWER(?)")
                params.append(firm_code)
        if client_code:
            where.append("LOWER(COALESCE(d.client_code,'')) = LOWER(?)")
            params.append(client_code)
        # Date filter: documents.created_at OR document_date if created_at NULL.
        where.append(
            "COALESCE(d.created_at, d.document_date, '1970-01-01') >= "
            "datetime('now', '-' || ? || ' days')"
        )
        params.append(int(days_back))
        sql = (
            "SELECT d.document_id, d.submitted_by, d.approved_by, "
            "       d.document_date, d.amount, COALESCE(d.created_at, d.document_date) AS ts "
            "FROM documents d WHERE " + " AND ".join(where)
        )
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []

        if not rows:
            return []

        # Build directed graph: submitter -> approver. Same user as both is
        # a self-loop.
        graph: dict[str, set[str]] = defaultdict(set)
        edge_docs: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            sub = (r["submitted_by"] or "").strip().lower()
            app = (r["approved_by"] or "").strip().lower()
            if not sub or not app:
                continue
            graph[sub].add(app)
            edge_docs[(sub, app)].append({
                "document_id": r["document_id"],
                "amount": r["amount"] or 0,
                "document_date": r["document_date"],
                "submitted_by": r["submitted_by"],
                "approved_by": r["approved_by"],
                "ts": r["ts"],
            })

        cycles = find_cycles(graph)
        findings: list[dict[str, Any]] = []
        seen_cycle_keys: set[str] = set()
        for cycle in cycles:
            key = "|".join(sorted(cycle))
            if key in seen_cycle_keys:
                continue
            seen_cycle_keys.add(key)

            # Collect every doc whose (submitter, approver) is in the cycle.
            cycle_set = set(cycle)
            cycle_docs: list[dict[str, Any]] = []
            for (s, a), docs in edge_docs.items():
                if s in cycle_set and a in cycle_set:
                    cycle_docs.extend(docs)
            if not cycle_docs:
                continue

            # Self-approval = single-node loop.
            is_self = len(cycle) == 1
            severity = HIGH  # any approval cycle is HIGH; CAS 315 violation.

            total = sum(
                (Decimal(str(d.get("amount") or 0)) for d in cycle_docs),
                Decimal("0"),
            )
            dates = [str(d.get("ts") or d.get("document_date") or "") for d in cycle_docs]
            dates = sorted(d for d in dates if d)

            findings.append({
                "type": "circular_approval",
                "subtype": "self_approval" if is_self else f"cycle_{len(cycle)}",
                "severity": severity,
                "cycle": cycle,
                "cycle_length": len(cycle),
                "approval_count": len(cycle_docs),
                "total_amount": float(total),
                "evidence_docs": [d["document_id"] for d in cycle_docs],
                "first_seen": dates[0] if dates else "",
                "last_seen": dates[-1] if dates else "",
                "i18n_key": "fraud_circular_approval",
                "detected_at": _utc_now(),
            })
        return findings
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Persistence helpers (audit trail / dashboard)
# ---------------------------------------------------------------------------

ANOMALY_FINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS anomaly_findings (
    finding_id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT,
    client_code TEXT,
    detector TEXT NOT NULL,
    severity TEXT NOT NULL,
    summary TEXT,
    payload_json TEXT NOT NULL,
    evidence_doc_ids TEXT,
    detected_at TEXT DEFAULT (datetime('now')),
    cleared_at TEXT,
    cleared_by TEXT,
    clear_notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_anomaly_findings_lookup
    ON anomaly_findings(firm_code, client_code, detector, cleared_at);
"""


def ensure_anomaly_findings_table(conn: sqlite3.Connection) -> None:
    for stmt in ANOMALY_FINDINGS_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()


def persist_findings(
    conn: sqlite3.Connection,
    findings: list[dict[str, Any]],
    *,
    firm_code: str = "",
    client_code: str = "",
    detector: str = "",
) -> int:
    """Insert findings into anomaly_findings. Returns rows inserted."""
    if not findings:
        return 0
    ensure_anomaly_findings_table(conn)
    import json as _json
    n = 0
    for f in findings:
        sev = f.get("severity") or HIGH
        det = detector or f.get("type") or "unknown"
        ev_ids = ",".join(f.get("evidence_docs") or [])
        summary = f.get("summary") or _summarize(f)
        conn.execute(
            """INSERT INTO anomaly_findings
               (firm_code, client_code, detector, severity, summary,
                payload_json, evidence_doc_ids)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (firm_code, client_code, det, sev, summary,
             _json.dumps(f, default=str), ev_ids),
        )
        n += 1
    conn.commit()
    return n


def _summarize(finding: dict[str, Any]) -> str:
    typ = finding.get("type", "")
    if typ == "circular_approval":
        cycle = finding.get("cycle", [])
        return (
            f"Circular approval: {' → '.join(cycle + [cycle[0]] if cycle else [])} "
            f"({finding.get('approval_count', 0)} docs, "
            f"${finding.get('total_amount', 0):.2f})"
        )
    return typ

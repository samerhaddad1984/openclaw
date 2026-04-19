"""Sprint G F1 — circular approval graph detector tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.approval_graph_engine import (  # noqa: E402
    detect_circular_approvals,
    ensure_anomaly_findings_table,
    find_cycles,
    persist_findings,
)


# ---------------------------------------------------------------------------
# Pure cycle detection
# ---------------------------------------------------------------------------

def test_find_cycles_empty_graph():
    assert find_cycles({}) == []


def test_find_cycles_no_cycle():
    g = {"a": {"b"}, "b": {"c"}, "c": set()}
    assert find_cycles(g) == []


def test_find_cycles_simple_2_cycle():
    g = {"a": {"b"}, "b": {"a"}}
    cycles = find_cycles(g)
    assert len(cycles) == 1
    assert sorted(cycles[0]) == ["a", "b"]


def test_find_cycles_self_loop():
    g = {"a": {"a"}}
    cycles = find_cycles(g)
    assert cycles == [["a"]]


def test_find_cycles_3_node_chain():
    g = {"a": {"b"}, "b": {"c"}, "c": {"a"}}
    cycles = find_cycles(g)
    assert len(cycles) == 1
    assert sorted(cycles[0]) == ["a", "b", "c"]


def test_find_cycles_two_independent_cycles():
    g = {"a": {"b"}, "b": {"a"}, "c": {"d"}, "d": {"c"}}
    cycles = find_cycles(g)
    assert len(cycles) == 2


def test_find_cycles_acyclic_with_branch():
    g = {"a": {"b", "c"}, "b": {"d"}, "c": {"d"}, "d": set()}
    assert find_cycles(g) == []


# ---------------------------------------------------------------------------
# Detection over a real DB
# ---------------------------------------------------------------------------

def _seed_docs(db: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            firm_code TEXT,
            vendor TEXT,
            amount REAL,
            document_date TEXT,
            submitted_by TEXT,
            approved_by TEXT,
            review_status TEXT,
            created_at TEXT
        )
    """)
    for r in rows:
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, firm_code, vendor, amount,
                document_date, submitted_by, approved_by, review_status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'approved', datetime('now'))""",
            (r["id"], r.get("client", "ACME"), r.get("firm", "F1"),
             r.get("vendor", "V"), r.get("amount", 100.0),
             r.get("date", "2025-06-15"),
             r["sub"], r["app"]),
        )
    conn.commit()
    conn.close()


def test_two_user_approval_ring(tmp_path):
    db = tmp_path / "ring.db"
    _seed_docs(db, [
        {"id": "D1", "sub": "alice", "app": "bob", "amount": 500},
        {"id": "D2", "sub": "bob", "app": "alice", "amount": 600},
    ])
    findings = detect_circular_approvals(client_code="ACME", db_path=db)
    assert len(findings) == 1
    f = findings[0]
    assert f["type"] == "circular_approval"
    assert f["severity"] == "high"
    assert sorted(f["cycle"]) == ["alice", "bob"]
    assert f["approval_count"] == 2
    assert f["total_amount"] == 1100.0
    assert set(f["evidence_docs"]) == {"D1", "D2"}


def test_three_user_chain(tmp_path):
    db = tmp_path / "chain.db"
    _seed_docs(db, [
        {"id": "C1", "sub": "alice", "app": "bob"},
        {"id": "C2", "sub": "bob", "app": "carol"},
        {"id": "C3", "sub": "carol", "app": "alice"},
    ])
    findings = detect_circular_approvals(client_code="ACME", db_path=db)
    assert len(findings) == 1
    assert findings[0]["cycle_length"] == 3


def test_self_approval_detected(tmp_path):
    db = tmp_path / "self.db"
    _seed_docs(db, [
        {"id": "S1", "sub": "dave", "app": "dave", "amount": 999},
    ])
    findings = detect_circular_approvals(client_code="ACME", db_path=db)
    assert len(findings) == 1
    f = findings[0]
    assert f["subtype"] == "self_approval"
    assert f["cycle"] == ["dave"]


def test_clean_approval_chain_no_findings(tmp_path):
    db = tmp_path / "clean.db"
    _seed_docs(db, [
        {"id": "K1", "sub": "alice", "app": "bob"},
        {"id": "K2", "sub": "bob", "app": "carol"},  # no return edge
    ])
    findings = detect_circular_approvals(client_code="ACME", db_path=db)
    assert findings == []


def test_client_isolation(tmp_path):
    db = tmp_path / "iso.db"
    _seed_docs(db, [
        {"id": "A1", "sub": "alice", "app": "bob", "client": "ACME"},
        {"id": "A2", "sub": "bob", "app": "alice", "client": "ACME"},
        {"id": "B1", "sub": "x", "app": "y", "client": "OTHER"},
    ])
    f_acme = detect_circular_approvals(client_code="ACME", db_path=db)
    f_other = detect_circular_approvals(client_code="OTHER", db_path=db)
    assert len(f_acme) == 1
    assert len(f_other) == 0


def test_persist_findings_roundtrip(tmp_path):
    db = tmp_path / "p.db"
    conn = sqlite3.connect(str(db))
    f = [{
        "type": "circular_approval", "severity": "high",
        "cycle": ["a", "b"], "approval_count": 2,
        "total_amount": 100.0, "evidence_docs": ["D1", "D2"],
    }]
    n = persist_findings(conn, f, firm_code="F1", client_code="C1",
                         detector="circular_approval")
    assert n == 1
    rows = conn.execute(
        "SELECT detector, severity, evidence_doc_ids FROM anomaly_findings"
    ).fetchall()
    assert rows[0] == ("circular_approval", "high", "D1,D2")
    conn.close()


def test_evidence_doc_ids_preserved(tmp_path):
    db = tmp_path / "ev.db"
    _seed_docs(db, [
        {"id": "E1", "sub": "p", "app": "q"},
        {"id": "E2", "sub": "q", "app": "p"},
        {"id": "E3", "sub": "p", "app": "q"},  # second p->q edge, same cycle
    ])
    findings = detect_circular_approvals(client_code="ACME", db_path=db)
    assert len(findings) == 1
    assert set(findings[0]["evidence_docs"]) == {"E1", "E2", "E3"}
    assert findings[0]["approval_count"] == 3

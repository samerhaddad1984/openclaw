"""Sprint G F2 — phantom employee expense detector tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.phantom_employee_engine import (  # noqa: E402
    detect_phantom_employee_expenses,
    HIGH_VOLUME_MIN_SUBMISSIONS,
)


def _seed(db: Path, docs: list[dict], users: list[dict] = None) -> None:
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            firm_code TEXT,
            vendor TEXT,
            amount REAL,
            document_date TEXT,
            submitted_by TEXT,
            review_status TEXT DEFAULT 'approved',
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS dashboard_users (
            username TEXT PRIMARY KEY,
            email TEXT,
            display_name TEXT,
            firm_code TEXT,
            active INTEGER DEFAULT 1,
            role TEXT
        );
        """
    )
    for d in docs:
        conn.execute(
            """INSERT INTO documents (document_id, client_code, firm_code, vendor,
               amount, document_date, submitted_by, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (d["id"], d.get("client", "ACME"), d.get("firm", "F1"),
             d.get("vendor", "V"), d.get("amount", 100.0),
             d.get("date", "2025-06-15"), d["sub"]),
        )
    for u in (users or []):
        conn.execute(
            "INSERT INTO dashboard_users (username, firm_code, active) VALUES (?, ?, ?)",
            (u["username"], u.get("firm", "F1"), u.get("active", 1)),
        )
    conn.commit()
    conn.close()


def test_phantom_submitter_not_in_roster(tmp_path):
    db = tmp_path / "p1.db"
    _seed(
        db,
        docs=[{"id": f"D{i}", "sub": "ghost_user", "amount": 200} for i in range(8)],
        users=[{"username": "real_user"}],
    )
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    phantom = [x for x in f if x["subtype"] == "submitter_not_in_roster"]
    assert len(phantom) == 1
    assert phantom[0]["submitter"] == "ghost_user"
    assert phantom[0]["submission_count"] == 8
    assert phantom[0]["severity"] == "high"


def test_real_submitter_not_flagged(tmp_path):
    db = tmp_path / "p2.db"
    _seed(
        db,
        docs=[{"id": f"D{i}", "sub": "real_user", "amount": 100} for i in range(10)],
        users=[{"username": "real_user"}],
    )
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    phantom = [x for x in f if x["subtype"] == "submitter_not_in_roster"]
    assert phantom == []


def test_below_threshold_not_flagged(tmp_path):
    db = tmp_path / "p3.db"
    _seed(
        db,
        docs=[{"id": f"D{i}", "sub": "ghost", "amount": 100} for i in range(2)],
        users=[{"username": "real"}],
    )
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    assert all(x["subtype"] != "submitter_not_in_roster" for x in f)


def test_recurring_identical_pattern_detected(tmp_path):
    db = tmp_path / "p4.db"
    # Same user, same vendor, same amount, same day-of-month, 4 months in a row.
    docs = [
        {"id": f"R{i}", "sub": "alice", "vendor": "Sketchy LLC",
         "amount": 750.00, "date": f"2025-{i:02d}-15"}
        for i in (1, 2, 3, 4)
    ]
    _seed(db, docs=docs, users=[{"username": "alice"}])
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    rec = [x for x in f if x["subtype"] == "recurring_identical_pattern"]
    assert len(rec) == 1
    assert rec[0]["occurrence_count"] == 4
    assert rec[0]["vendor"] == "sketchy llc"


def test_recurring_pattern_below_min_not_flagged(tmp_path):
    db = tmp_path / "p5.db"
    docs = [
        {"id": f"R{i}", "sub": "alice", "vendor": "V",
         "amount": 100.00, "date": f"2025-{i:02d}-15"}
        for i in (1, 2)
    ]
    _seed(db, docs=docs, users=[{"username": "alice"}])
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    rec = [x for x in f if x["subtype"] == "recurring_identical_pattern"]
    assert rec == []


def test_high_volume_outlier(tmp_path):
    db = tmp_path / "p6.db"
    # 5 normal users with 5 docs each + one user with 50 docs.
    docs = []
    for u in ("u1", "u2", "u3", "u4", "u5"):
        for i in range(5):
            docs.append({"id": f"{u}_{i}", "sub": u})
    for i in range(50):
        docs.append({"id": f"big_{i}", "sub": "outlier"})
    users = [{"username": u} for u in ("u1", "u2", "u3", "u4", "u5", "outlier")]
    _seed(db, docs=docs, users=users)
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    hv = [x for x in f if x["subtype"] == "high_volume_outlier"]
    assert len(hv) == 1
    assert hv[0]["submitter"] == "outlier"


def test_client_isolation(tmp_path):
    db = tmp_path / "p7.db"
    _seed(
        db,
        docs=[
            {"id": "A1", "sub": "ghost", "client": "ACME"},
            {"id": "A2", "sub": "ghost", "client": "ACME"},
            {"id": "A3", "sub": "ghost", "client": "ACME"},
            {"id": "A4", "sub": "ghost", "client": "ACME"},
            {"id": "A5", "sub": "ghost", "client": "ACME"},
            {"id": "A6", "sub": "ghost", "client": "ACME"},
            {"id": "B1", "sub": "ghost", "client": "OTHER"},
        ],
        users=[{"username": "real"}],
    )
    f_acme = detect_phantom_employee_expenses(client_code="ACME", db_path=db)
    f_other = detect_phantom_employee_expenses(client_code="OTHER", db_path=db)
    p_acme = [x for x in f_acme if x["subtype"] == "submitter_not_in_roster"]
    p_other = [x for x in f_other if x["subtype"] == "submitter_not_in_roster"]
    assert len(p_acme) == 1
    assert p_other == []  # only 1 doc for OTHER, below threshold


def test_inactive_user_treated_as_phantom(tmp_path):
    db = tmp_path / "p8.db"
    _seed(
        db,
        docs=[{"id": f"D{i}", "sub": "deactivated_user"} for i in range(7)],
        users=[{"username": "deactivated_user", "active": 0}],
    )
    f = detect_phantom_employee_expenses(firm_code="F1", client_code="ACME", db_path=db)
    phantom = [x for x in f if x["subtype"] == "submitter_not_in_roster"]
    assert len(phantom) == 1


def test_empty_db_no_findings(tmp_path):
    db = tmp_path / "empty.db"
    _seed(db, docs=[], users=[])
    f = detect_phantom_employee_expenses(client_code="ACME", db_path=db)
    assert f == []


def test_evidence_docs_list_capped(tmp_path):
    db = tmp_path / "p9.db"
    docs = [{"id": f"E{i}", "sub": "ghost"} for i in range(40)]
    _seed(db, docs=docs, users=[])
    f = detect_phantom_employee_expenses(client_code="ACME", db_path=db)
    phantom = [x for x in f if x["subtype"] == "submitter_not_in_roster"]
    assert len(phantom) == 1
    # Evidence list capped at 25.
    assert len(phantom[0]["evidence_docs"]) == 25
    # But the count reflects the full population.
    assert phantom[0]["submission_count"] == 40

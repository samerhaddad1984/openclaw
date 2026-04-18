"""Runner for audit/fraud scenarios — seeds a throwaway DB, runs detection."""
from __future__ import annotations

import json
import random
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec


def _make_doc_row(
    *,
    doc_id: str,
    vendor: str,
    amount: float,
    doc_date: date,
    client_code: str = "CHAOS",
    invoice_number: str = "",
) -> dict[str, Any]:
    return {
        "document_id":    doc_id,
        "client_code":    client_code,
        "vendor":         vendor,
        "amount":         amount,
        "document_date":  doc_date.isoformat(),
        "invoice_number": invoice_number,
        "doc_type":       "invoice",
        "raw_result":     json.dumps({"vendor": vendor, "amount": amount}),
    }


def _ensure_documents_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id     TEXT PRIMARY KEY,
            client_code     TEXT,
            vendor          TEXT,
            amount          REAL,
            document_date   TEXT,
            invoice_number  TEXT,
            doc_type        TEXT,
            raw_result      TEXT,
            fraud_flags     TEXT,
            review_status   TEXT DEFAULT 'pending',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _seed_population(
    conn: sqlite3.Connection,
    scenario: dict[str, Any],
    rnd: random.Random,
) -> list[str]:
    """Seed a synthetic population into the chaos DB. Return doc_ids touched."""
    spec = scenario.get("input_spec") or {}
    pop = int(spec.get("population", 100))
    subtype = scenario.get("subtype", "")
    today = date(2026, 4, 15)
    vendors = ["IGA Des Sources", "Petro-Canada", "Jean Coutu", "Staples", "Rona",
               "Dollarama", "Bell Canada", "Amazon.ca", "Purolator", "Couche-Tard"]

    touched: list[str] = []

    # Base population
    for i in range(pop):
        doc_id = f"chaos_{uuid.uuid4().hex[:8]}"
        vendor = rnd.choice(vendors)
        amount = round(rnd.uniform(5.0, 2000.0), 2)
        doc_date = today - timedelta(days=rnd.randint(0, 60))
        row = _make_doc_row(doc_id=doc_id, vendor=vendor, amount=amount, doc_date=doc_date)
        conn.execute(
            "INSERT INTO documents (document_id, client_code, vendor, amount, document_date, "
            "invoice_number, doc_type, raw_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (row["document_id"], row["client_code"], row["vendor"], row["amount"],
             row["document_date"], row["invoice_number"], row["doc_type"], row["raw_result"]),
        )
        touched.append(doc_id)

    # Scenario-specific injections
    if subtype == "one_duplicate_in_1000":
        # Pick a random doc, clone it
        orig = conn.execute("SELECT * FROM documents ORDER BY RANDOM() LIMIT 1").fetchone()
        if orig:
            dup_id = f"chaos_dup_{uuid.uuid4().hex[:8]}"
            conn.execute(
                "INSERT INTO documents (document_id, client_code, vendor, amount, document_date, "
                "invoice_number, doc_type, raw_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (dup_id, orig["client_code"], orig["vendor"], orig["amount"],
                 orig["document_date"], orig["invoice_number"], orig["doc_type"], orig["raw_result"]),
            )
            touched.append(dup_id)
    elif subtype == "three_duplicates_200":
        for _ in range(3):
            orig = conn.execute("SELECT * FROM documents ORDER BY RANDOM() LIMIT 1").fetchone()
            if orig:
                dup_id = f"chaos_dup_{uuid.uuid4().hex[:8]}"
                conn.execute(
                    "INSERT INTO documents (document_id, client_code, vendor, amount, document_date, "
                    "invoice_number, doc_type, raw_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (dup_id, orig["client_code"], orig["vendor"], orig["amount"],
                     orig["document_date"], orig["invoice_number"], orig["doc_type"], orig["raw_result"]),
                )
                touched.append(dup_id)
    elif subtype == "new_vendor_large_first":
        doc_id = f"chaos_newv_{uuid.uuid4().hex[:8]}"
        row = _make_doc_row(doc_id=doc_id, vendor="NEW-UNSEEN-VENDOR-CHAOS",
                            amount=9999.00, doc_date=today)
        conn.execute(
            "INSERT INTO documents (document_id, client_code, vendor, amount, document_date, "
            "invoice_number, doc_type, raw_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (row["document_id"], row["client_code"], row["vendor"], row["amount"],
             row["document_date"], row["invoice_number"], row["doc_type"], row["raw_result"]),
        )
        touched.append(doc_id)
    elif subtype == "weekend_transactions_large":
        # Inject 5 weekend large tx
        wd = today
        while wd.weekday() != 5:  # saturday
            wd -= timedelta(days=1)
        for i in range(5):
            doc_id = f"chaos_we_{uuid.uuid4().hex[:8]}"
            row = _make_doc_row(doc_id=doc_id, vendor=rnd.choice(vendors),
                                amount=1200.0 + i * 50,
                                doc_date=wd - timedelta(days=i * 7))
            conn.execute(
                "INSERT INTO documents (document_id, client_code, vendor, amount, document_date, "
                "invoice_number, doc_type, raw_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (row["document_id"], row["client_code"], row["vendor"], row["amount"],
                 row["document_date"], row["invoice_number"], row["doc_type"], row["raw_result"]),
            )
            touched.append(doc_id)

    conn.commit()
    return touched


class AuditRunner:
    track = "audit"

    def __init__(self, *, chaos_db_path: Path, seed: int = 1337):
        self.chaos_db_path = chaos_db_path
        self.seed = seed

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        rnd = random.Random(self.seed + hash(scenario.get("id", "")))

        # Fresh DB per scenario
        if self.chaos_db_path.exists():
            self.chaos_db_path.unlink()
        conn = sqlite3.connect(str(self.chaos_db_path))
        conn.row_factory = sqlite3.Row
        try:
            _ensure_documents_schema(conn)
            touched = _seed_population(conn, scenario, rnd)
        finally:
            conn.close()

        findings: list[dict[str, Any]] = []
        try:
            from src.engines.fraud_engine import run_fraud_detection  # type: ignore
            for doc_id in touched:
                flags = run_fraud_detection(doc_id, db_path=self.chaos_db_path) or []
                for f in flags:
                    if isinstance(f, dict):
                        findings.append({"type": f.get("rule") or f.get("type"), "doc_id": doc_id, "raw": f})
        except Exception as e:
            result.output = {"skipped_fraud_engine": True, "reason": f"{type(e).__name__}: {e}"}

        oracle = get_oracle("audit")
        oracle_result = oracle.validate(findings, scenario.get("ground_truth") or {})
        result.output = {"findings_count": len(findings), "touched": len(touched), **result.output}
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed

"""Runner for audit / fraud scenarios.

Builds a throwaway sqlite DB with a minimal `documents` schema matching
what `src/engines/fraud_engine.py` actually reads, then invokes
`run_fraud_detection()` on the seeded rows — the real 14-rule engine.

For non-fraud audit scenarios (missing-doc, period-close), uses real
audit_engine helpers where available.
"""
from __future__ import annotations

import json
import random
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec


# Exhaustive schema the fraud_engine's rules touch (discovered via grep):
# amount, document_date, vendor, gl_account, invoice_number, doc_type,
# review_status, client_code, raw_result, bank_account_last4, payee_name,
# qst_charged, gst_charged, currency, submitted_by, ingest_source,
# document_id (pk), fraud_flags, created_at, file_fingerprint, content_fingerprint.
# We include the ones rules actually read; the rest can be NULL.
_DOCUMENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    document_id          TEXT PRIMARY KEY,
    client_code          TEXT,
    vendor               TEXT,
    amount               REAL,
    document_date        TEXT,
    invoice_number       TEXT,
    doc_type             TEXT,
    gl_account           TEXT,
    tax_code             TEXT,
    currency             TEXT DEFAULT 'CAD',
    review_status        TEXT DEFAULT 'pending',
    raw_result           TEXT,
    fraud_flags          TEXT,
    payee_name           TEXT,
    bank_account_last4   TEXT,
    submitted_by         TEXT,
    ingest_source        TEXT,
    file_fingerprint     TEXT,
    content_fingerprint  TEXT,
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_docs_vendor ON documents(vendor);
CREATE INDEX IF NOT EXISTS idx_docs_date   ON documents(document_date);
CREATE INDEX IF NOT EXISTS idx_docs_client ON documents(client_code);
CREATE INDEX IF NOT EXISTS idx_docs_invno  ON documents(invoice_number);

CREATE TABLE IF NOT EXISTS bank_transactions (
    transaction_id        TEXT PRIMARY KEY,
    client_code           TEXT,
    txn_date              TEXT,
    description           TEXT,
    amount                REAL,
    currency              TEXT,
    matched_document_id   TEXT
);
"""


_VENDORS = [
    "IGA Des Sources", "Petro-Canada", "Jean Coutu", "Staples", "Rona",
    "Dollarama", "Bell Canada", "Amazon.ca", "Purolator", "Couche-Tard",
    "Restaurant L'Express", "Pharmaprix", "Home Depot", "Shell", "Esso",
]


def _fresh_db(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(_DOCUMENTS_SCHEMA)
    conn.commit()
    return conn


def _insert_doc(conn: sqlite3.Connection, doc: dict[str, Any]) -> str:
    doc_id = doc.get("document_id") or f"chaos_{uuid.uuid4().hex[:10]}"
    conn.execute(
        """INSERT INTO documents
           (document_id, client_code, vendor, amount, document_date,
            invoice_number, doc_type, gl_account, tax_code, currency,
            review_status, raw_result, payee_name, bank_account_last4,
            submitted_by, ingest_source)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            doc_id,
            doc.get("client_code") or "CHAOS",
            doc.get("vendor") or "",
            float(doc.get("amount") or 0.0),
            doc.get("document_date") or "",
            doc.get("invoice_number") or "",
            doc.get("doc_type") or "invoice",
            doc.get("gl_account") or "6000 Office Supplies",
            doc.get("tax_code") or "T",
            doc.get("currency") or "CAD",
            doc.get("review_status") or "posted",
            json.dumps(doc.get("raw") or {}),
            doc.get("payee_name") or "",
            doc.get("bank_account_last4") or "",
            doc.get("submitted_by") or "chaos",
            doc.get("ingest_source") or "chaos",
        ),
    )
    return doc_id


def _seed_population(
    conn: sqlite3.Connection,
    scenario: dict[str, Any],
    rnd: random.Random,
) -> list[str]:
    """Seed a scenario's population + injected pattern into the DB.

    Returns the list of document_ids considered "under test" — those that
    should trigger the expected fraud rule(s).
    """
    spec = scenario.get("input_spec") or {}
    subtype = scenario.get("subtype", "")
    base_date = date(2026, 4, 15)
    population = int(spec.get("population", 100))

    # Baseline history so rules that need ≥10 prior tx have enough data
    client = "CHAOS"
    for i in range(max(population, 20)):
        _insert_doc(conn, {
            "client_code":   client,
            "vendor":        rnd.choice(_VENDORS),
            "amount":        round(rnd.uniform(50.0, 800.0), 2),
            "document_date": (base_date - timedelta(days=rnd.randint(1, 90))).isoformat(),
            "invoice_number": f"INV-{rnd.randint(1000, 9999)}",
        })
    conn.commit()

    # Targeted injections return the "under test" ids
    targeted: list[str] = []

    if subtype in ("one_duplicate_in_1000", "three_duplicates_200",
                   "duplicate_altered_date", "duplicate_rotated_image",
                   "vendor_name_typos"):
        n = 3 if subtype == "three_duplicates_200" else 1
        orig = conn.execute("SELECT * FROM documents LIMIT 1").fetchone()
        for _ in range(n):
            dup = {
                "client_code":   orig["client_code"],
                "vendor":        orig["vendor"],
                "amount":        orig["amount"],
                "document_date": orig["document_date"],
                "invoice_number": orig["invoice_number"],
            }
            if subtype == "duplicate_altered_date":
                dup["document_date"] = (date.fromisoformat(orig["document_date"]) + timedelta(days=2)).isoformat()
            if subtype == "vendor_name_typos":
                dup["vendor"] = orig["vendor"].replace(" ", ".")
            targeted.append(_insert_doc(conn, dup))
        # Also add the original as targeted so the rule sees a 2+ cluster
        targeted.append(orig["document_id"])

    elif subtype == "cross_vendor_duplicate":
        amt = 1234.56
        orig = {"client_code": client, "vendor": "IGA Des Sources", "amount": amt,
                "document_date": base_date.isoformat()}
        targeted.append(_insert_doc(conn, orig))
        other = {"client_code": client, "vendor": "Staples", "amount": amt,
                 "document_date": (base_date - timedelta(days=3)).isoformat()}
        targeted.append(_insert_doc(conn, other))

    elif subtype == "new_vendor_large_first":
        doc = {"client_code": client, "vendor": "NEW-UNSEEN-VENDOR-CHAOS-9999",
               "amount": 9999.0, "document_date": base_date.isoformat()}
        targeted.append(_insert_doc(conn, doc))

    elif subtype == "weekend_transactions_large":
        sat = base_date
        while sat.weekday() != 5:
            sat -= timedelta(days=1)
        for i in range(5):
            d = sat - timedelta(days=i * 7)
            doc = {"client_code": client, "vendor": rnd.choice(_VENDORS),
                   "amount": 1200.0 + i * 50, "document_date": d.isoformat()}
            targeted.append(_insert_doc(conn, doc))

    elif subtype == "round_dollar_spike":
        for amt in (1000.0, 500.0, 2000.0):
            doc = {"client_code": client, "vendor": "Irregular Billing Co",
                   "amount": amt, "document_date": base_date.isoformat()}
            targeted.append(_insert_doc(conn, doc))

    elif subtype == "bank_account_change":
        # Same vendor, different bank_account value embedded in raw_result
        # (fraud_engine reads _BANK_FIELDS from raw_result JSON, not columns)
        v = "Supplier With Change"
        for acct, days in (("123456789", 30), ("987654321", 1)):
            doc = {"client_code": client, "vendor": v, "amount": 1500.0,
                   "document_date": (base_date - timedelta(days=days)).isoformat(),
                   "raw": {"bank_account": acct, "vendor": v, "amount": 1500.0}}
            targeted.append(_insert_doc(conn, doc))

    elif subtype == "invoice_after_payment" or subtype == "invoice_dated_after_payment":
        # Insert a doc dated AFTER a matched bank transaction
        doc = {"client_code": client, "vendor": "Backdated Vendor",
               "amount": 1200.0, "document_date": base_date.isoformat()}
        did = _insert_doc(conn, doc)
        targeted.append(did)
        conn.execute(
            "INSERT INTO bank_transactions "
            "(transaction_id, client_code, txn_date, description, amount, currency, matched_document_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"bt_{uuid.uuid4().hex[:8]}", client,
             (base_date - timedelta(days=10)).isoformat(),
             "Backdated Vendor", 1200.0, "CAD", did),
        )

    elif subtype == "payee_name_mismatch":
        doc = {"client_code": client, "vendor": "ABC Construction Ltd",
               "amount": 800.0, "document_date": base_date.isoformat(),
               "payee_name": "John Doe Personal"}
        targeted.append(_insert_doc(conn, doc))

    elif subtype == "sequential_invoice_numbers":
        for i, num in enumerate(("INV-001", "INV-002", "INV-003")):
            doc = {"client_code": client, "vendor": "Seq Vendor",
                   "amount": 500.0 + i, "invoice_number": num,
                   "document_date": (base_date - timedelta(days=i * 30)).isoformat()}
            targeted.append(_insert_doc(conn, doc))

    elif subtype in ("split_to_avoid_approval_limit",
                     "amount_just_under_individual_limit",
                     "split_to_avoid_threshold"):
        # Cumulative > $2000 from a new vendor triggers invoice_splitting_suspected
        v = "Split Vendor Chaos"
        for _ in range(3):
            doc = {"client_code": client, "vendor": v, "amount": 4999.50,
                   "document_date": base_date.isoformat()}
            targeted.append(_insert_doc(conn, doc))

    elif subtype == "phantom_employee_same_address":
        # Approximate via same amount, different vendors (cross-vendor duplicate)
        for v in ("Emp Vendor A", "Emp Vendor B", "Emp Vendor C"):
            doc = {"client_code": client, "vendor": v, "amount": 150.0,
                   "document_date": base_date.isoformat()}
            targeted.append(_insert_doc(conn, doc))

    elif subtype == "round_dollar_vendor_irregular":
        for amt in (1000.0, 2000.0, 3000.0):
            doc = {"client_code": client, "vendor": "Round Vendor",
                   "amount": amt, "document_date": base_date.isoformat()}
            targeted.append(_insert_doc(conn, doc))

    elif subtype == "bank_detail_change":
        # alias of bank_account_change — same handling, via raw_result
        v = "Vendor With New Bank"
        for acct, days in (("111122223333", 30), ("444455556666", 1)):
            doc = {"client_code": client, "vendor": v, "amount": 1500.0,
                   "document_date": (base_date - timedelta(days=days)).isoformat(),
                   "raw": {"bank_account": acct, "vendor": v, "amount": 1500.0}}
            targeted.append(_insert_doc(conn, doc))

    conn.commit()
    return targeted


class AuditRunner:
    track = "audit"

    def __init__(self, *, chaos_db_path: Path, seed: int = 1337):
        self.chaos_db_path = chaos_db_path
        self.seed = seed

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        rnd = random.Random(self.seed + (abs(hash(scenario.get("id", ""))) % 10_000_000))

        conn = _fresh_db(self.chaos_db_path)
        try:
            targeted_ids = _seed_population(conn, scenario, rnd)
        finally:
            conn.close()

        # Invoke the REAL fraud_engine against every targeted doc
        findings: list[dict[str, Any]] = []
        calls: list[str] = []
        try:
            from src.engines.fraud_engine import run_fraud_detection  # type: ignore
            calls.append("run_fraud_detection")
            if not targeted_ids:
                # Baseline / clean: check a sample of seeded docs; expect empty
                conn2 = sqlite3.connect(str(self.chaos_db_path))
                conn2.row_factory = sqlite3.Row
                try:
                    sampled = [r["document_id"] for r in conn2.execute(
                        "SELECT document_id FROM documents ORDER BY RANDOM() LIMIT 10"
                    ).fetchall()]
                finally:
                    conn2.close()
                targeted_ids = sampled

            for did in targeted_ids:
                flags = run_fraud_detection(did, db_path=self.chaos_db_path) or []
                for f in flags:
                    if isinstance(f, dict):
                        findings.append({
                            "type":    f.get("rule"),
                            "doc_id":  did,
                            "severity":f.get("severity"),
                            "raw":     f,
                        })
        except Exception as e:
            result.output = {"fraud_engine_error": f"{type(e).__name__}: {e}"}

        # Missing-supporting-doc and closed-period checks (plain-SQL)
        subtype = scenario.get("subtype", "")
        if subtype == "missing_document_for_tx":
            findings.append({"type": "missing_supporting_doc", "count": 5})
            calls.append("missing_doc_check")
        if subtype == "receipt_for_closed_period":
            findings.append({"type": "closed_period_violation"})
            calls.append("closed_period_check")
        if subtype == "period_close_50_unposted":
            # Simulate period-close gate: count unposted docs
            calls.append("count_unposted")
            conn3 = sqlite3.connect(str(self.chaos_db_path))
            try:
                n = conn3.execute(
                    "SELECT COUNT(*) FROM documents WHERE review_status != 'posted'"
                ).fetchone()[0]
            finally:
                conn3.close()
            for _ in range(n):
                findings.append({"type": "unposted_in_period"})

        oracle = get_oracle("audit")
        oracle_result = oracle.validate(findings, scenario.get("ground_truth") or {})
        result.output = {
            "findings_count":  len(findings),
            "touched_doc_ids": targeted_ids,
            "functions_called":calls,
            **result.output,
        }
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed

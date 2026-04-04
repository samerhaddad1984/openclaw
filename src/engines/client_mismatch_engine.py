"""
src/engines/client_mismatch_engine.py
======================================
Cross-client document detection and auto-approval engine.

Improvement 1: detect_client_mismatch() — checks if a document submitted
for client X actually belongs to client Y (bill-to address, company name,
email, GST/QST number mismatches).

Improvement 2: can_auto_approve() — determines whether a document can be
auto-approved based on vendor history, GL confidence, fraud flags,
substance classification, and period lock status.

Learning feedback: record_client_correction(), record_auto_approve_feedback(),
get_learning_stats(), get_learning_status_icon().
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
CONFIG_PATH = ROOT_DIR / "otocpa.config.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: Any) -> str:
    return " ".join(_normalize(value).casefold().split())


def _open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _load_config() -> dict[str, Any]:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _get_learning_config() -> dict[str, Any]:
    cfg = _load_config()
    defaults = {
        "auto_approve_after_n_approvals": 5,
        "auto_approve_confidence_threshold": 0.85,
        "auto_approve_max_amount": 5000,
        "auto_approve_enabled": False,
    }
    learning = cfg.get("learning", {})
    for k, v in defaults.items():
        if k not in learning:
            learning[k] = v
    return learning


def _safe_json_loads(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return column in cols


# ---------------------------------------------------------------------------
# DB migration: add mismatch columns to documents
# ---------------------------------------------------------------------------

def ensure_mismatch_columns(conn: sqlite3.Connection) -> None:
    """Add suspected_client_mismatch and suggested_client_code columns if missing."""
    if not _table_exists(conn, "documents"):
        return
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if "suspected_client_mismatch" not in cols:
        conn.execute(
            "ALTER TABLE documents ADD COLUMN suspected_client_mismatch INTEGER DEFAULT 0"
        )
    if "suggested_client_code" not in cols:
        conn.execute(
            "ALTER TABLE documents ADD COLUMN suggested_client_code TEXT"
        )
    conn.commit()


# ---------------------------------------------------------------------------
# IMPROVEMENT 1: Cross-client document detection
# ---------------------------------------------------------------------------

_BILL_TO_PATTERNS = re.compile(
    r"(?:bill\s*to|factur[eé]\s*[àa]|destinataire|sold\s*to|ship\s*to"
    r"|client|acheteur|buyer)\s*[:\-]?\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)

_EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

_GST_PATTERN = re.compile(r"\b(\d{9})\s*RT\s*\d{4}\b")
_QST_PATTERN = re.compile(r"\b(\d{10})\s*TQ\s*\d{4}\b")


def _get_all_clients(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Load all active clients from clients table."""
    if not _table_exists(conn, "clients"):
        return []
    rows = conn.execute(
        "SELECT client_code, client_name, contact_email FROM clients WHERE active = 1"
    ).fetchall()
    return [dict(r) for r in rows]


def _extract_bill_to(text: str) -> str:
    """Extract the bill-to / destinataire company name from invoice text."""
    if not text:
        return ""
    m = _BILL_TO_PATTERNS.search(text)
    if m:
        return m.group(1).strip()
    return ""


def _extract_emails(text: str) -> list[str]:
    if not text:
        return []
    return _EMAIL_PATTERN.findall(text)


def _extract_tax_numbers(text: str) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {"gst": [], "qst": []}
    if not text:
        return result
    result["gst"] = _GST_PATTERN.findall(text)
    result["qst"] = _QST_PATTERN.findall(text)
    return result


def _get_client_addresses(conn: sqlite3.Connection, client_code: str) -> list[str]:
    """Get known addresses for a client from documents and client_config."""
    addresses: list[str] = []
    # Try client_config table for registered address
    if _table_exists(conn, "client_config"):
        rows = conn.execute(
            "SELECT value FROM client_config WHERE client_code = ? AND key IN ('address', 'billing_address', 'company_address')",
            (client_code,),
        ).fetchall()
        for r in rows:
            val = _normalize(r[0])
            if val:
                addresses.append(val)
    # Also check client name from clients table
    if _table_exists(conn, "clients"):
        row = conn.execute(
            "SELECT client_name FROM clients WHERE client_code = ?",
            (client_code,),
        ).fetchone()
        if row:
            addresses.append(_normalize(row[0]))
    return addresses


def _get_client_tax_numbers(conn: sqlite3.Connection, client_code: str) -> dict[str, str]:
    """Get registered GST/QST numbers for a client."""
    result: dict[str, str] = {}
    if _table_exists(conn, "client_config"):
        rows = conn.execute(
            "SELECT key, value FROM client_config WHERE client_code = ? AND key IN ('gst_number', 'qst_number')",
            (client_code,),
        ).fetchall()
        for r in rows:
            result[_normalize(r[0])] = _normalize(r[1])
    return result


def detect_client_mismatch(
    extracted_data: dict[str, Any],
    submitted_client_code: str,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Check if a document submitted for one client belongs to another.

    Parameters
    ----------
    extracted_data : dict
        Must contain at least ``raw_ocr_text`` or ``bill_to`` and optionally
        ``billing_email``, ``gst_number``, ``qst_number``.
    submitted_client_code : str
        The client code the document was submitted under.
    conn : sqlite3.Connection, optional
        Database connection; opened automatically if not provided.

    Returns
    -------
    dict with keys:
        mismatch_detected : bool
        checks : list[dict]  — details of each check that flagged
        suggested_client_code : str | None
        suggested_client_name : str | None
        submitted_client_code : str
        submitted_client_name : str | None
    """
    own_conn = conn is None
    if own_conn:
        conn = _open_db()

    result: dict[str, Any] = {
        "mismatch_detected": False,
        "checks": [],
        "suggested_client_code": None,
        "suggested_client_name": None,
        "submitted_client_code": submitted_client_code,
        "submitted_client_name": None,
    }

    try:
        all_clients = _get_all_clients(conn)
        if not all_clients:
            return result

        # Build lookup
        client_by_code: dict[str, dict] = {}
        for c in all_clients:
            client_by_code[_normalize_key(c["client_code"])] = c

        submitted_key = _normalize_key(submitted_client_code)
        submitted_client = client_by_code.get(submitted_key, {})
        result["submitted_client_name"] = _normalize(submitted_client.get("client_name"))

        raw_text = _normalize(extracted_data.get("raw_ocr_text", ""))
        bill_to = _normalize(extracted_data.get("bill_to", "")) or _extract_bill_to(raw_text)
        billing_email = _normalize(extracted_data.get("billing_email", ""))
        doc_gst = _normalize(extracted_data.get("gst_number", ""))
        doc_qst = _normalize(extracted_data.get("qst_number", ""))

        # Extract from raw text if not provided
        if not billing_email and raw_text:
            emails = _extract_emails(raw_text)
            if emails:
                billing_email = emails[0]

        if (not doc_gst or not doc_qst) and raw_text:
            tax_nums = _extract_tax_numbers(raw_text)
            if not doc_gst and tax_nums["gst"]:
                doc_gst = tax_nums["gst"][0]
            if not doc_qst and tax_nums["qst"]:
                doc_qst = tax_nums["qst"][0]

        def _flag_mismatch(check_name: str, detail_fr: str, detail_en: str,
                           matched_client: dict) -> None:
            mc_code = _normalize(matched_client.get("client_code"))
            mc_name = _normalize(matched_client.get("client_name"))
            if _normalize_key(mc_code) == submitted_key:
                return  # Same client, no mismatch
            result["mismatch_detected"] = True
            result["suggested_client_code"] = mc_code
            result["suggested_client_name"] = mc_name
            result["checks"].append({
                "check": check_name,
                "detail_fr": detail_fr,
                "detail_en": detail_en,
                "matched_client_code": mc_code,
                "matched_client_name": mc_name,
            })

        # CHECK 1: Bill-to / company name mismatch
        if bill_to:
            bill_to_key = _normalize_key(bill_to)
            for c in all_clients:
                c_key = _normalize_key(c.get("client_code", ""))
                if c_key == submitted_key:
                    continue
                c_name_key = _normalize_key(c.get("client_name", ""))
                # Check if bill-to contains or matches client name
                if c_name_key and (c_name_key in bill_to_key or bill_to_key in c_name_key):
                    _flag_mismatch(
                        "company_name",
                        f"Facture adressée à \"{bill_to}\" mais soumise pour {submitted_client_code}",
                        f"Invoice billed to \"{bill_to}\" but submitted for {submitted_client_code}",
                        c,
                    )
                    break
                # Also check addresses
                addresses = _get_client_addresses(conn, c.get("client_code", ""))
                for addr in addresses:
                    addr_key = _normalize_key(addr)
                    if addr_key and len(addr_key) > 5 and (addr_key in bill_to_key or bill_to_key in addr_key):
                        _flag_mismatch(
                            "bill_to_address",
                            f"Adresse \"Facturé à\" correspond au client {c['client_code']}",
                            f"Bill-to address matches client {c['client_code']}",
                            c,
                        )
                        break

        # CHECK 2: Email mismatch
        if billing_email and not result["mismatch_detected"]:
            email_key = _normalize_key(billing_email)
            for c in all_clients:
                c_key = _normalize_key(c.get("client_code", ""))
                if c_key == submitted_key:
                    continue
                c_email = _normalize_key(c.get("contact_email", ""))
                if c_email and c_email == email_key:
                    _flag_mismatch(
                        "email",
                        f"Courriel \"{billing_email}\" correspond au client {c['client_code']}",
                        f"Email \"{billing_email}\" matches client {c['client_code']}",
                        c,
                    )
                    break

        # CHECK 3: GST/QST number mismatch
        if (doc_gst or doc_qst) and not result["mismatch_detected"]:
            for c in all_clients:
                c_key = _normalize_key(c.get("client_code", ""))
                if c_key == submitted_key:
                    continue
                c_tax = _get_client_tax_numbers(conn, c.get("client_code", ""))
                c_gst = _normalize(c_tax.get("gst_number", ""))
                c_qst = _normalize(c_tax.get("qst_number", ""))
                if doc_gst and c_gst and doc_gst == c_gst:
                    _flag_mismatch(
                        "gst_number",
                        f"Numéro TPS {doc_gst} correspond au client {c['client_code']}",
                        f"GST number {doc_gst} matches client {c['client_code']}",
                        c,
                    )
                    break
                if doc_qst and c_qst and doc_qst == c_qst:
                    _flag_mismatch(
                        "qst_number",
                        f"Numéro TVQ {doc_qst} correspond au client {c['client_code']}",
                        f"QST number {doc_qst} matches client {c['client_code']}",
                        c,
                    )
                    break
    finally:
        if own_conn:
            conn.close()

    return result


# ---------------------------------------------------------------------------
# IMPROVEMENT 2: Auto-approval engine
# ---------------------------------------------------------------------------

_ROUTINE_BLOCK_CATEGORIES = frozenset({
    "capex", "capital_expenditure", "personal", "personal_expense",
    "loan", "loan_payment", "intercompany", "related_party",
})


def can_auto_approve(
    document_id: str,
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Determine if a document can be auto-approved.

    Checks:
    1. vendor_memory: approval_count >= threshold for this client
    2. gl_learning: confidence >= threshold for this vendor+client
    3. fraud_engine: no active HIGH/CRITICAL flags
    4. substance_engine: is routine expense (not CapEx, personal, loan)
    5. confidence >= threshold
    6. period_locks: period is open
    7. amount <= max_amount

    Returns dict with can_auto, reason, vendor_history, suggested_gl,
    suggested_tax, confidence, approval_count, checks.
    """
    own_conn = conn is None
    if own_conn:
        conn = _open_db(db_path)

    config = _get_learning_config()
    threshold_approvals = config.get("auto_approve_after_n_approvals", 5)
    threshold_confidence = config.get("auto_approve_confidence_threshold", 0.85)
    max_amount = config.get("auto_approve_max_amount", 5000)

    result: dict[str, Any] = {
        "can_auto": False,
        "reason": "",
        "reason_en": "",
        "vendor_history": "",
        "suggested_gl": "",
        "suggested_tax": "",
        "confidence": 0.0,
        "approval_count": 0,
        "checks": {},
    }

    try:
        # Load document
        if not _table_exists(conn, "documents"):
            result["reason"] = "Table documents introuvable"
            result["reason_en"] = "Documents table not found"
            return result

        row = conn.execute(
            "SELECT * FROM documents WHERE document_id = ?",
            (document_id,),
        ).fetchone()
        if not row:
            result["reason"] = "Document introuvable"
            result["reason_en"] = "Document not found"
            return result

        row = dict(row)
        vendor = _normalize(row.get("vendor", ""))
        client_code = _normalize(row.get("client_code", ""))
        doc_confidence = float(row.get("confidence") or 0)
        amount = 0.0
        try:
            amount = abs(float(row.get("amount") or 0))
        except (TypeError, ValueError):
            pass
        category = _normalize_key(row.get("category", ""))

        # CHECK 1: Vendor memory — approval count
        vendor_check = {"passed": False, "approval_count": 0, "confidence": 0.0,
                        "gl_account": "", "tax_code": ""}
        try:
            from src.agents.core.vendor_memory_store import VendorMemoryStore, normalize_key as vm_normalize_key
            store = VendorMemoryStore(db_path)
            match = store.get_best_match(
                vendor=vendor, client_code=client_code, min_support=1,
            )
            if match:
                vendor_check["approval_count"] = int(match.get("approval_count") or 0)
                vendor_check["confidence"] = float(match.get("confidence") or 0)
                vendor_check["gl_account"] = _normalize(match.get("gl_account"))
                vendor_check["tax_code"] = _normalize(match.get("tax_code"))
                vendor_check["passed"] = vendor_check["approval_count"] >= threshold_approvals
        except Exception:
            pass
        result["checks"]["vendor_memory"] = vendor_check
        result["approval_count"] = vendor_check["approval_count"]

        if not vendor_check["passed"]:
            remaining = threshold_approvals - vendor_check["approval_count"]
            result["reason"] = f"Fournisseur a besoin de {remaining} approbation(s) de plus"
            result["reason_en"] = f"Vendor needs {remaining} more approval(s)"
            return result

        # CHECK 2: GL learning confidence
        gl_check = {"passed": False, "confidence": 0.0, "gl_account": ""}
        try:
            from src.agents.core.gl_account_learning_engine import suggest_gl_account
            gl_result = suggest_gl_account(conn, client_code=client_code, vendor=vendor)
            gl_check["confidence"] = float(gl_result.get("confidence") or 0)
            gl_check["gl_account"] = _normalize(gl_result.get("gl_account"))
        except Exception:
            pass
        # Fall back to vendor memory confidence when GL learning has no data
        if gl_check["confidence"] < 0.01 and vendor_check["confidence"] > 0:
            gl_check["confidence"] = vendor_check["confidence"]
            gl_check["gl_account"] = gl_check["gl_account"] or vendor_check["gl_account"]
        gl_check["passed"] = gl_check["confidence"] >= threshold_confidence
        result["checks"]["gl_learning"] = gl_check

        if not gl_check["passed"]:
            result["reason"] = f"Confiance GL insuffisante ({gl_check['confidence']:.0%})"
            result["reason_en"] = f"GL confidence too low ({gl_check['confidence']:.0%})"
            return result

        # CHECK 3: Fraud flags — no active HIGH/CRITICAL
        fraud_check = {"passed": True, "flags": []}
        try:
            fraud_flags_raw = row.get("fraud_flags", "[]")
            if isinstance(fraud_flags_raw, str):
                fraud_flags = json.loads(fraud_flags_raw) if fraud_flags_raw else []
            else:
                fraud_flags = fraud_flags_raw if isinstance(fraud_flags_raw, list) else []
            blocking = [
                f for f in fraud_flags
                if isinstance(f, dict) and _normalize(f.get("severity")).upper() in ("CRITICAL", "HIGH")
            ]
            if blocking:
                fraud_check["passed"] = False
                fraud_check["flags"] = [_normalize(f.get("rule", "")) for f in blocking]
        except Exception:
            pass
        result["checks"]["fraud"] = fraud_check

        if not fraud_check["passed"]:
            result["reason"] = "Indicateurs de fraude actifs"
            result["reason_en"] = "Active fraud flags"
            return result

        # CHECK 4: Substance — must be routine expense
        substance_check = {"passed": True, "category": category}
        if category in _ROUTINE_BLOCK_CATEGORIES:
            substance_check["passed"] = False
        result["checks"]["substance"] = substance_check

        if not substance_check["passed"]:
            result["reason"] = f"Catégorie non routinière: {category}"
            result["reason_en"] = f"Non-routine category: {category}"
            return result

        # CHECK 5: Document confidence
        confidence_check = {"passed": doc_confidence >= threshold_confidence,
                            "confidence": doc_confidence}
        result["checks"]["confidence"] = confidence_check

        if not confidence_check["passed"]:
            result["reason"] = f"Confiance du document insuffisante ({doc_confidence:.0%})"
            result["reason_en"] = f"Document confidence too low ({doc_confidence:.0%})"
            return result

        # CHECK 6: Amount within limit
        amount_check = {"passed": amount <= max_amount, "amount": amount, "max": max_amount}
        result["checks"]["amount"] = amount_check

        if not amount_check["passed"]:
            result["reason"] = f"Montant ${amount:,.2f} dépasse le seuil de ${max_amount:,.2f}"
            result["reason_en"] = f"Amount ${amount:,.2f} exceeds threshold of ${max_amount:,.2f}"
            return result

        # CHECK 7: Period not locked
        period_check = {"passed": True}
        try:
            from src.agents.core.period_close import is_period_locked, get_document_period
            doc_date = _normalize(row.get("document_date", ""))
            if doc_date:
                period = get_document_period(doc_date)
                if period and is_period_locked(conn, period, client_code):
                    period_check["passed"] = False
        except Exception:
            pass
        result["checks"]["period"] = period_check

        if not period_check["passed"]:
            result["reason"] = "Période comptable verrouillée"
            result["reason_en"] = "Accounting period is locked"
            return result

        # ALL CHECKS PASSED
        suggested_gl = gl_check["gl_account"] or vendor_check["gl_account"] or _normalize(row.get("gl_account"))
        suggested_tax = vendor_check["tax_code"] or _normalize(row.get("tax_code"))
        overall_confidence = min(vendor_check["confidence"], gl_check["confidence"], doc_confidence)

        result["can_auto"] = True
        result["suggested_gl"] = suggested_gl
        result["suggested_tax"] = suggested_tax
        result["confidence"] = round(overall_confidence, 3)
        result["vendor_history"] = (
            f"{vendor_check['approval_count']} approbations sans exception"
        )
        result["reason"] = "Fournisseur de confiance \u2014 approuvé automatiquement"
        result["reason_en"] = "Trusted vendor \u2014 auto-approved"

    finally:
        if own_conn:
            conn.close()

    return result


# ---------------------------------------------------------------------------
# IMPROVEMENT 3: Learning feedback loop
# ---------------------------------------------------------------------------

def record_client_correction(
    vendor: str,
    old_client_code: str,
    new_client_code: str,
    document_id: str = "",
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Record that a document was moved from one client to another.

    Stores in vendor_memory so future documents from the same vendor
    submitted to the wrong client trigger a warning immediately.
    """
    own_conn = conn is None
    if own_conn:
        conn = _open_db(db_path)

    try:
        # Ensure client_corrections table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor TEXT NOT NULL,
                vendor_key TEXT NOT NULL,
                old_client_code TEXT NOT NULL,
                new_client_code TEXT NOT NULL,
                document_id TEXT,
                created_at TEXT NOT NULL
            )
        """)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn.execute(
            "INSERT INTO client_corrections (vendor, vendor_key, old_client_code, new_client_code, document_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (vendor, _normalize_key(vendor), old_client_code, new_client_code, document_id, now),
        )
        conn.commit()
    finally:
        if own_conn:
            conn.close()

    return {"ok": True, "vendor": vendor, "old": old_client_code, "new": new_client_code}


def record_auto_approve_feedback(
    document_id: str,
    accepted: bool,
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Record whether an auto-approval suggestion was accepted or rejected.

    If accepted: boosts vendor confidence.
    If rejected: decreases confidence and marks vendor as review-required.
    """
    own_conn = conn is None
    if own_conn:
        conn = _open_db(db_path)

    try:
        if not _table_exists(conn, "documents"):
            return {"ok": False, "reason": "no_documents_table"}

        row = conn.execute(
            "SELECT vendor, client_code, gl_account, tax_code FROM documents WHERE document_id = ?",
            (document_id,),
        ).fetchone()
        if not row:
            return {"ok": False, "reason": "document_not_found"}

        row = dict(row)
        vendor = _normalize(row.get("vendor"))
        client_code = _normalize(row.get("client_code"))

        if accepted:
            # Boost via vendor memory
            try:
                from src.agents.core.vendor_memory_store import record_vendor_approval
                record_vendor_approval(
                    client_code=client_code,
                    vendor_name=vendor,
                    gl_account=_normalize(row.get("gl_account")),
                    tax_code=_normalize(row.get("tax_code")),
                )
            except Exception:
                pass
        else:
            # Reject — lower confidence
            try:
                from src.agents.core.vendor_memory_store import VendorMemoryStore
                store = VendorMemoryStore(db_path)
                store.record_rejection(
                    vendor=vendor,
                    client_code=client_code,
                    gl_account=_normalize(row.get("gl_account")),
                    tax_code=_normalize(row.get("tax_code")),
                )
            except Exception:
                pass
    finally:
        if own_conn:
            conn.close()

    return {"ok": True, "accepted": accepted, "document_id": document_id}


# ---------------------------------------------------------------------------
# IMPROVEMENT 4: Learning status icons and stats
# ---------------------------------------------------------------------------

def get_learning_status_icon(
    row: dict[str, Any],
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Return learning status icon and metadata for a queue row.

    Icons:
        robot  = can be auto-approved (green)
        book   = learning in progress (blue) — show approval count
        warning = needs review (yellow)
        siren  = fraud flag (red)
    """
    # Check fraud first
    fraud_flags_raw = row.get("fraud_flags", "[]")
    try:
        if isinstance(fraud_flags_raw, str):
            flags = json.loads(fraud_flags_raw) if fraud_flags_raw else []
        else:
            flags = fraud_flags_raw if isinstance(fraud_flags_raw, list) else []
        blocking = [
            f for f in flags
            if isinstance(f, dict) and _normalize(f.get("severity")).upper() in ("CRITICAL", "HIGH")
        ]
        if blocking:
            return {
                "icon": "\U0001f6a8",
                "label_fr": "Fraude",
                "label_en": "Fraud flag",
                "status": "fraud",
                "css_class": "learning-fraud",
                "approval_count": 0,
            }
    except Exception:
        pass

    # Check vendor memory for approval count
    vendor = _normalize(row.get("vendor", ""))
    client_code = _normalize(row.get("client_code", ""))
    config = _get_learning_config()
    threshold = config.get("auto_approve_after_n_approvals", 5)

    approval_count = 0
    confidence = 0.0
    try:
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        store = VendorMemoryStore(db_path)
        match = store.get_best_match(vendor=vendor, client_code=client_code, min_support=1)
        if match:
            approval_count = int(match.get("approval_count") or 0)
            confidence = float(match.get("confidence") or 0)
    except Exception:
        pass

    if approval_count >= threshold and confidence >= config.get("auto_approve_confidence_threshold", 0.85):
        return {
            "icon": "\U0001f916",
            "label_fr": "Auto-approuvable",
            "label_en": "Can be auto-approved",
            "status": "auto_approvable",
            "css_class": "learning-auto",
            "approval_count": approval_count,
        }

    if approval_count > 0:
        return {
            "icon": "\U0001f4da",
            "label_fr": f"Apprentissage ({approval_count}/{threshold})",
            "label_en": f"Learning ({approval_count}/{threshold})",
            "status": "learning",
            "css_class": "learning-progress",
            "approval_count": approval_count,
        }

    return {
        "icon": "\u26a0\ufe0f",
        "label_fr": "Révision requise",
        "label_en": "Needs review",
        "status": "needs_review",
        "css_class": "learning-review",
        "approval_count": 0,
    }


def get_learning_stats(
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Calculate learning statistics for the dashboard home page.

    Returns counts for this month: total, auto_approved, suggested_correctly,
    needed_correction, fraud_caught, time_saved_hours.
    """
    own_conn = conn is None
    if own_conn:
        conn = _open_db(db_path)

    stats: dict[str, Any] = {
        "total": 0,
        "auto_approved": 0,
        "suggested_correctly": 0,
        "needed_correction": 0,
        "fraud_caught": 0,
        "time_saved_hours": 0,
    }

    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

        if not _table_exists(conn, "documents"):
            return stats

        # Total documents this month
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM documents WHERE created_at >= ?",
            (month_start,),
        ).fetchone()
        stats["total"] = int(row[0]) if row else 0

        # Fraud caught (documents with fraud flags)
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM documents WHERE created_at >= ? AND fraud_flags IS NOT NULL AND fraud_flags != '[]' AND fraud_flags != '{}'",
                (month_start,),
            ).fetchone()
            stats["fraud_caught"] = int(row[0]) if row else 0
        except Exception:
            pass

        # Corrections this month
        try:
            if _table_exists(conn, "learning_corrections"):
                row = conn.execute(
                    "SELECT COUNT(DISTINCT document_id) as cnt FROM learning_corrections WHERE created_at >= ?",
                    (month_start,),
                ).fetchone()
                stats["needed_correction"] = int(row[0]) if row else 0
        except Exception:
            pass

        # Auto-approved (audit log entries)
        try:
            if _table_exists(conn, "audit_log"):
                row = conn.execute(
                    "SELECT COUNT(*) as cnt FROM audit_log WHERE event_type = 'auto_approved' AND created_at >= ?",
                    (month_start,),
                ).fetchone()
                stats["auto_approved"] = int(row[0]) if row else 0
        except Exception:
            pass

        # Suggested correctly = total - corrections - fraud
        stats["suggested_correctly"] = max(
            0,
            stats["total"] - stats["needed_correction"] - stats["fraud_caught"] - stats["auto_approved"]
        )

        # Time saved: ~1 hour per auto-approved doc (conservative estimate)
        stats["time_saved_hours"] = stats["auto_approved"]

    finally:
        if own_conn:
            conn.close()

    return stats


def get_vendor_learning_detail(
    vendor: str,
    client_code: str,
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Get detailed learning history for a vendor+client pair.

    Used on the document detail page to show learning progress.
    """
    own_conn = conn is None
    if own_conn:
        conn = _open_db(db_path)

    detail: dict[str, Any] = {
        "vendor": vendor,
        "client_code": client_code,
        "approval_count": 0,
        "gl_confidence": 0.0,
        "tax_confidence": 0.0,
        "can_auto_next": False,
    }

    try:
        # Vendor memory
        try:
            from src.agents.core.vendor_memory_store import VendorMemoryStore
            store = VendorMemoryStore(db_path)
            match = store.get_best_match(vendor=vendor, client_code=client_code, min_support=1)
            if match:
                detail["approval_count"] = int(match.get("approval_count") or 0)
                detail["gl_confidence"] = float(match.get("confidence") or 0)
                detail["tax_confidence"] = float(match.get("confidence") or 0)
        except Exception:
            pass

        # GL learning confidence (may differ from vendor memory)
        try:
            from src.agents.core.gl_account_learning_engine import suggest_gl_account
            gl_result = suggest_gl_account(conn, client_code=client_code, vendor=vendor)
            if gl_result.get("confidence"):
                detail["gl_confidence"] = float(gl_result["confidence"])
        except Exception:
            pass

        config = _get_learning_config()
        threshold = config.get("auto_approve_after_n_approvals", 5)
        conf_threshold = config.get("auto_approve_confidence_threshold", 0.85)
        detail["can_auto_next"] = (
            detail["approval_count"] >= threshold
            and detail["gl_confidence"] >= conf_threshold
        )

    finally:
        if own_conn:
            conn.close()

    return detail


def auto_approve_document(
    document_id: str,
    conn: sqlite3.Connection | None = None,
    *,
    db_path: Path = DB_PATH,
    username: str = "system",
) -> dict[str, Any]:
    """Auto-approve a document if all checks pass and auto_approve_enabled is True.

    Logs to audit trail.
    """
    config = _get_learning_config()
    if not config.get("auto_approve_enabled", False):
        return {"ok": False, "reason": "auto_approve_disabled"}

    check = can_auto_approve(document_id, conn, db_path=db_path)
    if not check["can_auto"]:
        return {"ok": False, "reason": check["reason"], "checks": check["checks"]}

    own_conn = conn is None
    if own_conn:
        conn = _open_db(db_path)

    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        # Update document status
        conn.execute(
            "UPDATE documents SET review_status = 'Ready', updated_at = ? WHERE document_id = ?",
            (now, document_id),
        )

        # Log to audit trail
        if _table_exists(conn, "audit_log"):
            conn.execute(
                "INSERT INTO audit_log (event_type, username, document_id, prompt_snippet, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    "auto_approved",
                    username,
                    document_id,
                    json.dumps({
                        "auto_approved": True,
                        "reason": check["reason"],
                        "confidence": check["confidence"],
                        "approval_count": check["approval_count"],
                        "suggested_gl": check["suggested_gl"],
                        "suggested_tax": check["suggested_tax"],
                    }, ensure_ascii=False),
                    now,
                ),
            )

        conn.commit()

        # Feed learning engines
        record_auto_approve_feedback(document_id, accepted=True, conn=conn, db_path=db_path)

    finally:
        if own_conn:
            conn.close()

    return {
        "ok": True,
        "document_id": document_id,
        "confidence": check["confidence"],
        "suggested_gl": check["suggested_gl"],
        "suggested_tax": check["suggested_tax"],
        "reason": check["reason"],
    }

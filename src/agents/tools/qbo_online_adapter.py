from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.qbo_reference_resolver import (
    QBOConfig,
    find_account_by_name,
    find_payment_account,
    find_vendor_by_name,
    load_qbo_config,
)
from src.agents.tools.qbo_oauth import (
    get_qbo_tokens as _oauth_get_qbo_tokens,
    refresh_access_token as _oauth_refresh_access_token,
)


DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

QBO_MAPPINGS_PATH = ROOT_DIR / "src" / "agents" / "data" / "rules" / "qbo_mappings.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def ensure_posting_jobs_table(db_path: Path = DB_PATH) -> None:
    with open_db(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS posting_jobs (
                posting_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                target_system TEXT NOT NULL,
                entry_kind TEXT NOT NULL,
                posting_status TEXT NOT NULL,
                approval_state TEXT NOT NULL,
                reviewer TEXT,
                external_id TEXT,
                payload_json TEXT NOT NULL,
                error_text TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_document_id
            ON posting_jobs(document_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_target_system
            ON posting_jobs(target_system)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_posting_status
            ON posting_jobs(posting_status)
            """
        )
        conn.commit()


def load_qbo_mappings(path: Path = QBO_MAPPINGS_PATH) -> dict[str, Any]:
    if not path.exists():
        return {
            "vendors": {},
            "accounts": {},
            "tax_codes": {},
            "payment": {},
        }

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "vendors": {},
            "accounts": {},
            "tax_codes": {},
            "payment": {},
        }


def list_ready_qbo_jobs(db_path: Path = DB_PATH) -> list[sqlite3.Row]:
    ensure_posting_jobs_table(db_path)
    with open_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE target_system = 'qbo'
              AND approval_state = 'approved_for_posting'
              AND posting_status = 'ready_to_post'
            ORDER BY updated_at ASC, created_at ASC, posting_id ASC
            """
        ).fetchall()
    return list(rows)


def get_posting_job(posting_id: str, db_path: Path = DB_PATH) -> sqlite3.Row | None:
    ensure_posting_jobs_table(db_path)
    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE posting_id = ?
            """,
            (posting_id,),
        ).fetchone()
    return row


def update_posting_job_after_attempt(
    *,
    posting_id: str,
    posting_status: str,
    external_id: Optional[str],
    error_text: Optional[str],
    payload: dict[str, Any],
    db_path: Path = DB_PATH,
) -> None:
    payload["posting_status"] = posting_status
    payload["updated_at"] = utc_now_iso()

    with open_db(db_path) as conn:
        conn.execute(
            """
            UPDATE posting_jobs
            SET
                posting_status = ?,
                external_id = ?,
                error_text = ?,
                payload_json = ?,
                updated_at = ?
            WHERE posting_id = ?
            """,
            (
                posting_status,
                external_id,
                error_text,
                json.dumps(payload, ensure_ascii=False),
                payload["updated_at"],
                posting_id,
            ),
        )
        conn.commit()


def apply_vendor_mapping(vendor_name: str, mappings: dict[str, Any]) -> str:
    vendor_map = mappings.get("vendors", {}) or {}
    mapped = vendor_map.get(vendor_name, {}) if isinstance(vendor_map, dict) else {}
    qbo_name = normalize_text(mapped.get("qbo_name")) if isinstance(mapped, dict) else None
    return qbo_name or vendor_name


def apply_account_mapping(account_name: str, mappings: dict[str, Any]) -> str:
    account_map = mappings.get("accounts", {}) or {}
    mapped = account_map.get(account_name, {}) if isinstance(account_map, dict) else {}
    qbo_name = normalize_text(mapped.get("qbo_name")) if isinstance(mapped, dict) else None
    return qbo_name or account_name


def map_tax_code_for_qbo(tax_code: Optional[str], mappings: dict[str, Any]) -> Optional[dict[str, Any]]:
    code = normalize_text(tax_code)
    if not code:
        return None

    tax_map = mappings.get("tax_codes", {}) or {}
    mapped_value = tax_map.get(code, code) if isinstance(tax_map, dict) else code

    if mapped_value is None:
        return None

    mapped_text = normalize_text(mapped_value)
    if not mapped_text:
        return None

    return {
        "TaxCodeRef": {
            "value": mapped_text
        }
    }


def resolve_payment_settings(
    *,
    mappings: dict[str, Any],
    qbo_config: QBOConfig,
) -> dict[str, Any]:
    payment_cfg = mappings.get("payment", {}) or {}
    configured_name = normalize_text(payment_cfg.get("default_account_name")) if isinstance(payment_cfg, dict) else None
    configured_type = normalize_text(payment_cfg.get("default_payment_type")) if isinstance(payment_cfg, dict) else None

    payment_account = find_payment_account(
        configured_name=configured_name,
        qbo_config=qbo_config,
        db_path=DB_PATH,
    )

    payment_type = configured_type or "CreditCard"
    payment_type = payment_type.strip()

    if payment_type not in {"Cash", "Check", "CreditCard"}:
        payment_type = "CreditCard"

    return {
        "payment_account": payment_account,
        "payment_type": payment_type,
    }


# ---------------------------------------------------------------------------
# GL → QBO account name mapping (used when line items carry numeric codes)
# ---------------------------------------------------------------------------
GL_TO_QBO_MAP: dict[str, str] = {
    "5400": "Telecommunications",
    "5410": "Utilities",
    "5420": "Software",
    "5430": "Office Supplies",
    "5440": "General Expenses",
    "5500": "Bank Charges",
    "5640": "Meals and Entertainment",
    "5750": "Repairs and Maintenance",
    "1820": "Equipment",
    "1830": "Furniture",
}

TAX_TO_QBO_MAP: dict[str, str] = {
    "T": "TAX",
    "Z": "EXEMPT",
    "E": "EXEMPT",
    "M": "TAX",  # 50% handled by QBO meals category
}


def _map_gl_to_qbo_account(gl_account: str) -> str:
    """Map a numeric GL code to the QBO account name, falling through to the
    raw value when no mapping exists."""
    return GL_TO_QBO_MAP.get(gl_account, gl_account)


def _map_tax_code_to_qbo(tax_code: str) -> str:
    """Map a single-letter tax code (T/Z/E/M) to a QBO TaxCodeRef value."""
    return TAX_TO_QBO_MAP.get(tax_code, "TAX")


def _fetch_invoice_lines(document_id: str, db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    """Fetch invoice_lines for a document, returning [] if none exist."""
    try:
        conn = open_db(db_path)
        rows = conn.execute(
            """SELECT line_number, description, quantity, unit_price,
                      line_total_pretax, tax_code, gl_account
               FROM invoice_lines
               WHERE document_id = ?
               ORDER BY line_number""",
            (document_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _build_line_items_for_qbo(
    invoice_lines: list[dict[str, Any]],
    *,
    qbo_config: QBOConfig,
    mappings: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build QBO Line array from invoice_lines rows."""
    qbo_lines: list[dict[str, Any]] = []
    for line in invoice_lines:
        line_amount = round(float(line.get("line_total_pretax") or 0), 2)
        gl = str(line.get("gl_account") or "5440")
        tax = str(line.get("tax_code") or "T")
        desc = str(line.get("description") or "")

        mapped_account_name = _map_gl_to_qbo_account(gl)
        # Try to resolve via the regular account mapping first, then fall back
        mapped_account_name = apply_account_mapping(mapped_account_name, mappings)
        expense_account_ref = find_account_by_name(mapped_account_name, qbo_config=qbo_config, db_path=DB_PATH)

        detail: dict[str, Any] = {
            "AccountRef": {
                "value": expense_account_ref["qbo_id"],
                "name": expense_account_ref["display_name"],
            },
        }

        # Tax code mapping — use the per-line tax code
        mapped_tax = _map_tax_code_to_qbo(tax)
        detail["TaxCodeRef"] = {"value": mapped_tax}
        detail["BillableStatus"] = "NotBillable"

        qbo_line: dict[str, Any] = {
            "Amount": line_amount,
            "DetailType": "AccountBasedExpenseLineDetail",
            "AccountBasedExpenseLineDetail": detail,
            "Description": desc,
        }
        qbo_lines.append(qbo_line)
    return qbo_lines


def build_qbo_expense_payload(
    posting_payload: dict[str, Any],
    *,
    qbo_config: QBOConfig,
    mappings: dict[str, Any],
) -> dict[str, Any]:
    amount = posting_payload.get("amount")
    if amount is None:
        raise ValueError("Posting payload missing amount")

    document_date = normalize_text(posting_payload.get("document_date"))
    if not document_date:
        raise ValueError("Posting payload missing document_date")

    vendor = normalize_text(posting_payload.get("vendor"))
    if not vendor:
        raise ValueError("Posting payload missing vendor")

    gl_account = normalize_text(posting_payload.get("gl_account"))

    currency = normalize_text(posting_payload.get("currency")) or "CAD"
    memo = normalize_text(posting_payload.get("memo")) or vendor
    file_name = normalize_text(posting_payload.get("file_name"))
    client_code = normalize_text(posting_payload.get("client_code"))
    category = normalize_text(posting_payload.get("category"))
    tax_code = normalize_text(posting_payload.get("tax_code"))

    mapped_vendor_name = apply_vendor_mapping(vendor, mappings)

    vendor_ref = find_vendor_by_name(mapped_vendor_name, qbo_config=qbo_config, db_path=DB_PATH)
    payment_settings = resolve_payment_settings(mappings=mappings, qbo_config=qbo_config)
    payment_account_ref = payment_settings["payment_account"]
    payment_type = payment_settings["payment_type"]

    private_note_parts: list[str] = []
    if file_name:
        private_note_parts.append(f"source_file={file_name}")
    if client_code:
        private_note_parts.append(f"client_code={client_code}")
    if category:
        private_note_parts.append(f"category={category}")
    if memo:
        private_note_parts.append(f"memo={memo}")
    private_note_parts.append(f"otocpa_vendor={vendor}")
    if gl_account:
        private_note_parts.append(f"otocpa_gl={gl_account}")

    # Try to use per-line items from invoice_lines table
    document_id = normalize_text(posting_payload.get("document_id"))
    invoice_lines = _fetch_invoice_lines(document_id) if document_id else []

    if invoice_lines:
        # Multi-line posting: each invoice line becomes a QBO line
        qbo_lines = _build_line_items_for_qbo(
            invoice_lines,
            qbo_config=qbo_config,
            mappings=mappings,
        )
        private_note_parts.append(f"line_items={len(invoice_lines)}")
    else:
        # Single-line fallback (original behaviour)
        if not gl_account:
            raise ValueError("Posting payload missing gl_account")
        mapped_account_name = apply_account_mapping(gl_account, mappings)
        expense_account_ref = find_account_by_name(mapped_account_name, qbo_config=qbo_config, db_path=DB_PATH)

        line_detail: dict[str, Any] = {
            "AccountBasedExpenseLineDetail": {
                "AccountRef": {
                    "value": expense_account_ref["qbo_id"],
                    "name": expense_account_ref["display_name"],
                }
            },
            "Amount": round(float(amount), 2),
            "DetailType": "AccountBasedExpenseLineDetail",
            "Description": memo,
        }

        tax_fragment = map_tax_code_for_qbo(tax_code, mappings)
        if tax_fragment:
            line_detail["AccountBasedExpenseLineDetail"].update(tax_fragment)

        qbo_lines = [line_detail]

    payload: dict[str, Any] = {
        "PaymentType": payment_type,
        "AccountRef": {
            "value": payment_account_ref["qbo_id"],
            "name": payment_account_ref["display_name"],
        },
        "EntityRef": {
            "type": "Vendor",
            "value": vendor_ref["qbo_id"],
            "name": vendor_ref["display_name"],
        },
        "TxnDate": document_date,
        "PrivateNote": " | ".join(private_note_parts),
        "Line": qbo_lines,
        "CurrencyRef": {
            "value": currency
        },
    }

    return payload


def build_qbo_bill_payload(
    posting_payload: dict[str, Any],
    *,
    qbo_config: QBOConfig,
    mappings: dict[str, Any],
) -> dict[str, Any]:
    amount = posting_payload.get("amount")
    if amount is None:
        raise ValueError("Posting payload missing amount")

    document_date = normalize_text(posting_payload.get("document_date"))
    if not document_date:
        raise ValueError("Posting payload missing document_date")

    vendor = normalize_text(posting_payload.get("vendor"))
    if not vendor:
        raise ValueError("Posting payload missing vendor")

    gl_account = normalize_text(posting_payload.get("gl_account"))

    currency = normalize_text(posting_payload.get("currency")) or "CAD"
    memo = normalize_text(posting_payload.get("memo")) or vendor
    tax_code = normalize_text(posting_payload.get("tax_code"))

    mapped_vendor_name = apply_vendor_mapping(vendor, mappings)
    vendor_ref = find_vendor_by_name(mapped_vendor_name, qbo_config=qbo_config, db_path=DB_PATH)

    # Try to use per-line items from invoice_lines table
    document_id = normalize_text(posting_payload.get("document_id"))
    invoice_lines = _fetch_invoice_lines(document_id) if document_id else []

    if invoice_lines:
        qbo_lines = _build_line_items_for_qbo(
            invoice_lines,
            qbo_config=qbo_config,
            mappings=mappings,
        )
    else:
        if not gl_account:
            raise ValueError("Posting payload missing gl_account")
        mapped_account_name = apply_account_mapping(gl_account, mappings)
        expense_account_ref = find_account_by_name(mapped_account_name, qbo_config=qbo_config, db_path=DB_PATH)

        line_detail: dict[str, Any] = {
            "AccountBasedExpenseLineDetail": {
                "AccountRef": {
                    "value": expense_account_ref["qbo_id"],
                    "name": expense_account_ref["display_name"],
                }
            },
            "Amount": round(float(amount), 2),
            "DetailType": "AccountBasedExpenseLineDetail",
            "Description": memo,
        }

        tax_fragment = map_tax_code_for_qbo(tax_code, mappings)
        if tax_fragment:
            line_detail["AccountBasedExpenseLineDetail"].update(tax_fragment)

        qbo_lines = [line_detail]

    payload: dict[str, Any] = {
        "TxnDate": document_date,
        "VendorRef": {
            "value": vendor_ref["qbo_id"],
            "name": vendor_ref["display_name"],
        },
        "Line": qbo_lines,
        "CurrencyRef": {
            "value": currency
        },
    }

    return payload


def build_qbo_api_payload(
    posting_payload: dict[str, Any],
    *,
    qbo_config: QBOConfig,
    mappings: dict[str, Any],
) -> dict[str, Any]:
    entry_kind = normalize_text(posting_payload.get("entry_kind")) or "expense"
    entry_kind = entry_kind.lower()

    if entry_kind == "expense":
        return build_qbo_expense_payload(
            posting_payload,
            qbo_config=qbo_config,
            mappings=mappings,
        )

    if entry_kind == "bill":
        return build_qbo_bill_payload(
            posting_payload,
            qbo_config=qbo_config,
            mappings=mappings,
        )

    raise ValueError(f"Unsupported QBO entry_kind: {entry_kind}")


def post_json(
    *,
    url: str,
    access_token: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return safe_json_loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"QBO HTTP {exc.code}: {raw}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"QBO network error: {exc}") from exc


def extract_external_id(response_json: dict[str, Any], entry_kind: str) -> Optional[str]:
    entry_kind = entry_kind.lower()

    if entry_kind == "expense":
        purchase = response_json.get("Purchase")
        if isinstance(purchase, dict):
            value = normalize_text(purchase.get("Id"))
            if value:
                return value

    if entry_kind == "bill":
        bill = response_json.get("Bill")
        if isinstance(bill, dict):
            value = normalize_text(bill.get("Id"))
            if value:
                return value

    return None


def _lookup_firm_code(client_code: str, db_path: Path = DB_PATH) -> Optional[str]:
    """Resolve the owning firm_code for a client_code (None when unknown)."""
    try:
        with open_db(db_path) as conn:
            row = conn.execute(
                "SELECT firm_code FROM clients WHERE client_code=?",
                (client_code,),
            ).fetchone()
        return row["firm_code"] if row and row["firm_code"] else None
    except Exception:
        return None


def _build_qbo_config_from_tokens(
    tokens: dict[str, Any],
    base_config: Optional[QBOConfig] = None,
) -> QBOConfig:
    """Create a QBOConfig that uses the stored per-client tokens/realm.

    ``base_config`` supplies the environment-level defaults (base_url,
    minor_version, auto_create_vendors flag) since those are global to the
    QBO app, not per-client.
    """
    base = base_config or load_qbo_config()
    return QBOConfig(
        access_token=tokens["access_token"],
        realm_id=tokens["realm_id"],
        base_url=base.base_url,
        minor_version=base.minor_version,
        auto_create_vendors=base.auto_create_vendors,
    )


def _resolve_client_qbo_config(
    posting_payload: dict[str, Any],
    db_path: Path = DB_PATH,
    base_config: Optional[QBOConfig] = None,
) -> tuple[Optional[QBOConfig], Optional[str], Optional[str], Optional[str]]:
    """Look up QBO tokens for this posting's client.

    Returns ``(qbo_config, firm_code, client_code, error_text)``.
    ``qbo_config`` is None (with error_text populated) when the client has no
    active QBO connection — the caller should skip the job in that case.
    """
    client_code = normalize_text(posting_payload.get("client_code"))
    if not client_code:
        return None, None, None, "Posting payload missing client_code"

    firm_code = _lookup_firm_code(client_code, db_path)
    if not firm_code:
        return None, None, client_code, f"No firm_code for client_code={client_code}"

    tokens = _oauth_get_qbo_tokens(firm_code, client_code)
    if tokens is None:
        return None, firm_code, client_code, (
            f"No QBO connection for firm={firm_code} client={client_code}"
        )
    if tokens.get("status") != "active":
        return None, firm_code, client_code, (
            f"QBO connection status={tokens.get('status')} "
            f"error={tokens.get('last_error') or ''}".strip()
        )

    return _build_qbo_config_from_tokens(tokens, base_config), firm_code, client_code, None


def _is_401(exc: Exception) -> bool:
    """True when a RuntimeError message looks like a QBO HTTP 401 response."""
    return "QBO HTTP 401" in str(exc)


def post_one_ready_job(
    posting_id: str,
    *,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    ensure_posting_jobs_table(db_path)
    # ``qbo_config`` is now only used for the environment-level defaults
    # (base_url/minor_version). Per-client access_token + realm_id are looked
    # up from the ``qbo_connections`` table.
    base_config = qbo_config or load_qbo_config()
    mappings = load_qbo_mappings()

    row = get_posting_job(posting_id, db_path=db_path)
    if row is None:
        raise ValueError(f"Posting job not found: {posting_id}")

    if normalize_text(row["target_system"]) != "qbo":
        raise ValueError(f"Posting job is not for qbo: {posting_id}")

    if normalize_text(row["approval_state"]) != "approved_for_posting":
        raise ValueError(f"Posting job is not approved_for_posting: {posting_id}")

    if normalize_text(row["posting_status"]) != "ready_to_post":
        raise ValueError(f"Posting job is not ready_to_post: {posting_id}")

    payload = safe_json_loads(row["payload_json"])
    blocking_issues = payload.get("blocking_issues", []) or []
    if blocking_issues:
        raise ValueError(f"Posting job has blocking issues: {blocking_issues}")

    per_client_config, firm_code, client_code, connection_err = (
        _resolve_client_qbo_config(payload, db_path, base_config)
    )
    if per_client_config is None:
        update_posting_job_after_attempt(
            posting_id=posting_id,
            posting_status="post_failed",
            external_id=None,
            error_text=connection_err or "No QBO connection for this client",
            payload=payload,
            db_path=db_path,
        )
        return {
            "posting_id": posting_id,
            "status": "skipped_no_connection",
            "error": connection_err,
        }

    entry_kind = normalize_text(payload.get("entry_kind")) or "expense"
    qbo_payload = build_qbo_api_payload(
        payload,
        qbo_config=per_client_config,
        mappings=mappings,
    )

    def _url_for(kind: str, cfg: QBOConfig) -> str:
        encoded_realm = urllib.parse.quote(cfg.realm_id)
        encoded_minor = urllib.parse.quote(cfg.minor_version)
        if kind == "expense":
            return f"{cfg.base_url}/v3/company/{encoded_realm}/purchase?minorversion={encoded_minor}"
        if kind == "bill":
            return f"{cfg.base_url}/v3/company/{encoded_realm}/bill?minorversion={encoded_minor}"
        raise ValueError(f"Unsupported entry kind: {kind}")

    kind_lc = entry_kind.lower()
    try:
        url = _url_for(kind_lc, per_client_config)

        try:
            response_json = post_json(
                url=url,
                access_token=per_client_config.access_token,
                payload=qbo_payload,
            )
        except RuntimeError as exc:
            # 401 → try a refresh, retry once. On refresh failure surface
            # the original 401 so the UI flags the connection as expired.
            if not _is_401(exc):
                raise
            refreshed = _oauth_refresh_access_token(firm_code, client_code)
            if refreshed is None:
                raise RuntimeError(
                    f"QBO 401 and refresh failed for "
                    f"firm={firm_code} client={client_code}: {exc}"
                ) from exc
            per_client_config = _build_qbo_config_from_tokens(refreshed, base_config)
            url = _url_for(kind_lc, per_client_config)
            response_json = post_json(
                url=url,
                access_token=per_client_config.access_token,
                payload=qbo_payload,
            )

        external_id = extract_external_id(response_json, entry_kind)
        if not external_id:
            raise RuntimeError(
                f"QBO response did not return an external id: {json.dumps(response_json, ensure_ascii=False)}"
            )

        update_posting_job_after_attempt(
            posting_id=posting_id,
            posting_status="posted",
            external_id=external_id,
            error_text=None,
            payload=payload,
            db_path=db_path,
        )

        return {
            "posting_id": posting_id,
            "status": "posted",
            "external_id": external_id,
            "firm_code": firm_code,
            "client_code": client_code,
            "qbo_request": qbo_payload,
            "qbo_response": response_json,
        }

    except Exception as exc:
        update_posting_job_after_attempt(
            posting_id=posting_id,
            posting_status="post_failed",
            external_id=None,
            error_text=str(exc),
            payload=payload,
            db_path=db_path,
        )
        return {
            "posting_id": posting_id,
            "status": "post_failed",
            "error": str(exc),
            "firm_code": firm_code,
            "client_code": client_code,
            "qbo_request": qbo_payload,
        }


def post_all_ready_jobs(
    *,
    db_path: Path = DB_PATH,
    qbo_config: Optional[QBOConfig] = None,
) -> dict[str, Any]:
    qbo_config = qbo_config or load_qbo_config()
    rows = list_ready_qbo_jobs(db_path=db_path)

    results: list[dict[str, Any]] = []
    posted_count = 0
    failed_count = 0

    for row in rows:
        result = post_one_ready_job(
            posting_id=str(row["posting_id"]),
            qbo_config=qbo_config,
            db_path=db_path,
        )
        results.append(result)

        if result["status"] == "posted":
            posted_count += 1
        else:
            failed_count += 1

    return {
        "run_at": utc_now_iso(),
        "ready_job_count": len(rows),
        "posted_count": posted_count,
        "failed_count": failed_count,
        "results": results,
    }


def export_post_results(results: dict[str, Any], out_path: Optional[Path] = None) -> Path:
    if out_path is None:
        out_path = EXPORTS_DIR / "qbo_post_results.json"

    out_path.write_text(
        json.dumps(results, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return out_path


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA QuickBooks Online adapter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    post_one_parser = subparsers.add_parser("post-one", help="Post one approved QBO posting job")
    post_one_parser.add_argument("--posting-id", required=True)

    post_all_parser = subparsers.add_parser("post-all", help="Post all approved QBO posting jobs")

    export_parser = subparsers.add_parser("export-ready", help="Export currently ready QBO jobs without posting")
    export_parser.add_argument("--out", default="")

    args = parser.parse_args()

    ensure_posting_jobs_table(DB_PATH)

    if args.command == "post-one":
        result = post_one_ready_job(args.posting_id, db_path=DB_PATH)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "post-all":
        results = post_all_ready_jobs(db_path=DB_PATH)
        out_path = export_post_results(results)
        print(json.dumps(results, indent=2, ensure_ascii=False))
        print(f"Saved results to: {out_path}")
        return 0

    if args.command == "export-ready":
        rows = list_ready_qbo_jobs(db_path=DB_PATH)
        export_payload: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload_json"] = safe_json_loads(item.get("payload_json"))
            export_payload.append(item)

        out_path = Path(args.out) if args.out else EXPORTS_DIR / "qbo_ready_jobs.json"
        out_path.write_text(
            json.dumps(export_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Exported ready jobs to: {out_path}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
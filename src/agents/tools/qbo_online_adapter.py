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


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
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
    if not gl_account:
        raise ValueError("Posting payload missing gl_account")

    currency = normalize_text(posting_payload.get("currency")) or "CAD"
    memo = normalize_text(posting_payload.get("memo")) or vendor
    file_name = normalize_text(posting_payload.get("file_name"))
    client_code = normalize_text(posting_payload.get("client_code"))
    category = normalize_text(posting_payload.get("category"))
    tax_code = normalize_text(posting_payload.get("tax_code"))

    mapped_vendor_name = apply_vendor_mapping(vendor, mappings)
    mapped_account_name = apply_account_mapping(gl_account, mappings)

    vendor_ref = find_vendor_by_name(mapped_vendor_name, qbo_config=qbo_config, db_path=DB_PATH)
    expense_account_ref = find_account_by_name(mapped_account_name, qbo_config=qbo_config, db_path=DB_PATH)
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
    private_note_parts.append(f"ledgerlink_vendor={vendor}")
    private_note_parts.append(f"ledgerlink_gl={gl_account}")

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
        "Line": [line_detail],
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
    if not gl_account:
        raise ValueError("Posting payload missing gl_account")

    currency = normalize_text(posting_payload.get("currency")) or "CAD"
    memo = normalize_text(posting_payload.get("memo")) or vendor
    tax_code = normalize_text(posting_payload.get("tax_code"))

    mapped_vendor_name = apply_vendor_mapping(vendor, mappings)
    mapped_account_name = apply_account_mapping(gl_account, mappings)

    vendor_ref = find_vendor_by_name(mapped_vendor_name, qbo_config=qbo_config, db_path=DB_PATH)
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

    payload: dict[str, Any] = {
        "TxnDate": document_date,
        "VendorRef": {
            "value": vendor_ref["qbo_id"],
            "name": vendor_ref["display_name"],
        },
        "Line": [line_detail],
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


def post_one_ready_job(
    posting_id: str,
    *,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    ensure_posting_jobs_table(db_path)
    qbo_config = qbo_config or load_qbo_config()
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

    entry_kind = normalize_text(payload.get("entry_kind")) or "expense"
    qbo_payload = build_qbo_api_payload(
        payload,
        qbo_config=qbo_config,
        mappings=mappings,
    )

    try:
        if entry_kind.lower() == "expense":
            encoded_realm = urllib.parse.quote(qbo_config.realm_id)
            url = f"{qbo_config.base_url}/v3/company/{encoded_realm}/purchase?minorversion={urllib.parse.quote(qbo_config.minor_version)}"
        elif entry_kind.lower() == "bill":
            encoded_realm = urllib.parse.quote(qbo_config.realm_id)
            url = f"{qbo_config.base_url}/v3/company/{encoded_realm}/bill?minorversion={urllib.parse.quote(qbo_config.minor_version)}"
        else:
            raise ValueError(f"Unsupported entry kind: {entry_kind}")

        response_json = post_json(
            url=url,
            access_token=qbo_config.access_token,
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

    parser = argparse.ArgumentParser(description="LedgerLink QuickBooks Online adapter")
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
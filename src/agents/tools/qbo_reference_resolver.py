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

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

QBO_CONFIG_PATH = ROOT_DIR / "data" / "qbo_config.json"


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


def normalize_key(value: Any) -> str:
    text = normalize_text(value) or ""
    return " ".join(text.lower().split())


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def load_qbo_config_file(path: Path = QBO_CONFIG_PATH) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def get_config_value(file_config: dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = normalize_text(file_config.get(key))
        if value:
            return value
    return None


@dataclass
class QBOConfig:
    access_token: str
    realm_id: str
    base_url: str
    minor_version: str
    auto_create_vendors: bool = False

    @property
    def query_url(self) -> str:
        encoded_realm = urllib.parse.quote(self.realm_id)
        encoded_minor = urllib.parse.quote(self.minor_version)
        return f"{self.base_url}/v3/company/{encoded_realm}/query?minorversion={encoded_minor}"

    @property
    def vendor_create_url(self) -> str:
        encoded_realm = urllib.parse.quote(self.realm_id)
        encoded_minor = urllib.parse.quote(self.minor_version)
        return f"{self.base_url}/v3/company/{encoded_realm}/vendor?minorversion={encoded_minor}"


def check_qbo_auth_status(config_path: Path = QBO_CONFIG_PATH) -> dict[str, Any]:
    file_config = load_qbo_config_file(config_path)

    env_access_token = normalize_text(os.environ.get("QBO_ACCESS_TOKEN"))
    env_realm_id = normalize_text(os.environ.get("QBO_REALM_ID"))

    file_access_token = get_config_value(file_config, "access_token", "QBO_ACCESS_TOKEN")
    file_realm_id = get_config_value(file_config, "realm_id", "QBO_REALM_ID")

    access_token = env_access_token or file_access_token
    realm_id = env_realm_id or file_realm_id

    environment = (
        normalize_text(os.environ.get("QBO_ENVIRONMENT"))
        or get_config_value(file_config, "environment", "QBO_ENVIRONMENT")
        or "production"
    ).lower()

    minor_version = (
        normalize_text(os.environ.get("QBO_MINOR_VERSION"))
        or get_config_value(file_config, "minor_version", "QBO_MINOR_VERSION")
        or "75"
    )

    auto_create_vendors = (
        normalize_text(os.environ.get("QBO_AUTO_CREATE_VENDORS"))
        or get_config_value(file_config, "auto_create_vendors", "QBO_AUTO_CREATE_VENDORS")
        or "false"
    ).lower() in {"1", "true", "yes", "y"}

    source = "environment" if env_access_token and env_realm_id else "file" if file_access_token and file_realm_id else "missing"

    missing: list[str] = []
    if not access_token:
        missing.append("QBO_ACCESS_TOKEN")
    if not realm_id:
        missing.append("QBO_REALM_ID")

    return {
        "ok": len(missing) == 0,
        "source": source,
        "environment": environment,
        "minor_version": minor_version,
        "realm_id_present": bool(realm_id),
        "access_token_present": bool(access_token),
        "auto_create_vendors": auto_create_vendors,
        "config_file_path": str(config_path),
        "config_file_exists": config_path.exists(),
        "missing": missing,
    }


def load_qbo_config(config_path: Path = QBO_CONFIG_PATH) -> QBOConfig:
    file_config = load_qbo_config_file(config_path)

    access_token = normalize_text(os.environ.get("QBO_ACCESS_TOKEN")) or get_config_value(file_config, "access_token", "QBO_ACCESS_TOKEN")
    realm_id = normalize_text(os.environ.get("QBO_REALM_ID")) or get_config_value(file_config, "realm_id", "QBO_REALM_ID")

    if not access_token:
        raise ValueError(
            f"Missing QBO access token. Provide QBO_ACCESS_TOKEN env var or put access_token in {config_path}"
        )

    if not realm_id:
        raise ValueError(
            f"Missing QBO realm id. Provide QBO_REALM_ID env var or put realm_id in {config_path}"
        )

    environment = (
        normalize_text(os.environ.get("QBO_ENVIRONMENT"))
        or get_config_value(file_config, "environment", "QBO_ENVIRONMENT")
        or "production"
    ).lower()

    if environment == "sandbox":
        base_url = "https://sandbox-quickbooks.api.intuit.com"
    else:
        base_url = "https://quickbooks.api.intuit.com"

    minor_version = (
        normalize_text(os.environ.get("QBO_MINOR_VERSION"))
        or get_config_value(file_config, "minor_version", "QBO_MINOR_VERSION")
        or "75"
    )

    auto_create_vendors = (
        normalize_text(os.environ.get("QBO_AUTO_CREATE_VENDORS"))
        or get_config_value(file_config, "auto_create_vendors", "QBO_AUTO_CREATE_VENDORS")
        or "false"
    ).lower() in {"1", "true", "yes", "y"}

    return QBOConfig(
        access_token=access_token,
        realm_id=realm_id,
        base_url=base_url,
        minor_version=minor_version,
        auto_create_vendors=auto_create_vendors,
    )


def ensure_reference_cache_table(db_path: Path = DB_PATH) -> None:
    with open_db(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qbo_reference_cache (
                cache_id TEXT PRIMARY KEY,
                ref_type TEXT NOT NULL,
                lookup_key TEXT NOT NULL,
                display_name TEXT,
                qbo_id TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_qbo_reference_cache_type_key
            ON qbo_reference_cache(ref_type, lookup_key)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_qbo_reference_cache_qbo_id
            ON qbo_reference_cache(qbo_id)
            """
        )
        conn.commit()


def make_cache_id(ref_type: str, lookup_key: str) -> str:
    safe_type = normalize_key(ref_type).replace(" ", "_")
    safe_key = normalize_key(lookup_key).replace(" ", "_")
    return f"qbo_ref_{safe_type}_{safe_key}"[:200]


def qbo_get_query(*, url: str, access_token: str, query: str) -> dict[str, Any]:
    encoded_query = urllib.parse.urlencode({"query": query})
    final_url = f"{url}&{encoded_query}"

    req = urllib.request.Request(
        url=final_url,
        method="GET",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
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


def qbo_post_json(*, url: str, access_token: str, payload: dict[str, Any]) -> dict[str, Any]:
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


def escape_qbo_query_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def get_cached_reference(
    *,
    ref_type: str,
    lookup_key: str,
    db_path: Path = DB_PATH,
) -> Optional[dict[str, Any]]:
    ensure_reference_cache_table(db_path)

    key = normalize_key(lookup_key)
    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM qbo_reference_cache
            WHERE ref_type = ?
              AND lookup_key = ?
            """,
            (ref_type, key),
        ).fetchone()

    if not row:
        return None

    return {
        "ref_type": row["ref_type"],
        "lookup_key": row["lookup_key"],
        "display_name": row["display_name"],
        "qbo_id": row["qbo_id"],
        "raw_json": safe_json_loads(row["raw_json"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "source": "cache",
    }


def cache_reference(
    *,
    ref_type: str,
    lookup_key: str,
    display_name: Optional[str],
    qbo_id: str,
    raw_json: dict[str, Any],
    db_path: Path = DB_PATH,
) -> None:
    ensure_reference_cache_table(db_path)

    key = normalize_key(lookup_key)
    now = utc_now_iso()
    cache_id = make_cache_id(ref_type, key)

    with open_db(db_path) as conn:
        existing = conn.execute(
            """
            SELECT cache_id
            FROM qbo_reference_cache
            WHERE ref_type = ?
              AND lookup_key = ?
            """,
            (ref_type, key),
        ).fetchone()

        if existing is None:
            conn.execute(
                """
                INSERT INTO qbo_reference_cache (
                    cache_id,
                    ref_type,
                    lookup_key,
                    display_name,
                    qbo_id,
                    raw_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cache_id,
                    ref_type,
                    key,
                    display_name,
                    qbo_id,
                    json.dumps(raw_json, ensure_ascii=False),
                    now,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE qbo_reference_cache
                SET
                    display_name = ?,
                    qbo_id = ?,
                    raw_json = ?,
                    updated_at = ?
                WHERE ref_type = ?
                  AND lookup_key = ?
                """,
                (
                    display_name,
                    qbo_id,
                    json.dumps(raw_json, ensure_ascii=False),
                    now,
                    ref_type,
                    key,
                ),
            )

        conn.commit()


def extract_query_items(response_json: dict[str, Any], collection_name: str) -> list[dict[str, Any]]:
    query_response = response_json.get("QueryResponse")
    if not isinstance(query_response, dict):
        return []

    items = query_response.get(collection_name)
    if not isinstance(items, list):
        return []

    cleaned: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            cleaned.append(item)
    return cleaned


def query_qbo_objects(
    *,
    entity_name: str,
    where_clause: str,
    qbo_config: QBOConfig,
) -> list[dict[str, Any]]:
    query = f"select * from {entity_name} where {where_clause}"
    response_json = qbo_get_query(
        url=qbo_config.query_url,
        access_token=qbo_config.access_token,
        query=query,
    )
    return extract_query_items(response_json, entity_name)


def create_vendor_in_qbo(
    vendor_name: str,
    *,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    vendor_name = normalize_text(vendor_name) or ""
    if not vendor_name:
        raise ValueError("vendor_name is required")

    qbo_config = qbo_config or load_qbo_config()

    payload = {
        "DisplayName": vendor_name,
        "CompanyName": vendor_name,
    }

    response_json = qbo_post_json(
        url=qbo_config.vendor_create_url,
        access_token=qbo_config.access_token,
        payload=payload,
    )

    vendor_obj = response_json.get("Vendor")
    if not isinstance(vendor_obj, dict):
        raise RuntimeError(f"QBO vendor create returned unexpected response: {json.dumps(response_json, ensure_ascii=False)}")

    qbo_id = normalize_text(vendor_obj.get("Id"))
    if not qbo_id:
        raise RuntimeError(f"QBO vendor create missing Id: {json.dumps(response_json, ensure_ascii=False)}")

    display_name = normalize_text(vendor_obj.get("DisplayName")) or vendor_name

    cache_reference(
        ref_type="vendor",
        lookup_key=vendor_name,
        display_name=display_name,
        qbo_id=qbo_id,
        raw_json=vendor_obj,
        db_path=db_path,
    )

    return {
        "ref_type": "vendor",
        "lookup_key": normalize_key(vendor_name),
        "display_name": display_name,
        "qbo_id": qbo_id,
        "raw_json": vendor_obj,
        "source": "api_created",
    }


def find_vendor_by_name(
    vendor_name: str,
    *,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    vendor_name = normalize_text(vendor_name) or ""
    if not vendor_name:
        raise ValueError("vendor_name is required")

    cached = get_cached_reference(ref_type="vendor", lookup_key=vendor_name, db_path=db_path)
    if cached:
        return cached

    qbo_config = qbo_config or load_qbo_config()
    escaped_name = escape_qbo_query_literal(vendor_name)

    exact_matches = query_qbo_objects(
        entity_name="Vendor",
        where_clause=f"DisplayName = '{escaped_name}'",
        qbo_config=qbo_config,
    )

    if not exact_matches:
        exact_matches = query_qbo_objects(
            entity_name="Vendor",
            where_clause=f"CompanyName = '{escaped_name}'",
            qbo_config=qbo_config,
        )

    if exact_matches:
        chosen = exact_matches[0]
        qbo_id = normalize_text(chosen.get("Id"))
        if not qbo_id:
            raise ValueError(f"QBO vendor found but missing Id: {vendor_name}")

        display_name = normalize_text(chosen.get("DisplayName")) or normalize_text(chosen.get("CompanyName")) or vendor_name

        cache_reference(
            ref_type="vendor",
            lookup_key=vendor_name,
            display_name=display_name,
            qbo_id=qbo_id,
            raw_json=chosen,
            db_path=db_path,
        )

        return {
            "ref_type": "vendor",
            "lookup_key": normalize_key(vendor_name),
            "display_name": display_name,
            "qbo_id": qbo_id,
            "raw_json": chosen,
            "source": "api",
        }

    if qbo_config.auto_create_vendors:
        return create_vendor_in_qbo(
            vendor_name,
            qbo_config=qbo_config,
            db_path=db_path,
        )

    raise ValueError(f"QBO vendor not found: {vendor_name}")


def find_account_by_name(
    account_name: str,
    *,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    account_name = normalize_text(account_name) or ""
    if not account_name:
        raise ValueError("account_name is required")

    cached = get_cached_reference(ref_type="account", lookup_key=account_name, db_path=db_path)
    if cached:
        return cached

    qbo_config = qbo_config or load_qbo_config()
    escaped_name = escape_qbo_query_literal(account_name)

    exact_matches = query_qbo_objects(
        entity_name="Account",
        where_clause=f"Name = '{escaped_name}'",
        qbo_config=qbo_config,
    )

    if not exact_matches:
        exact_matches = query_qbo_objects(
            entity_name="Account",
            where_clause=f"FullyQualifiedName = '{escaped_name}'",
            qbo_config=qbo_config,
        )

    if not exact_matches:
        raise ValueError(f"QBO account not found: {account_name}")

    chosen = exact_matches[0]
    qbo_id = normalize_text(chosen.get("Id"))
    if not qbo_id:
        raise ValueError(f"QBO account found but missing Id: {account_name}")

    display_name = (
        normalize_text(chosen.get("FullyQualifiedName"))
        or normalize_text(chosen.get("Name"))
        or account_name
    )

    cache_reference(
        ref_type="account",
        lookup_key=account_name,
        display_name=display_name,
        qbo_id=qbo_id,
        raw_json=chosen,
        db_path=db_path,
    )

    return {
        "ref_type": "account",
        "lookup_key": normalize_key(account_name),
        "display_name": display_name,
        "qbo_id": qbo_id,
        "raw_json": chosen,
        "source": "api",
    }


def find_payment_account(
    *,
    configured_name: Optional[str] = None,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    qbo_config = qbo_config or load_qbo_config()

    candidate_name = normalize_text(configured_name) or normalize_text(os.environ.get("QBO_PAYMENT_ACCOUNT_NAME"))
    if not candidate_name:
        file_config = load_qbo_config_file()
        candidate_name = get_config_value(file_config, "payment_account_name", "QBO_PAYMENT_ACCOUNT_NAME")

    if candidate_name:
        return find_account_by_name(candidate_name, qbo_config=qbo_config, db_path=db_path)

    cached_credit = get_cached_reference(ref_type="payment_account_creditcard_default", lookup_key="default", db_path=db_path)
    if cached_credit:
        return cached_credit

    credit_matches = query_qbo_objects(
        entity_name="Account",
        where_clause="AccountType = 'Credit Card' AND Active = true",
        qbo_config=qbo_config,
    )
    if credit_matches:
        chosen = credit_matches[0]
        qbo_id = normalize_text(chosen.get("Id"))
        display_name = normalize_text(chosen.get("FullyQualifiedName")) or normalize_text(chosen.get("Name")) or "Credit Card"
        if not qbo_id:
            raise ValueError("Default credit card account missing Id in QBO")

        cache_reference(
            ref_type="payment_account_creditcard_default",
            lookup_key="default",
            display_name=display_name,
            qbo_id=qbo_id,
            raw_json=chosen,
            db_path=db_path,
        )
        return {
            "ref_type": "payment_account_creditcard_default",
            "lookup_key": "default",
            "display_name": display_name,
            "qbo_id": qbo_id,
            "raw_json": chosen,
            "source": "api",
        }

    cached_bank = get_cached_reference(ref_type="payment_account_bank_default", lookup_key="default", db_path=db_path)
    if cached_bank:
        return cached_bank

    bank_matches = query_qbo_objects(
        entity_name="Account",
        where_clause="AccountType = 'Bank' AND Active = true",
        qbo_config=qbo_config,
    )
    if bank_matches:
        chosen = bank_matches[0]
        qbo_id = normalize_text(chosen.get("Id"))
        display_name = normalize_text(chosen.get("FullyQualifiedName")) or normalize_text(chosen.get("Name")) or "Bank"
        if not qbo_id:
            raise ValueError("Default bank account missing Id in QBO")

        cache_reference(
            ref_type="payment_account_bank_default",
            lookup_key="default",
            display_name=display_name,
            qbo_id=qbo_id,
            raw_json=chosen,
            db_path=db_path,
        )
        return {
            "ref_type": "payment_account_bank_default",
            "lookup_key": "default",
            "display_name": display_name,
            "qbo_id": qbo_id,
            "raw_json": chosen,
            "source": "api",
        }

    raise ValueError(
        "No QBO payment account could be resolved. Set QBO_PAYMENT_ACCOUNT_NAME or create an active Credit Card/Bank account in QBO."
    )


def resolve_references_for_posting_payload(
    posting_payload: dict[str, Any],
    *,
    qbo_config: Optional[QBOConfig] = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    qbo_config = qbo_config or load_qbo_config()

    vendor_name = normalize_text(posting_payload.get("vendor"))
    gl_account_name = normalize_text(posting_payload.get("gl_account"))

    if not vendor_name:
        raise ValueError("Posting payload missing vendor")
    if not gl_account_name:
        raise ValueError("Posting payload missing gl_account")

    vendor_ref = find_vendor_by_name(vendor_name, qbo_config=qbo_config, db_path=db_path)
    expense_account_ref = find_account_by_name(gl_account_name, qbo_config=qbo_config, db_path=db_path)
    payment_account_ref = find_payment_account(qbo_config=qbo_config, db_path=db_path)

    payment_account_type = normalize_text(payment_account_ref["raw_json"].get("AccountType")) or ""
    payment_type = "CreditCard" if payment_account_type.lower() == "credit card" else "Cash"

    return {
        "vendor": vendor_ref,
        "expense_account": expense_account_ref,
        "payment_account": payment_account_ref,
        "payment_type": payment_type,
        "resolved_at": utc_now_iso(),
    }


def export_reference_cache_snapshot(
    *,
    db_path: Path = DB_PATH,
    out_path: Optional[Path] = None,
) -> Path:
    ensure_reference_cache_table(db_path)

    with open_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM qbo_reference_cache
            ORDER BY ref_type ASC, lookup_key ASC
            """
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["raw_json"] = safe_json_loads(item["raw_json"])
        items.append(item)

    payload = {
        "generated_at": utc_now_iso(),
        "count": len(items),
        "items": items,
    }

    if out_path is None:
        out_path = EXPORTS_DIR / "qbo_reference_cache.json"

    out_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return out_path


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA QBO reference resolver")
    subparsers = parser.add_subparsers(dest="command", required=True)

    vendor_parser = subparsers.add_parser("find-vendor", help="Resolve a QBO vendor by name")
    vendor_parser.add_argument("--name", required=True)

    create_vendor_parser = subparsers.add_parser("create-vendor", help="Create a QBO vendor by name")
    create_vendor_parser.add_argument("--name", required=True)

    account_parser = subparsers.add_parser("find-account", help="Resolve a QBO account by name")
    account_parser.add_argument("--name", required=True)

    payment_parser = subparsers.add_parser("find-payment-account", help="Resolve the default payment account")
    payment_parser.add_argument("--name", default="")

    export_parser = subparsers.add_parser("export-cache", help="Export cached references")
    export_parser.add_argument("--out", default="")

    auth_parser = subparsers.add_parser("check-auth", help="Check QBO auth/config availability")

    args = parser.parse_args()
    ensure_reference_cache_table(DB_PATH)

    if args.command == "find-vendor":
        result = find_vendor_by_name(args.name, db_path=DB_PATH)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "create-vendor":
        result = create_vendor_in_qbo(args.name, db_path=DB_PATH)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "find-account":
        result = find_account_by_name(args.name, db_path=DB_PATH)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "find-payment-account":
        result = find_payment_account(
            configured_name=args.name or None,
            db_path=DB_PATH,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "export-cache":
        out_path = Path(args.out) if args.out else None
        final_out = export_reference_cache_snapshot(db_path=DB_PATH, out_path=out_path)
        print(f"Exported QBO reference cache to: {final_out}")
        return 0

    if args.command == "check-auth":
        result = check_qbo_auth_status()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
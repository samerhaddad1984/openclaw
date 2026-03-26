from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


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


def get_required_env(name: str) -> str:
    value = normalize_text(os.environ.get(name))
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_qbo_base_url() -> str:
    environment = (normalize_text(os.environ.get("QBO_ENVIRONMENT")) or "production").lower()
    if environment == "sandbox":
        return "https://sandbox-quickbooks.api.intuit.com"
    return "https://quickbooks.api.intuit.com"


def get_query_url() -> str:
    realm_id = get_required_env("QBO_REALM_ID")
    base_url = get_qbo_base_url()
    minor_version = normalize_text(os.environ.get("QBO_MINOR_VERSION")) or "75"
    encoded_realm = urllib.parse.quote(realm_id)
    encoded_minor = urllib.parse.quote(minor_version)
    return f"{base_url}/v3/company/{encoded_realm}/query?minorversion={encoded_minor}"


def qbo_get_query(query: str) -> dict[str, Any]:
    access_token = get_required_env("QBO_ACCESS_TOKEN")
    base_url = get_query_url()
    encoded_query = urllib.parse.urlencode({"query": query})
    final_url = f"{base_url}&{encoded_query}"

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


def extract_items(response_json: dict[str, Any], entity_name: str) -> list[dict[str, Any]]:
    query_response = response_json.get("QueryResponse")
    if not isinstance(query_response, dict):
        return []

    items = query_response.get(entity_name)
    if not isinstance(items, list):
        return []

    cleaned: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            cleaned.append(item)
    return cleaned


def list_vendors(limit: int = 100) -> list[dict[str, Any]]:
    query = f"select * from Vendor maxresults {int(limit)}"
    response = qbo_get_query(query)
    return extract_items(response, "Vendor")


def list_accounts(limit: int = 200) -> list[dict[str, Any]]:
    query = f"select * from Account maxresults {int(limit)}"
    response = qbo_get_query(query)
    return extract_items(response, "Account")


def simplify_vendor(v: dict[str, Any]) -> dict[str, Any]:
    return {
        "Id": v.get("Id"),
        "DisplayName": v.get("DisplayName"),
        "CompanyName": v.get("CompanyName"),
        "Active": v.get("Active"),
        "PrimaryEmailAddr": (v.get("PrimaryEmailAddr") or {}).get("Address") if isinstance(v.get("PrimaryEmailAddr"), dict) else None,
    }


def simplify_account(a: dict[str, Any]) -> dict[str, Any]:
    return {
        "Id": a.get("Id"),
        "Name": a.get("Name"),
        "FullyQualifiedName": a.get("FullyQualifiedName"),
        "AccountType": a.get("AccountType"),
        "AccountSubType": a.get("AccountSubType"),
        "Active": a.get("Active"),
        "Currency": (a.get("CurrencyRef") or {}).get("name") if isinstance(a.get("CurrencyRef"), dict) else None,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="List QBO vendors/accounts so you stop guessing names.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    vendors_parser = subparsers.add_parser("vendors", help="List vendors")
    vendors_parser.add_argument("--limit", type=int, default=100)

    accounts_parser = subparsers.add_parser("accounts", help="List accounts")
    accounts_parser.add_argument("--limit", type=int, default=200)

    args = parser.parse_args()

    if args.command == "vendors":
        vendors = [simplify_vendor(v) for v in list_vendors(limit=args.limit)]
        print(json.dumps(vendors, indent=2, ensure_ascii=False))
        return 0

    if args.command == "accounts":
        accounts = [simplify_account(a) for a in list_accounts(limit=args.limit)]
        print(json.dumps(accounts, indent=2, ensure_ascii=False))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
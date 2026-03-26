from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.qbo_reference_resolver import QBOConfig, load_qbo_config


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


def qbo_get(*, url: str, access_token: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url=url,
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


def build_entity_url(qbo_config: QBOConfig, entity_name: str, txn_id: str) -> str:
    encoded_realm = urllib.parse.quote(qbo_config.realm_id)
    encoded_txn_id = urllib.parse.quote(txn_id)
    encoded_minor = urllib.parse.quote(qbo_config.minor_version)
    return f"{qbo_config.base_url}/v3/company/{encoded_realm}/{entity_name}/{encoded_txn_id}?minorversion={encoded_minor}"


def fetch_entity_by_id(
    *,
    qbo_config: QBOConfig,
    entity_name: str,
    txn_id: str,
) -> dict[str, Any]:
    url = build_entity_url(qbo_config, entity_name, txn_id)
    response_json = qbo_get(url=url, access_token=qbo_config.access_token)
    return {
        "entity_name": entity_name,
        "txn_id": txn_id,
        "url": url,
        "response_json": response_json,
    }


def verify_transaction(
    *,
    txn_id: str,
    qbo_config: QBOConfig | None = None,
) -> dict[str, Any]:
    txn_id = str(txn_id).strip()
    if not txn_id:
        raise ValueError("txn_id is required")

    qbo_config = qbo_config or load_qbo_config()

    attempts: list[dict[str, Any]] = []
    entity_candidates = ["purchase", "bill", "journalentry"]

    for entity_name in entity_candidates:
        try:
            result = fetch_entity_by_id(
                qbo_config=qbo_config,
                entity_name=entity_name,
                txn_id=txn_id,
            )
            response_json = result["response_json"]

            if entity_name == "purchase" and isinstance(response_json.get("Purchase"), dict):
                return {
                    "ok": True,
                    "txn_id": txn_id,
                    "entity_name": "purchase",
                    "realm_id": qbo_config.realm_id,
                    "base_url": qbo_config.base_url,
                    "transaction": response_json.get("Purchase"),
                    "raw_response": response_json,
                    "attempts": attempts,
                }

            if entity_name == "bill" and isinstance(response_json.get("Bill"), dict):
                return {
                    "ok": True,
                    "txn_id": txn_id,
                    "entity_name": "bill",
                    "realm_id": qbo_config.realm_id,
                    "base_url": qbo_config.base_url,
                    "transaction": response_json.get("Bill"),
                    "raw_response": response_json,
                    "attempts": attempts,
                }

            if entity_name == "journalentry" and isinstance(response_json.get("JournalEntry"), dict):
                return {
                    "ok": True,
                    "txn_id": txn_id,
                    "entity_name": "journalentry",
                    "realm_id": qbo_config.realm_id,
                    "base_url": qbo_config.base_url,
                    "transaction": response_json.get("JournalEntry"),
                    "raw_response": response_json,
                    "attempts": attempts,
                }

            attempts.append(
                {
                    "entity_name": entity_name,
                    "status": "not_found_in_response",
                    "response_keys": list(response_json.keys()) if isinstance(response_json, dict) else [],
                }
            )

        except Exception as exc:
            attempts.append(
                {
                    "entity_name": entity_name,
                    "status": "error",
                    "error": str(exc),
                }
            )

    return {
        "ok": False,
        "txn_id": txn_id,
        "realm_id": qbo_config.realm_id,
        "base_url": qbo_config.base_url,
        "message": "Transaction not verified in purchase, bill, or journalentry endpoints.",
        "attempts": attempts,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Verify a QBO transaction by ID")
    parser.add_argument("--txn-id", required=True)

    args = parser.parse_args()

    result = verify_transaction(txn_id=args.txn_id)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
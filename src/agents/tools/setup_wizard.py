from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import msal
import requests

# -----------------------------
# Paths
# -----------------------------
TOOLS_DIR = Path(__file__).resolve().parent
AGENTS_DIR = TOOLS_DIR.parent
DATA_DIR = AGENTS_DIR / "data" / "tenants"

# -----------------------------
# Azure App (YOU create once)
# Delegated permissions needed in Entra ID:
#   - Mail.ReadWrite
#   - Mail.Send
#   - Sites.ReadWrite.All  (or Sites.Read.All if you only read)
# -----------------------------
CLIENT_ID = "PASTE_YOUR_AZURE_APP_CLIENT_ID_HERE"

# Use "common" for multi-tenant delegated auth
AUTHORITY = "https://login.microsoftonline.com/common"

# Minimal scopes for delegated mode
SCOPES = [
    "User.Read",
    "Mail.ReadWrite",
    "Mail.Send",
    "Sites.ReadWrite.All",
]

GRAPH = "https://graph.microsoft.com/v1.0"


@dataclass
class TenantConfig:
    tenant_label: str  # friendly name (e.g. "CPA_Firm_ABC")
    tenant_domain: str  # e.g. firmname.onmicrosoft.com or firm domain
    mailbox: str  # ledgerlink@firm.com
    sharepoint_site_url: str  # https://firm.sharepoint.com/sites/AccountingAI
    folders_inbox: str = "/AI/Inbox"
    folders_processing: str = "/AI/Processing"
    folders_review: str = "/AI/For_Review"
    folders_archive: str = "/AI/Archive"
    language: str = "AUTO"  # EN / FR / AUTO
    province_default: str = "QC"  # QC/ON/BC etc


def _prompt(msg: str, default: Optional[str] = None) -> str:
    if default:
        v = input(f"{msg} [{default}]: ").strip()
        return v if v else default
    return input(f"{msg}: ").strip()


def _ensure_dirs(base: Path) -> None:
    (base / "downloads").mkdir(parents=True, exist_ok=True)
    (base / "debug").mkdir(parents=True, exist_ok=True)
    (base / "rules").mkdir(parents=True, exist_ok=True)


def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _load_cache(cache_path: Path) -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    if cache_path.exists():
        cache.deserialize(cache_path.read_text(encoding="utf-8"))
    return cache


def _persist_cache(cache: msal.SerializableTokenCache, cache_path: Path) -> None:
    if cache.has_state_changed:
        cache_path.write_text(cache.serialize(), encoding="utf-8")


def _graph_get(access_token: str, url: str):
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    return r.status_code, r.json() if r.text else {}


def _graph_post(access_token: str, url: str, body: dict):
    r = requests.post(
        url, headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=body, timeout=30
    )
    return r.status_code, r.json() if r.text else {}


def _resolve_tenant_id(access_token: str) -> str:
    # /organization returns tenant info for the signed-in tenant
    code, data = _graph_get(access_token, f"{GRAPH}/organization")
    if code != 200 or "value" not in data or not data["value"]:
        raise RuntimeError(f"Failed to resolve tenant id via /organization. HTTP {code}: {data}")
    return data["value"][0]["id"]


def _test_email(access_token: str, mailbox: str) -> None:
    # Delegated: /me works; but we want to ensure mailbox is the right one.
    # If mailbox != signed-in user mailbox, this may fail unless user has access.
    url = f"{GRAPH}/users/{mailbox}/mailFolders/Inbox/messages?$top=1"
    code, data = _graph_get(access_token, url)
    if code not in (200, 201):
        raise RuntimeError(f"Email test failed. HTTP {code}: {data}")


def _test_sharepoint(access_token: str, site_url: str) -> None:
    # Convert site URL to Graph site identifier:
    # GET /sites/{hostname}:/sites/{site-path}
    # Example: https://tenant.sharepoint.com/sites/AccountingAI
    site_url = site_url.rstrip("/")
    if "sharepoint.com" not in site_url:
        raise RuntimeError("SharePoint Site URL must be a sharepoint.com site URL.")

    # crude parse
    # https://{host}/sites/{name...}
    parts = site_url.split("/")
    host = parts[2]
    path = "/" + "/".join(parts[3:])  # /sites/AccountingAI

    code, site = _graph_get(access_token, f"{GRAPH}/sites/{host}:{path}")
    if code != 200 or "id" not in site:
        raise RuntimeError(f"SharePoint site lookup failed. HTTP {code}: {site}")

    # list drives as basic proof of access
    code2, drives = _graph_get(access_token, f"{GRAPH}/sites/{site['id']}/drives?$top=5")
    if code2 != 200:
        raise RuntimeError(f"SharePoint drives test failed. HTTP {code2}: {drives}")


def run_setup_wizard():
    print("\nLedgerLink Setup Wizard (Delegated Graph)\n")

    cfg = TenantConfig(
        tenant_label=_prompt("Tenant label (friendly name)", "CPA_FIRM_ABC"),
        tenant_domain=_prompt("Tenant domain (firmname.onmicrosoft.com OR firm domain)", "firmname.onmicrosoft.com"),
        mailbox=_prompt("LedgerLink mailbox (must exist and you must have access)", "ledgerlink@firm.com"),
        sharepoint_site_url=_prompt("SharePoint Site URL", "https://firm.sharepoint.com/sites/AccountingAI"),
        folders_inbox=_prompt("SharePoint inbox folder", "/AI/Inbox"),
        folders_processing=_prompt("SharePoint processing folder", "/AI/Processing"),
        folders_review=_prompt("SharePoint review folder", "/AI/For_Review"),
        folders_archive=_prompt("SharePoint archive folder", "/AI/Archive"),
        language=_prompt("Language mode (EN/FR/AUTO)", "AUTO").upper(),
        province_default=_prompt("Default province (QC/ON/BC/...)", "QC").upper(),
    )

    if CLIENT_ID.startswith("PASTE_"):
        raise RuntimeError("You forgot to set CLIENT_ID in setup_wizard.py")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # We don't know tenant_id until after auth; cache is stored temporarily by label
    temp_dir = DATA_DIR / "_pending" / cfg.tenant_label
    temp_dir.mkdir(parents=True, exist_ok=True)

    token_cache_path = temp_dir / "tokens.json"
    cache = _load_cache(token_cache_path)

    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache,
    )

    # Acquire token (device code)
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Failed to create device flow. {flow}")

    print("\nAUTH REQUIRED")
    print(flow["message"])  # shows URL + code

    result = app.acquire_token_by_device_flow(flow)
    _persist_cache(cache, token_cache_path)

    if "access_token" not in result:
        raise RuntimeError(f"Authentication failed: {result}")

    access_token = result["access_token"]
    tenant_id = _resolve_tenant_id(access_token)

    # Move pending folder to tenant_id folder
    tenant_dir = DATA_DIR / tenant_id
    tenant_dir.mkdir(parents=True, exist_ok=True)
    _ensure_dirs(tenant_dir)

    # Move token cache to tenant_dir
    final_token_cache_path = tenant_dir / "tokens.json"
    final_token_cache_path.write_text(token_cache_path.read_text(encoding="utf-8"), encoding="utf-8")

    # Save config
    config_path = tenant_dir / "config.json"
    _save_json(config_path, asdict(cfg) | {"tenant_id": tenant_id})

    # Connectivity tests
    print("\nTesting email access...")
    _test_email(access_token, cfg.mailbox)
    print("Email OK")

    print("\nTesting SharePoint access...")
    _test_sharepoint(access_token, cfg.sharepoint_site_url)
    print("SharePoint OK")

    print("\nDONE.")
    print(f"Tenant ID: {tenant_id}")
    print(f"Config: {config_path}")
    print(f"Tokens: {final_token_cache_path}")
    print(f"Tenant data folder: {tenant_dir}\n")


if __name__ == "__main__":
    run_setup_wizard()
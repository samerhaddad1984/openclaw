"""
src/engines/license_engine.py — LedgerLink license key validation engine.

License key format: LLAI-<base64url_payload>
where payload is JSON: {"tier": "...", "firm_name": "...", "max_clients": N,
                        "max_users": N, "expiry_date": "YYYY-MM-DD",
                        "issued_at": "YYYY-MM-DD", "sig": "<hmac_hex>"}

HMAC-SHA256 is computed over the JSON of all fields EXCEPT "sig",
then encoded as hex and stored in the "sig" field.
The key is: LLAI- + base64.urlsafe_b64encode(json_bytes).decode().rstrip("=")
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# Tier definitions
# ---------------------------------------------------------------------------
TIER_DEFAULTS: dict[str, dict[str, Any]] = {
    "essentiel": {
        "max_clients": 10,
        "max_users": 3,
        "features": [
            "basic_review",
            "basic_posting",
        ],
    },
    "professionnel": {
        "max_clients": 30,
        "max_users": 5,
        "features": [
            "basic_review",
            "basic_posting",
            "ai_router",
            "bank_parser",
            "fraud_detection",
            "revenu_quebec",
            "time_tracking",
            "month_end",
        ],
    },
    "cabinet": {
        "max_clients": 75,
        "max_users": 15,
        "features": [
            "basic_review",
            "basic_posting",
            "ai_router",
            "bank_parser",
            "fraud_detection",
            "revenu_quebec",
            "time_tracking",
            "month_end",
            "analytics",
            "microsoft365",
            "filing_calendar",
            "client_comms",
        ],
    },
    "entreprise": {
        "max_clients": 999999,
        "max_users": 999999,
        "features": [
            "basic_review",
            "basic_posting",
            "ai_router",
            "bank_parser",
            "fraud_detection",
            "revenu_quebec",
            "time_tracking",
            "month_end",
            "analytics",
            "microsoft365",
            "filing_calendar",
            "client_comms",
            "audit_module",
            "financial_statements",
            "sampling",
            "api_access",
        ],
    },
}

_EMPTY_STATUS: dict[str, Any] = {
    "valid": False,
    "tier": "none",
    "firm_name": "",
    "max_clients": 0,
    "max_users": 0,
    "expiry_date": "",
    "days_remaining": 0,
    "features": [],
    "error": "",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sign(payload: dict[str, Any], secret: str) -> str:
    """Return HMAC-SHA256 hex of the payload JSON (without 'sig' key, sorted keys)."""
    data = {k: v for k, v in payload.items() if k != "sig"}
    msg = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def _add_padding(s: str) -> str:
    """Add base64 padding that was stripped."""
    return s + "=" * (-len(s) % 4)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_license(key: str, secret: str) -> dict:
    """Decode and verify the license key.

    Returns the payload dict.
    Raises ValueError if key is invalid, signature mismatch, or expiry passed.
    """
    if not key or not key.startswith("LLAI-"):
        raise ValueError("Invalid license key format")
    b64_part = key[5:]  # strip "LLAI-"
    try:
        padded = _add_padding(b64_part)
        json_bytes = base64.urlsafe_b64decode(padded)
        payload = json.loads(json_bytes.decode())
    except Exception as exc:
        raise ValueError(f"Could not decode license key: {exc}") from exc

    # Verify signature
    expected_sig = _sign(payload, secret)
    if not hmac.compare_digest(expected_sig, payload.get("sig", "")):
        raise ValueError("License signature mismatch")

    # Check expiry
    expiry_str = payload.get("expiry_date", "")
    try:
        expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid expiry_date in license: {expiry_str!r}")

    if expiry < date.today():
        raise ValueError(f"License expired on {expiry_str}")

    return payload


def get_signing_secret() -> str:
    """Read LEDGERLINK_SIGNING_SECRET from .env file or environment variable."""
    env_file = ROOT_DIR / ".env"
    if env_file.exists():
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("LEDGERLINK_SIGNING_SECRET"):
                    _, _, value = line.partition("=")
                    return value.strip().strip('"').strip("'")
        except Exception:
            pass
    return os.environ.get("LEDGERLINK_SIGNING_SECRET", "")


def get_license_status() -> dict:
    """Read license from ledgerlink.config.json and return status dict."""
    config_path = ROOT_DIR / "ledgerlink.config.json"
    defaults = dict(_EMPTY_STATUS)

    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        defaults["error"] = "Could not read ledgerlink.config.json"
        return defaults

    lic_cfg = cfg.get("license")
    if not lic_cfg:
        defaults["error"] = "No license installed"
        defaults["tier"] = "none"
        return defaults

    key = lic_cfg.get("key", "")
    secret = lic_cfg.get("secret", "") or get_signing_secret()

    try:
        payload = load_license(key, secret)
    except ValueError as exc:
        defaults["error"] = str(exc)
        # Try to extract tier/expiry even from bad license for display purposes
        try:
            b64_part = key[5:]
            raw = json.loads(base64.urlsafe_b64decode(_add_padding(b64_part)).decode())
            defaults["expiry_date"] = raw.get("expiry_date", "")
            defaults["tier"] = raw.get("tier", "none")
        except Exception:
            pass
        return defaults

    tier = payload.get("tier", "none")
    tier_info = TIER_DEFAULTS.get(tier, {})
    expiry_str = payload.get("expiry_date", "")
    try:
        expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        days_remaining = (expiry - date.today()).days
    except ValueError:
        days_remaining = 0

    return {
        "valid": True,
        "tier": tier,
        "firm_name": payload.get("firm_name", ""),
        "max_clients": payload.get("max_clients", tier_info.get("max_clients", 0)),
        "max_users": payload.get("max_users", tier_info.get("max_users", 0)),
        "expiry_date": expiry_str,
        "days_remaining": days_remaining,
        "features": tier_info.get("features", []),
        "error": "",
    }


def check_feature(feature_name: str) -> bool:
    """Return True if the current license tier includes the given feature."""
    status = get_license_status()
    return feature_name in status.get("features", [])


def check_limits(conn: sqlite3.Connection) -> dict:
    """Count actual clients and users from DB, compare to license limits."""
    status = get_license_status()
    max_clients = status.get("max_clients", 0)
    max_users = status.get("max_users", 0)

    try:
        row = conn.execute("SELECT COUNT(*) FROM clients").fetchone()
        client_count = row[0] if row else 0
    except Exception:
        client_count = 0

    try:
        row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        user_count = row[0] if row else 0
    except Exception:
        user_count = 0

    clients_ok = client_count <= max_clients
    users_ok = user_count <= max_users

    return {
        "client_count": client_count,
        "max_clients": max_clients,
        "clients_ok": clients_ok,
        "user_count": user_count,
        "max_users": max_users,
        "users_ok": users_ok,
        "within_limits": clients_ok and users_ok,
    }


def save_license_to_config(key: str, secret: str) -> dict:
    """Validate the key then save to ledgerlink.config.json. Return payload."""
    payload = load_license(key, secret)

    config_path = ROOT_DIR / "ledgerlink.config.json"
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}

    cfg["license"] = {"key": key, "secret": secret}
    config_path.write_text(
        json.dumps(cfg, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return payload


def generate_license_key(
    tier: str,
    firm_name: str,
    expiry_date: str,
    issued_at: str,
    secret: str,
    max_clients: int | None = None,
    max_users: int | None = None,
) -> str:
    """Generate a license key for the given parameters."""
    tier_info = TIER_DEFAULTS.get(tier, {})
    payload: dict[str, Any] = {
        "tier": tier,
        "firm_name": firm_name,
        "max_clients": max_clients if max_clients is not None else tier_info.get("max_clients", 0),
        "max_users": max_users if max_users is not None else tier_info.get("max_users", 0),
        "expiry_date": expiry_date,
        "issued_at": issued_at,
    }
    payload["sig"] = _sign(payload, secret)
    json_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    b64 = base64.urlsafe_b64encode(json_bytes).decode().rstrip("=")
    return f"LLAI-{b64}"

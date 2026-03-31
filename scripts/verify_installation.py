#!/usr/bin/env python3
"""
scripts/verify_installation.py — LedgerLink Remote Installation Verifier
=========================================================================
Run from your office after a client installs LedgerLink to verify everything
is working correctly.

Usage:
    python scripts/verify_installation.py --url https://tremblay-cpa.ledgerlink.app --license-tier professionnel
    python scripts/verify_installation.py --help
"""
from __future__ import annotations

import argparse
import json
import smtplib
import ssl
import sys
import urllib.request
from email.mime.text import MIMEText
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SUPPORT_EMAIL = "support@ledgerlink.ca"


def _load_config() -> dict:
    try:
        return json.loads((ROOT_DIR / "ledgerlink.config.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def _send_email(subject: str, body: str, to_addr: str) -> bool:
    """Send email via SMTP configured in ledgerlink.config.json."""
    cfg = _load_config().get("digest", {})
    smtp_host = cfg.get("smtp_host", "")
    smtp_port = cfg.get("smtp_port", 587)
    smtp_user = cfg.get("smtp_user", "")
    smtp_password = cfg.get("smtp_password", "")
    from_addr = cfg.get("from_address", smtp_user)

    if not smtp_host or not smtp_user or not smtp_password:
        return False

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr

        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_addr, [to_addr], msg.as_string())
        return True
    except Exception:
        return False


def _fetch_json(url: str, timeout: int = 15) -> dict | None:
    """Fetch JSON from a URL, handling both HTTP and HTTPS."""
    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode("utf-8"))
    except Exception:
        pass
    return None


def verify_installation(base_url: str, expected_tier: str | None = None) -> dict:
    """Run all 7 verification checks against a remote LedgerLink instance."""
    base_url = base_url.rstrip("/")
    results = []

    # Check 1: GET /health returns status ok
    health_url = f"{base_url}/health"
    health = _fetch_json(health_url)
    if health and health.get("status") == "ok":
        results.append(("Health endpoint responding", True, None))
    else:
        results.append(("Health endpoint responding", False, "GET /health did not return status ok"))

    # Check 2: License valid and correct tier
    if health:
        license_valid = health.get("license_valid", False)
        license_tier = health.get("license_tier", "")
        if license_valid:
            tier_msg = f"{license_tier.capitalize()} tier"
            if expected_tier and license_tier.lower() != expected_tier.lower():
                results.append((f"License valid — {tier_msg} (expected {expected_tier})", False,
                                "License tier mismatch"))
            else:
                results.append((f"License valid — {tier_msg}", True, None))
        else:
            results.append(("License valid", False, "License not valid"))
    else:
        results.append(("License valid", False, "Could not reach health endpoint"))

    # Check 3: Database initialized (documents table exists)
    if health:
        db_ok = health.get("db_ok", False)
        if db_ok:
            results.append(("Database initialized", True, None))
        else:
            results.append(("Database initialized", False, "Database not OK"))
    else:
        results.append(("Database initialized", False, "Could not reach health endpoint"))

    # Check 4: Service running (uptime > 0)
    if health:
        uptime = health.get("uptime_hours", 0)
        if uptime > 0:
            results.append((f"Service running (uptime: {uptime:.1f} hours)", True, None))
        else:
            results.append(("Service running", False, "Uptime is 0"))
    else:
        results.append(("Service running", False, "Could not reach health endpoint"))

    # Check 5: Cloudflare tunnel working (URL accessible from internet)
    if health:
        results.append(("Cloudflare tunnel working", True, None))
    else:
        results.append(("Cloudflare tunnel working", False, "URL not accessible"))

    # Check 6: At least one user account exists
    if health:
        users_count = health.get("users_count", 0)
        if users_count > 0:
            results.append((f"User accounts: {users_count}", True, None))
        else:
            results.append(("User accounts", False, "No user accounts found"))
    else:
        results.append(("User accounts", False, "Could not reach health endpoint"))

    # Check 7: Setup wizard was completed
    if health:
        wizard = health.get("wizard_complete", False)
        if wizard:
            results.append(("Setup wizard completed", True, None))
        else:
            results.append(("Setup wizard not completed", False, "Email client to complete setup wizard"))
    else:
        results.append(("Setup wizard completed", False, "Could not reach health endpoint"))

    return {"results": results, "health": health}


def print_report(base_url: str, results: list, health: dict | None) -> int:
    """Print formatted verification report."""
    print()
    print("\u2554" + "\u2550" * 34 + "\u2557")
    print("\u2551  LedgerLink Installation Verify  \u2551")
    print("\u255a" + "\u2550" * 34 + "\u255d")
    print()

    passed = 0
    total = len(results)
    actions = []

    for label, ok, detail in results:
        if ok:
            print(f"  \u2713 {label}")
            passed += 1
        else:
            print(f"  \u2717 {label}")
            if detail:
                actions.append(detail)

    print()
    print(f"  RESULT: {passed}/{total} checks passed")

    if actions:
        for action in actions:
            print(f"  ACTION: {action}")

    # Send appropriate email
    if passed == total:
        subject = f"[LedgerLink] Installation verified — {base_url}"
        body = (
            f"Congratulations! LedgerLink installation at {base_url} "
            f"has passed all {total} verification checks.\n\n"
            f"All systems are operational.\n"
        )
        # Try to find client email from health data
        if _send_email(subject, body, SUPPORT_EMAIL):
            print(f"  EMAIL: Congratulations email sent")
    else:
        subject = f"[LedgerLink] Installation issues — {base_url}"
        body = (
            f"LedgerLink installation verification at {base_url}\n"
            f"Result: {passed}/{total} checks passed\n\n"
            f"Issues:\n"
        )
        for label, ok, detail in results:
            status = "PASS" if ok else "FAIL"
            body += f"  [{status}] {label}\n"
            if detail and not ok:
                body += f"         Action: {detail}\n"
        if _send_email(subject, body, SUPPORT_EMAIL):
            print(f"  EMAIL: Diagnostic email sent to {SUPPORT_EMAIL}")

    print()
    return 0 if passed == total else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="LedgerLink Remote Installation Verifier",
        epilog="Example: python verify_installation.py --url https://tremblay-cpa.ledgerlink.app --license-tier professionnel",
    )
    parser.add_argument(
        "--url", required=True,
        help="Base URL of the LedgerLink instance (e.g. https://tremblay-cpa.ledgerlink.app)",
    )
    parser.add_argument(
        "--license-tier",
        help="Expected license tier (e.g. essentiel, professionnel, enterprise)",
    )

    args = parser.parse_args()

    data = verify_installation(args.url, args.license_tier)
    return print_report(args.url, data["results"], data["health"])


if __name__ == "__main__":
    raise SystemExit(main())

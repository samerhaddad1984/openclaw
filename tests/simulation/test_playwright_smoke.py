"""Test B — Playwright headless smoke test.

For each page we want a real CPA to click on, open it in a real Chromium
browser and record:
  * HTTP status / redirect behaviour
  * Page title
  * JS console errors (from page.on 'pageerror')
  * Visible text snippet (proof that something rendered, not just an empty shell)

We can't test authenticated pages without a valid password for the seeded
user, so the coverage is: anonymous probes + one authenticated probe using
a temporary test user we create + destroy. If login succeeds, we walk the
main routes.

Output: /tmp/playwright_smoke_report.md
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8787"
TEST_USER = "sim_browser_test"
TEST_PASSWORD = "Simtest-Playwright-12345!"

ANON_PATHS = [
    ("/", "landing-or-login"),
    ("/login", "login-form"),
    ("/health/page", "health"),
]

AUTH_PATHS = [
    ("/", "queue"),
    ("/clients", "clients"),
    ("/audit/anomalies", "anomalies-dashboard"),
    ("/audit/evidence", "evidence"),
    ("/engagements", "engagements"),
    ("/financial_statements", "financial-statements"),
    ("/partnerships", "partnerships"),
    ("/sred", "sred"),
    ("/tax/planning", "tax-planning"),
    ("/reconciliation/adjustments", "recon-adjustments"),
    ("/t2", "t2"),
]


def _ensure_test_user():
    """Seed a test user with a known password (bcrypt hash)."""
    import bcrypt
    pwd_hash = bcrypt.hashpw(TEST_PASSWORD.encode(), bcrypt.gensalt()).decode()
    conn = sqlite3.connect("/opt/otocpa/data/otocpa_agent.db")
    conn.execute(
        "INSERT OR REPLACE INTO dashboard_users (username, password_hash, role, "
        "display_name, active, language, created_at, must_reset_password, "
        "totp_enabled, email, firm_code) "
        "VALUES (?, ?, 'owner', 'Sim Browser Test', 1, 'en', datetime('now'), "
        "0, 0, ?, '')",
        (TEST_USER, pwd_hash, "sim_test@example.com"),
    )
    conn.commit()
    conn.close()


def _cleanup_test_user():
    conn = sqlite3.connect("/opt/otocpa/data/otocpa_agent.db")
    conn.execute("DELETE FROM dashboard_users WHERE username=?", (TEST_USER,))
    conn.commit()
    conn.close()


def _probe(page, path: str, label: str) -> dict[str, Any]:
    errs: list[str] = []
    def _rec(e):
        errs.append(str(e))
    page.on("pageerror", _rec)
    try:
        response = page.goto(f"{BASE}{path}", wait_until="domcontentloaded",
                              timeout=8000)
        status = response.status if response else None
        title = page.title()
        body_text = page.inner_text("body", timeout=2000)[:120]
        return {
            "path": path, "label": label, "status": status,
            "title": title, "body_preview": body_text,
            "js_errors": errs, "ok": True,
        }
    except Exception as e:
        return {
            "path": path, "label": label, "status": None,
            "error": f"{type(e).__name__}: {e}",
            "js_errors": errs, "ok": False,
        }


def run() -> dict:
    _ensure_test_user()
    results: list[dict[str, Any]] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # Anonymous probes.
            for path, label in ANON_PATHS:
                results.append({"phase": "anon", **_probe(page, path, label)})

            # Login flow.
            page.goto(f"{BASE}/login", wait_until="domcontentloaded", timeout=5000)
            try:
                page.fill("input[name=username]", TEST_USER, timeout=3000)
                page.fill("input[name=password]", TEST_PASSWORD, timeout=3000)
                # Find the form submit button.
                page.click("button[type=submit]", timeout=3000)
                page.wait_for_load_state("domcontentloaded", timeout=5000)
                login_final_url = page.url
                login_ok = "login" not in login_final_url
            except Exception as e:
                login_ok = False
                login_final_url = f"error: {e}"

            results.append({
                "phase": "login",
                "path": "/login",
                "label": "login-submit",
                "ok": login_ok,
                "final_url": login_final_url,
            })

            # Authenticated probes (only if login succeeded).
            if login_ok:
                for path, label in AUTH_PATHS:
                    results.append({"phase": "auth", **_probe(page, path, label)})

            browser.close()
    finally:
        _cleanup_test_user()

    # Summarise.
    passed = sum(1 for r in results
                  if r.get("ok") and (r.get("status") in (None, 200, 303, 302)
                                       or r.get("phase") == "login"))
    failed = sum(1 for r in results if not r.get("ok"))
    with_js_errors = sum(1 for r in results if r.get("js_errors"))
    return {
        "summary": {
            "total_probes": len(results),
            "ok": passed,
            "failed": failed,
            "with_js_errors": with_js_errors,
        },
        "results": results,
    }


def main():
    out = run()
    Path("/tmp/playwright_smoke_report.json").write_text(
        json.dumps(out, default=str, indent=2),
    )
    md = [
        "# Test B — Playwright headless browser smoke tests",
        "",
        f"Browser: Chromium headless (Playwright {'installed' if True else 'not available'})",
        f"Base URL: {BASE}",
        "",
        "## Summary",
        f"- Total probes: **{out['summary']['total_probes']}**",
        f"- OK: **{out['summary']['ok']}**",
        f"- Failed: **{out['summary']['failed']}**",
        f"- Pages with JS console errors: **{out['summary']['with_js_errors']}**",
        "",
        "## Per-route results",
        "",
        "| Phase | Route | Status | Title | JS errors | Preview |",
        "|---|---|---:|---|---:|---|",
    ]
    for r in out["results"]:
        title = (r.get("title") or "")[:40]
        preview = (r.get("body_preview") or "")[:50].replace("|", "/").replace("\n", " ")
        status = r.get("status") if r.get("status") is not None else (
            "LOGIN-OK" if r.get("ok") else "ERR"
        )
        je = len(r.get("js_errors") or [])
        md.append(
            f"| {r.get('phase', '')} | `{r.get('path', '')}` | {status} | "
            f"{title} | {je} | {preview} |"
        )
    Path("/tmp/playwright_smoke_report.md").write_text("\n".join(md))
    print("\n".join(md))
    return 0 if out["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(main())

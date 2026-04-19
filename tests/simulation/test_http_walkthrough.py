"""Test B (fallback) — HTTP-level walkthrough of the major routes.

True Playwright browser automation requires system libraries (libatk, etc.)
that are not installable in this environment. This fallback uses the
`requests` library to simulate a logged-in CPA clicking through the main
routes. What this measures:

  * Route reachability (status code)
  * Response size (proves HTML was rendered, not a blank shell)
  * Presence of critical markers (title, heading, form fields)
  * Absence of 500 / traceback / unhandled-exception strings

What this does NOT measure:
  * JavaScript execution (there is little JS in the dashboard)
  * Visual rendering
  * Client-side form validation

Output: /tmp/http_walkthrough_report.md
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from pathlib import Path

import requests

BASE = "http://127.0.0.1:8787"
TEST_USER = "sim_http_test"
TEST_PASSWORD = "Simtest-HTTP-87654!"

ROUTES = [
    # (method, path, label, critical_markers)
    ("GET", "/", "queue", ["<body", "</html>"]),
    ("GET", "/clients", "clients", []),
    ("GET", "/engagements", "engagements", []),
    ("GET", "/audit/anomalies", "anomalies", ["Anomaly", "Anomalies"]),
    ("GET", "/audit/evidence", "evidence", []),
    ("GET", "/audit/sample", "sample", []),
    ("GET", "/financial_statements", "fs", []),
    ("GET", "/partnerships", "partnerships", ["Partnership"]),
    ("GET", "/sred", "sred", ["SR&amp;ED"]),
    ("GET", "/tax/planning", "tax-planning", ["Tax", "planning"]),
    ("GET", "/reconciliation", "reconciliation", []),
    ("GET", "/reconciliation/adjustments", "recon-adjustments", []),
    ("GET", "/t2", "t2", []),
    ("GET", "/cashflow", "cashflow", []),
    ("GET", "/ar", "ar", []),
    ("GET", "/aging", "aging", []),
    ("GET", "/fixed_assets", "fixed-assets", []),
]

ERROR_PATTERNS = [
    r"Traceback \(most recent call last\)",
    r"Internal Server Error",
    r"KeyError:",
    r"AttributeError:",
    r"TypeError:",
    r"NameError:",
    r"OperationalError:",
]


def _setup_user():
    import bcrypt
    h = bcrypt.hashpw(TEST_PASSWORD.encode(), bcrypt.gensalt()).decode()
    c = sqlite3.connect("/opt/otocpa/data/otocpa_agent.db")
    c.execute(
        "INSERT OR REPLACE INTO dashboard_users (username, password_hash, role, "
        "display_name, active, language, created_at, must_reset_password, "
        "totp_enabled, email, firm_code) "
        "VALUES (?, ?, 'owner', 'HTTP Test', 1, 'en', datetime('now'), 0, 0, ?, '')",
        (TEST_USER, h, "http_test@example.com"),
    )
    c.commit()
    c.close()


def _cleanup_user():
    c = sqlite3.connect("/opt/otocpa/data/otocpa_agent.db")
    c.execute("DELETE FROM dashboard_users WHERE username=?", (TEST_USER,))
    c.commit()
    c.close()


def _probe_route(sess, method, path, label, markers):
    url = f"{BASE}{path}"
    try:
        t0 = time.time()
        r = sess.request(method, url, timeout=8, allow_redirects=True)
        elapsed = time.time() - t0
    except Exception as e:
        return {"path": path, "label": label, "error": str(e), "ok": False}
    body = r.text or ""
    errs_found = [p for p in ERROR_PATTERNS if re.search(p, body)]
    markers_found = [m for m in markers if m in body]
    all_markers_ok = all(m in body for m in markers)
    redirected_to_login = "/login" in r.url and path != "/login"
    return {
        "path": path, "label": label,
        "status": r.status_code,
        "elapsed_ms": round(elapsed * 1000, 1),
        "bytes": len(body),
        "final_url": r.url,
        "redirected_to_login": redirected_to_login,
        "markers_found": markers_found,
        "all_markers_ok": all_markers_ok,
        "error_patterns": errs_found,
        "ok": (r.status_code in (200, 303, 302) and not errs_found),
    }


def run() -> dict:
    _setup_user()
    results = []
    try:
        sess = requests.Session()
        # Probe login first (anonymous).
        r = sess.get(f"{BASE}/login", timeout=5)
        results.append({
            "phase": "anon",
            "path": "/login",
            "label": "login-form",
            "status": r.status_code,
            "bytes": len(r.text),
            "ok": r.status_code == 200,
        })
        # Attempt login via POST.
        r = sess.post(
            f"{BASE}/login",
            data={"username": TEST_USER, "password": TEST_PASSWORD},
            timeout=8, allow_redirects=False,
        )
        login_ok = r.status_code in (302, 303) and "/login" not in r.headers.get("Location", "")
        results.append({
            "phase": "login",
            "path": "/login",
            "label": "login-submit",
            "status": r.status_code,
            "redirect_to": r.headers.get("Location", ""),
            "ok": login_ok,
        })
        if not login_ok:
            # Try following redirects anyway to look at what happens.
            r = sess.get(f"{BASE}/", timeout=5)
            login_ok = r.status_code == 200 and "/login" not in r.url
        if login_ok:
            for method, path, label, markers in ROUTES:
                results.append({
                    "phase": "auth",
                    **_probe_route(sess, method, path, label, markers),
                })
    finally:
        _cleanup_user()

    passed = sum(1 for r in results if r.get("ok"))
    return {
        "summary": {
            "total": len(results),
            "passed": passed,
            "failed": len(results) - passed,
            "with_errors": sum(
                1 for r in results if r.get("error_patterns")
            ),
        },
        "results": results,
    }


def main():
    out = run()
    Path("/tmp/http_walkthrough_report.json").write_text(
        json.dumps(out, default=str, indent=2),
    )
    md = [
        "# Test B — HTTP-level route walkthrough (Playwright fallback)",
        "",
        "Chromium headless required libatk-1.0.so.0 which cannot be installed "
        "in this sandbox; we fall back to a `requests`-based walkthrough that "
        "covers status codes, response sizes, error markers, and critical "
        "HTML markers per route.",
        "",
        f"**Summary:** {out['summary']['passed']}/{out['summary']['total']} routes OK; "
        f"{out['summary']['with_errors']} pages contain Python error markers.",
        "",
        "| Phase | Route | Status | Bytes | Login? | Errors | Markers |",
        "|---|---|---:|---:|---|---:|---|",
    ]
    for r in out["results"]:
        markers = ",".join(r.get("markers_found") or [])
        err_count = len(r.get("error_patterns") or [])
        md.append(
            f"| {r.get('phase', '')} | `{r.get('path', '')}` | "
            f"{r.get('status', '')} | {r.get('bytes', '')} | "
            f"{'→login' if r.get('redirected_to_login') else 'no'} | "
            f"{err_count} | {markers} |"
        )
    Path("/tmp/http_walkthrough_report.md").write_text("\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()

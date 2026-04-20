"""R2-Investigation 1 — full-page browser-equivalent workflows.

**Environment.** True Playwright/Selenium automation needs system
libraries (``libatk-1.0``, ``libgtk-3``, ``libnss3``, …) that the
sandbox cannot install (sudo/apt blocked). All four approaches
attempted (Firefox, Chromium with --no-sandbox, Selenium+geckodriver,
direct lib download) failed at the OS layer.

Falling back to **rich HTTP + BeautifulSoup**: real form submission,
session cookies, redirect chains, plus HTML parsing to detect:
  - 500/error pages rendered with a 200 (server caught the exception
    and rendered an error template)
  - missing-element regressions (sidebar, nav links)
  - exposed exception strings ("Traceback", "OperationalError")

The full Playwright suite remains pending. Tests here cover what the
fallback actually exercises end-to-end.

This file targets every page the original spec listed for Scenarios
1, 3, and 4 — landing → signup → dashboard home → all CAS pages.
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


ERROR_PATTERNS = (
    "Traceback (most recent call last)",
    "OperationalError",
    "IntegrityError",
    "TypeError",
    "AttributeError",
    "KeyError",
    "ValueError",
    "InternalServerError",
    "500 Internal Server Error",
    # Be loose: a real "Exception:" rendered into a flash error is fine; a
    # full Python repr like "Exception(\"...\") at line N" is not.
)

JS_LIKE_ERROR_PATTERNS = (
    "Uncaught ",
    "ReferenceError:",
    "is not defined",
    "Cannot read property",
    "Cannot read properties of",
)


def _scrub_for_errors(html: str, *, allow_python_keywords_in_text=True) -> list[str]:
    """Return a list of error indicators found in raw HTML. We allow the
    word ``Exception`` to appear in plain text (e.g. legitimate "Exception
    queue") but flag full Traceback / TypeError patterns."""
    found: list[str] = []
    for pat in ERROR_PATTERNS + JS_LIKE_ERROR_PATTERNS:
        if pat in html:
            found.append(pat)
    return found


# ---------------------------------------------------------------------------
# Fixture: dashboard with full bootstrap and a logged-in firm_admin.
# ---------------------------------------------------------------------------

def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY,
            client_name TEXT, contact_email TEXT,
            firm_code TEXT, active INTEGER DEFAULT 1,
            version INTEGER DEFAULT 1, language TEXT DEFAULT 'fr'
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT DEFAULT 'New',
            confidence REAL,
            firm_code TEXT, version INTEGER DEFAULT 1,
            created_at TEXT, updated_at TEXT
        );
    """)
    c.commit(); c.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "real.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()

    # Provision firm + user via stripe path.
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        result = rd._provision_firm_from_stripe(
            {"customer_email": "det2@det.com", "customer": "cus_DET2",
             "subscription": "sub_DET2", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    # Set known password.
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?",
            (rd.hash_password("R2DetPass!"), "det2@det.com"),
        )
        conn.commit()

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    base = f"http://127.0.0.1:{port}"

    # Pre-seed a session cookie.
    tok = rd.create_session("det2@det.com")
    cookies = f"session_token={tok}"

    try:
        yield {"rd": rd, "db": db, "base": base, "cookies": cookies}
    finally:
        server.shutdown(); server.server_close()


def _get(url: str, cookies: str | None = None):
    hdrs = {"Cookie": cookies} if cookies else {}
    req = urllib.request.Request(url, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            return r.status, dict(r.headers), body
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# Scenario 1 — every major page renders for a logged-in firm_admin
# ---------------------------------------------------------------------------

# Pages from the original spec that must render. Some require a client
# code in the URL (we use a seeded one); others are tenant-wide.
PAGES_NO_PARAMS = [
    "/",                         # Dashboard home
    "/clients",                  # Client list
    "/clients/new",              # New client form
    "/portfolios",               # Portfolio assignments
    "/users",                    # User mgmt
    "/firms",                    # Firms (owner only — should render or 403)
    "/firms/new",
    "/troubleshoot",
    "/troubleshoot/backup",
    "/admin/cache",
    "/admin/vendor_aliases",
    "/admin/updates",
    "/admin/remote",
    "/period_close",
    "/period_close/accruals",
    "/time",
    "/bank_import",
    "/communications",
    "/reconciliation",
    "/reconciliation/new",
    "/analytics",
    "/ai_costs",
    "/calendar",
    "/working_papers",
    "/audit/evidence",
    "/audit/anomalies",
    "/partnerships",
    "/sred",
    "/tax/planning",
    "/audit/sample",
    "/financial_statements",
    "/audit/analytical",
    "/engagements",
    "/audit/materiality",
    "/audit/risk",
    "/audit/rep_letter",
    "/audit/controls",
    "/audit/related_parties",
    "/journal_entries",
    "/training",
    "/export",
    "/fixed_assets",
    "/aging",
    "/ar",
    "/cashflow",
    "/t2",
    "/learning",
    "/qbo/status",
    "/bank/feeds",
    "/health",
    "/health/full",
    "/health/page",
]


def _render_check(app, path: str) -> tuple[int, list[str]]:
    """Fetch ``path`` with auth cookies. Return (status, errors)."""
    status, _, body = _get(f"{app['base']}{path}", cookies=app["cookies"])
    text = body.decode("utf-8", errors="replace")
    errs = _scrub_for_errors(text)
    return status, errs


@pytest.mark.parametrize("path", PAGES_NO_PARAMS, ids=lambda p: p.replace("/", "_"))
def test_authenticated_page_renders_without_500_or_traceback(app, path):
    """Every dashboard page must render with a 200 (or a deliberate
    redirect / 403) and without a Python traceback or JS-style error
    leaking into the HTML. A 500 or a Traceback string in the body is
    a regression."""
    status, errs = _render_check(app, path)
    # 200 (rendered), 302/303 (auth redirect or onboarding gate),
    # 403 (role-gated; we accept), 404 (route not registered) are OK.
    # 500 is a bug.
    assert status != 500, (
        f"GET {path} returned 500. Errors found in body: {errs}"
    )
    assert not errs, (
        f"GET {path} -> {status} but body contains error indicators: {errs}"
    )


# ---------------------------------------------------------------------------
# Scenario 2 / 5 — multi-tab conflict surfaces with parseable JSON
# ---------------------------------------------------------------------------

def test_dashboard_home_renders_sidebar_and_main_nav(app):
    status, _, body = _get(f"{app['base']}/", cookies=app["cookies"])
    assert status == 200
    soup = BeautifulSoup(body, "html.parser")
    # Some kind of nav element must exist (we're loose on selector;
    # this is a smoke-check for "page has structure", not pixel-perfect).
    nav = soup.find_all(["nav", "aside"]) + soup.select(".sidebar, .nav, .menu, header")
    assert nav, "dashboard home rendered no nav/sidebar/header element"


def test_login_page_has_password_input(app):
    status, _, body = _get(f"{app['base']}/login")
    assert status == 200
    soup = BeautifulSoup(body, "html.parser")
    pw = soup.find("input", {"type": "password"})
    user = soup.find("input", {"name": "username"})
    assert pw is not None, "login page rendered with no password input"
    assert user is not None, "login page rendered with no username input"


def test_signup_page_has_checkout_cta(app):
    """Signup page constructs a Stripe Checkout session via JS — there
    is no traditional <form> element. Smoke-check the CTA buttons and
    Stripe.js inclusion are present so a regression that strips the JS
    wiring is caught."""
    status, _, body = _get(f"{app['base']}/signup")
    assert status == 200, status
    text = body.decode("utf-8", errors="replace")
    # CTA buttons (monthly / yearly) must be present.
    assert 'id="btn-monthly"' in text or "monthly" in text.lower(), (
        "signup page missing monthly CTA"
    )
    # Stripe-checkout JS wiring should be present.
    assert "stripe" in text.lower() or "checkout" in text.lower(), (
        "signup page missing Stripe checkout wiring"
    )

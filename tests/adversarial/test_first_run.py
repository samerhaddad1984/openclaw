"""R3-Investigation 1 — fresh install walks every dashboard page.

A brand-new firm provisioned via Stripe checkout has:
  - One firm row
  - One firm_admin user
  - Zero clients, zero documents, zero bank connections

The dashboard must render every major page as empty-state HTML.
Anything that 500s is a regression of the same class of
missing-column / missing-JOIN bugs from R1/R2.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1
        );
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture(scope="module")
def fresh_firm():
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="r3_fresh_"))
    db = tmp / "fresh.db"
    secret = tmp / "secret"; secret.write_text("x" * 48)
    _bootstrap(db)

    import scripts.review_dashboard as rd
    rd.DB_PATH = db
    rd.PASSWORD_LINK_SECRET_FILE = str(secret)
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()

    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "first@run.com", "customer": "cus_FR",
             "subscription": "sub_FR", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Fresh1!"), "first@run.com"),
        )
        conn.commit()
    token = rd.create_session("first@run.com")
    cookies = f"session_token={token}"

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    try:
        yield {"base": base, "cookies": cookies, "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


def _get(url, cookies):
    req = urllib.request.Request(url, headers={"Cookie": cookies})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# All pages the spec enumerated.
PAGES = [
    "/", "/clients", "/clients/new", "/bank/feeds",
    "/period_close", "/period_close/accruals",
    "/time", "/bank_import", "/communications",
    "/reconciliation", "/reconciliation/new",
    "/reconciliation/adjustments",
    "/financial_statements", "/aging", "/cashflow",
    "/fixed_assets", "/fixed_assets/schedule8",
    "/audit/evidence", "/audit/anomalies",
    "/audit/sample", "/audit/analytical",
    "/audit/materiality", "/audit/risk", "/audit/controls",
    "/audit/rep_letter", "/audit/related_parties",
    "/engagements",
    "/t2",
    "/partnerships", "/sred", "/tax/planning",
    "/journal_entries",
    "/filing_summary",
    "/working_papers",
    "/ar", "/learning",
    "/analytics", "/ai_costs",
    "/calendar",
    "/export",
    "/health", "/health/full", "/health/page",
    "/troubleshoot", "/admin/cache", "/admin/vendor_aliases",
    "/admin/updates", "/admin/remote",
    "/qbo/status",
    "/portfolios", "/users", "/firms",
]


@pytest.mark.parametrize("path", PAGES, ids=lambda p: p.replace("/", "_"))
def test_page_renders_on_fresh_install(fresh_firm, path):
    status, body = _get(f"{fresh_firm['base']}{path}", fresh_firm["cookies"])
    # 200 (rendered), 302/303 (onboarding redirect), 403 (owner-only),
    # 404 (route not registered for this role) are all acceptable.
    # 5xx is a REGRESSION.
    assert status < 500, (
        f"{path} returned {status} on a fresh install. Body: {body[:400]!r}"
    )
    # Also must not leak a Python traceback into the body.
    text = body.decode("utf-8", errors="replace")
    for pat in ("Traceback (most recent call last)", "OperationalError",
                 "KeyError: ", "AttributeError: "):
        assert pat not in text, (
            f"{path} returned {status} but leaked {pat!r} into the body"
        )


# ---------------------------------------------------------------------------
# Part B: first data-entry flows (empty states).
# ---------------------------------------------------------------------------

def test_financial_statements_on_empty_firm_renders_empty_state(fresh_firm):
    """No documents posted yet; the FS page should return a rendered
    empty-state, not crash."""
    status, body = _get(f"{fresh_firm['base']}/financial_statements",
                         fresh_firm["cookies"])
    assert status in (200, 302, 303), status


def test_reconciliation_with_no_bank_transactions(fresh_firm):
    status, body = _get(f"{fresh_firm['base']}/reconciliation",
                         fresh_firm["cookies"])
    assert status in (200, 302, 303)


def test_aging_with_no_ar(fresh_firm):
    status, body = _get(f"{fresh_firm['base']}/aging", fresh_firm["cookies"])
    assert status in (200, 302, 303)

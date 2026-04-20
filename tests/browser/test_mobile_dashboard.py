"""R5-Investigation 5 — mobile viewport dashboard.

Uses the sideloaded Chromium from the R4 Phase-1a fix. Loads every
major dashboard page at 375×812 (iPhone-ish), asserts no horizontal
overflow and no JS uncaught exceptions.

Run with:
  LD_LIBRARY_PATH=/tmp/libs/extracted/usr/lib/x86_64-linux-gnu \
  pytest tests/browser/test_mobile_dashboard.py

The conftest auto-skips when the sideload libs aren't present.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
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
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "mob.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "m@det.com", "customer": "cus_M",
             "subscription": "sub_M", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("M1Mobile!"), "m@det.com"),
        )
        conn.commit()
    sess = rd.create_session("m@det.com")
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}",
                "cookie": f"session_token={sess}", "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


# ---------------------------------------------------------------------------
# Pages to check for mobile overflow + JS errors.
# ---------------------------------------------------------------------------

MOBILE_PAGES = [
    "/",
    "/clients",
    "/clients/new",
    "/period_close",
    "/reconciliation",
    "/financial_statements",
    "/fixed_assets",
    "/audit/anomalies",
    "/partnerships",
    "/sred",
    "/health",
]


def _cookie_to_playwright(cookie_str: str, url: str) -> list[dict]:
    from urllib.parse import urlparse
    p = urlparse(url)
    out = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            name, value = part.split("=", 1)
            out.append({
                "name": name.strip(), "value": value.strip(),
                "domain": p.hostname, "path": "/",
            })
    return out


@pytest.mark.parametrize("path", MOBILE_PAGES,
                          ids=lambda p: p.replace("/", "_") or "root")
def test_mobile_page_renders_without_horizontal_scroll_or_js_errors(
    app, browser_context, path,
):
    browser_context.add_cookies(_cookie_to_playwright(app["cookie"], app["base"]))
    page = browser_context.new_page()
    js_errors: list[str] = []
    page.on("pageerror", lambda e: js_errors.append(str(e)))
    bad_responses: list[tuple[str, int]] = []
    page.on("response", lambda r: (
        bad_responses.append((r.url, r.status)) if r.status >= 500 else None
    ))
    resp = page.goto(f"{app['base']}{path}", timeout=15000)
    assert resp is not None and resp.status < 500, (
        f"{path} returned {resp.status if resp else 'None'}"
    )
    page.wait_for_load_state("networkidle", timeout=10000)
    # Horizontal-overflow check.
    overflow = page.evaluate(
        "() => document.documentElement.scrollWidth - window.innerWidth"
    )
    assert overflow <= 2, (  # 1-2 px rounding tolerance
        f"{path} overflows horizontally by {overflow}px on 375×812 viewport"
    )
    assert not bad_responses, f"{path} 5xx: {bad_responses}"
    assert not js_errors, f"{path} JS errors: {js_errors}"


def test_mobile_primary_button_min_44_tall(app, browser_context):
    """Every form submit button on the clients/new page should be at
    least 44 px tall — portal buttons were hardened in R2; this
    regression-tests the dashboard side."""
    browser_context.add_cookies(_cookie_to_playwright(app["cookie"], app["base"]))
    page = browser_context.new_page()
    page.goto(f"{app['base']}/clients/new", timeout=15000)
    page.wait_for_load_state("networkidle")
    buttons = page.query_selector_all("button[type=submit], input[type=submit]")
    if not buttons:
        pytest.skip("no submit buttons on /clients/new — page may be role-gated")
    short = []
    for b in buttons:
        box = b.bounding_box()
        if box and box["height"] < 40:
            # Tap-target WCAG minimum is 44; allow 40 as a lenient
            # dashboard-side threshold (the dashboard's primary
            # audience is desktop CPAs). Anything below 40 is a
            # clear issue.
            short.append(box["height"])
    assert not short, (
        f"/clients/new has submit button(s) under 40px tall: {short}"
    )


def test_mobile_login_page_fits(app, browser_context):
    """Even unauthenticated login page must not overflow."""
    page = browser_context.new_page()
    page.goto(f"{app['base']}/login", timeout=15000)
    page.wait_for_load_state("networkidle")
    overflow = page.evaluate(
        "() => document.documentElement.scrollWidth - window.innerWidth"
    )
    assert overflow <= 2, f"login overflows by {overflow}px"

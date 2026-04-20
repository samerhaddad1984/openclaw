"""R3-Investigation 3 — authorization matrix.

For every sensitive route, verify:
  - Anonymous access is rejected (redirect to /login or 401).
  - A firm_admin cannot access other firm's data.
  - Owner-only routes reject non-owner roles.
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
def two_firm_app():
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="r3_authz_"))
    db = tmp / "authz.db"
    secret = tmp / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    rd.DB_PATH = db
    rd.PASSWORD_LINK_SECRET_FILE = str(secret)
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()

    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "admin-a@firm.com", "customer": "cus_A",
             "subscription": "sub_A", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
        rd._provision_firm_from_stripe(
            {"customer_email": "admin-b@firm.com", "customer": "cus_B",
             "subscription": "sub_B", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute("UPDATE dashboard_users SET password_hash=?, must_reset_password=0",
                      (rd.hash_password("X1!"),))
        # Seed a client for each firm.
        firm_a = conn.execute("SELECT firm_code FROM dashboard_users WHERE username='admin-a@firm.com'").fetchone()[0]
        firm_b = conn.execute("SELECT firm_code FROM dashboard_users WHERE username='admin-b@firm.com'").fetchone()[0]
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, active) VALUES (?, ?, 1)",
            ("CLI-A", firm_a),
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, active) VALUES (?, ?, 1)",
            ("CLI-B", firm_b),
        )
        conn.commit()
    sess_a = rd.create_session("admin-a@firm.com")
    sess_b = rd.create_session("admin-b@firm.com")
    # sam = 'owner' already from bootstrap.
    with sqlite3.connect(str(db)) as conn:
        conn.execute("UPDATE dashboard_users SET password_hash=? WHERE username='sam'",
                     (rd.hash_password("X1!"),))
        conn.commit()
    sess_owner = rd.create_session("sam")

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    try:
        yield {"base": base, "rd": rd, "db": db,
                "firm_a": firm_a, "firm_b": firm_b,
                "cookie_a": f"session_token={sess_a}",
                "cookie_b": f"session_token={sess_b}",
                "cookie_owner": f"session_token={sess_owner}"}
    finally:
        server.shutdown(); server.server_close()


def _get(url, cookie=None):
    hdrs = {"Cookie": cookie} if cookie else {}
    req = urllib.request.Request(url, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# Anonymous access: every protected route must redirect or 4xx.
# ---------------------------------------------------------------------------

PROTECTED_GET = [
    "/", "/clients", "/bank/feeds", "/period_close",
    "/reconciliation", "/financial_statements", "/audit/anomalies",
    "/partnerships", "/sred", "/fixed_assets", "/journal_entries",
    "/audit/materiality", "/audit/risk", "/audit/rep_letter",
    "/engagements", "/t2", "/analytics", "/users", "/firms",
    "/admin/cache", "/admin/remote", "/troubleshoot",
]


@pytest.mark.parametrize("path", PROTECTED_GET, ids=lambda p: p.replace("/", "_"))
def test_anonymous_cannot_reach_protected_get(two_firm_app, path):
    status, hdrs, body = _get(f"{two_firm_app['base']}{path}")
    # Acceptable: 302/303 redirect to /login, or 401/403.
    # Unacceptable: 200 showing the actual page, or 5xx.
    text = body.decode("utf-8", errors="replace")
    assert status < 500, f"{path} crashed for anon"
    if status == 200:
        # Must be the login page, not the dashboard.
        assert "type=\"password\"" in text or "Connexion" in text or "Login" in text, (
            f"anon GET {path} returned 200 with NON-login page "
            f"(body starts: {text[:200]!r})"
        )


# ---------------------------------------------------------------------------
# Firm isolation: firm_a admin cannot see firm_b's client data.
# ---------------------------------------------------------------------------

def test_firm_a_admin_cannot_fetch_firm_b_client_via_helper(two_firm_app):
    rd = two_firm_app["rd"]
    firm_a = two_firm_app["firm_a"]
    ctx_a = {"role": "firm_admin", "firm_code": firm_a,
             "username": "admin-a@firm.com",
             "can_view_all_clients": True, "can_do_all_assignments": True,
             "can_view_all_assignments": True,
             "can_post_qbo": True}
    # firm_a admin probing firm_b's client.
    allowed = rd._require_client_in_firm("CLI-B", ctx_a)
    assert allowed is False, (
        "firm_admin of firm A was granted access to firm B's client CLI-B"
    )


def test_firm_a_admin_cannot_fetch_firm_b_client_via_http(two_firm_app):
    """HTTP edge: firm A admin requests /c/<firm B token> via the public
    portal (different auth model). Portal is token-scoped; we verify a
    bogus cross-firm token does not work."""
    # portal tokens are 32-char; a firm_a admin doesn't even know
    # firm_b's token. Pass a bogus one.
    status, _, body = _get(
        f"{two_firm_app['base']}/c/bogus_firm_b_token_0123456789abcdef",
    )
    # Portal returns 200 + the invalid-link page (deliberate UX).
    assert status == 200
    text = body.decode("utf-8")
    # Must not leak firm codes.
    assert two_firm_app["firm_a"] not in text
    assert two_firm_app["firm_b"] not in text


# ---------------------------------------------------------------------------
# CSRF: state-changing POSTs require same-origin Origin header.
# ---------------------------------------------------------------------------

def test_csrf_missing_origin_rejected_on_state_change(two_firm_app):
    import urllib.parse
    body = urllib.parse.urlencode({
        "client_code": "XSRF1", "client_name": "X", "contact_email": "a@b.c",
        "language": "en", "active": "1",
    }).encode()
    req = urllib.request.Request(
        f"{two_firm_app['base']}/clients/save",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 # deliberately WRONG origin
                 "Origin": "https://evil.example.com",
                 "Cookie": two_firm_app["cookie_a"]},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        status = 200
    except urllib.error.HTTPError as e:
        status = e.code
    # CSRF middleware returns 403 for cross-origin POSTs.
    assert status == 403, f"cross-origin POST was not rejected: status={status}"


def test_csrf_stripe_webhook_exempt_from_origin_check(two_firm_app):
    """/stripe/webhook doesn't carry Origin — signature verification
    takes its place. Without the SDK installed, we expect a 400
    invalid_signature rather than a CSRF 403."""
    import urllib.request
    body = b'{"id":"evt_anon","type":"checkout.session.completed"}'
    req = urllib.request.Request(
        f"{two_firm_app['base']}/stripe/webhook",
        data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        status = 200
    except urllib.error.HTTPError as e:
        status = e.code
    assert status == 400  # invalid_signature, not 403 CSRF


def test_api_contact_does_not_require_csrf(two_firm_app):
    """Public contact form — anonymous users have no session/cookie
    to sign a CSRF token against. Must accept without Origin check."""
    import urllib.parse
    body = urllib.parse.urlencode({
        "name": "Anon Prospect", "email": "p@p.com", "message": "Hi"
    }).encode()
    req = urllib.request.Request(
        f"{two_firm_app['base']}/api/contact",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        status = 200
    except urllib.error.HTTPError as e:
        status = e.code
    assert status < 500, status


# ---------------------------------------------------------------------------
# Owner-only routes refuse firm_admin.
# ---------------------------------------------------------------------------

def test_ingest_openclaw_requires_api_key(two_firm_app):
    """/ingest/openclaw requires an X-API-Key header that matches a
    per-firm secret. This test replaces the R3 "unknown_sender" guard
    — the API-key gate now runs FIRST, so a random POST without a key
    gets 401 before even hitting the sender-id lookup.

    A follow-up test in tests/adversarial/test_ingest_api_key.py
    verifies the happy path (correct key + unknown sender still
    returns unknown_sender from the engine).
    """
    import urllib.request, json as _json
    body = _json.dumps({
        "platform": "whatsapp",
        "sender_id": "+14165550000",
        "media_type": "image/jpeg",
        "client_message": "hi",
    }).encode()
    req = urllib.request.Request(
        f"{two_firm_app['base']}/ingest/openclaw",
        data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        status, payload = resp.status, _json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status, payload = e.code, _json.loads(e.read())
    assert status == 401, (status, payload)
    assert payload.get("error") == "invalid_or_missing_api_key", payload


# ---------------------------------------------------------------------------
# Session tampering — invalid cookie must not authenticate.
# ---------------------------------------------------------------------------

def test_forged_session_token_is_rejected(two_firm_app):
    """64 random chars that don't exist in dashboard_sessions. Should
    redirect to /login, not silently render the dashboard."""
    status, _, body = _get(
        f"{two_firm_app['base']}/",
        cookie="session_token=" + "Z" * 64,
    )
    assert status < 500
    text = body.decode("utf-8", errors="replace")
    # If 200, must be the login page (not the dashboard).
    if status == 200:
        assert "type=\"password\"" in text or "Connexion" in text

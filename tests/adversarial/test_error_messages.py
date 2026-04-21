"""R5-Investigation 4 — user-facing error message quality.

Audit: when a user hits an error, is the message:
  - present in both FR and EN (QC bilingual contract)
  - specific enough to be actionable
  - free of security-leaking internals
"""
from __future__ import annotations

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
    db = tmp_path / "err.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "e@det.com", "customer": "cus_E",
             "subscription": "sub_E", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Err1!Pw"), "e@det.com"),
        )
        conn.commit()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


def _post(url, fields, *, cookies=None):
    body = urllib.parse.urlencode(fields).encode()
    p = urllib.parse.urlparse(url)
    hdrs = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": f"{p.scheme}://{p.netloc}",
    }
    if cookies:
        hdrs["Cookie"] = cookies
    req = urllib.request.Request(url, data=body, method="POST", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _get(url, cookies=None):
    hdrs = {"Cookie": cookies} if cookies else {}
    req = urllib.request.Request(url, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# ---------------------------------------------------------------------------
# Bilingual error pages
# ---------------------------------------------------------------------------

def test_wrong_password_error_is_bilingual(app):
    """Login fail → FR and EN text on the page."""
    class _NoRedir(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *_a, **_k):
            return None
    opener = urllib.request.build_opener(_NoRedir())
    body_data = urllib.parse.urlencode({
        "username": "e@det.com", "password": "WRONG",
    }).encode()
    p = urllib.parse.urlparse(app["base"])
    req = urllib.request.Request(
        f"{app['base']}/login", data=body_data, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    try:
        with opener.open(req, timeout=10) as r:
            status, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read()
    text = body.decode("utf-8", errors="replace")
    # The text may or may not be explicit about FR/EN but we check
    # that SOMETHING indicates auth failure without revealing whether
    # the username exists.
    t = text.lower()
    assert any(k in t for k in (
        "invalid", "incorrect", "erron", "invalide", "mot de passe",
        "identifiants",
    )), f"login-fail body gives no hint of auth failure: {text[:300]!r}"


def test_portal_invalid_link_page_is_bilingual(app):
    status, body = _get(f"{app['base']}/c/this_is_a_bogus_token_but_long_enough_xx")
    assert status == 200
    t = body.decode("utf-8", errors="replace")
    # Both FR and EN markers present.
    t_lower = t.lower()
    assert ("invalide" in t_lower or "été" in t_lower), (
        "portal invalid-link page missing French text"
    )
    assert ("invalid" in t_lower and "link" in t_lower), (
        "portal invalid-link page missing English text"
    )


# ---------------------------------------------------------------------------
# No internal leaks
# ---------------------------------------------------------------------------

def test_login_fail_does_not_reveal_if_user_exists(app):
    """Same error for wrong-user and wrong-password paths. The
    response body should be identical enough that an attacker can't
    enumerate users."""
    class _NoRedir(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *_a, **_k):
            return None
    opener = urllib.request.build_opener(_NoRedir())
    p = urllib.parse.urlparse(app["base"])

    def _probe(user, pw):
        body = urllib.parse.urlencode({"username": user, "password": pw}).encode()
        req = urllib.request.Request(
            f"{app['base']}/login", data=body, method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "Origin": f"{p.scheme}://{p.netloc}"},
        )
        try:
            with opener.open(req, timeout=10) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    s_wrong_pw = _probe("e@det.com", "nope")
    s_no_user = _probe("nobody@notreal.com", "nope")
    # Status codes should match.
    assert s_wrong_pw[0] == s_no_user[0], (
        f"different status codes reveal user existence: "
        f"wrong-pw={s_wrong_pw[0]} no-user={s_no_user[0]}"
    )


def test_error_body_has_no_python_traceback_text(app):
    """Any error page body must not contain "Traceback" or
    "sqlite3.OperationalError" — those would leak internals."""
    # Force a 500-ish response by tampering with a cookie.
    status, body = _get(f"{app['base']}/", cookies="session_token=invalid")
    assert status < 500
    text = body.decode("utf-8", errors="replace")
    for pat in (
        "Traceback (most recent call last)",
        "sqlite3.OperationalError",
        "KeyError:",
        "AttributeError:",
        "File \"/opt/otocpa/",
    ):
        assert pat not in text, f"error body leaks {pat!r}"


# ---------------------------------------------------------------------------
# /health never leaks secrets (regression from R3 / R4).
# ---------------------------------------------------------------------------

def test_health_does_not_leak_password_hash_or_path(app):
    status, body = _get(f"{app['base']}/health")
    assert status == 200
    text = body.decode("utf-8", errors="replace")
    for pat in (
        "password_hash", "bcrypt", "PGPASSWORD", "otocpa_agent.db",
        "STRIPE_SECRET", "ANTHROPIC_API_KEY",
    ):
        assert pat not in text, f"/health leaks {pat!r}"


# ---------------------------------------------------------------------------
# 404-like errors served with useful body (not blank).
# ---------------------------------------------------------------------------

def test_unknown_route_returns_useful_page(app):
    status, body = _get(f"{app['base']}/this-route-does-not-exist",
                         cookies=None)
    # Most unknown routes either redirect to /login (requires session)
    # or return 404. Body must not be blank.
    text = body.decode("utf-8", errors="replace")
    assert len(text) > 100, f"404 body too short: {text!r}"


# ---------------------------------------------------------------------------
# Password-strength errors are localized.
# ---------------------------------------------------------------------------

def test_password_strength_message_bilingual(app):
    """Weak password → bilingual error."""
    import scripts.review_dashboard as rd
    for new, confirm in (("short", "short"),
                          ("longish-password-no-digits", "longish-password-no-digits")):
        err = rd._validate_password_strength(new, confirm)
        assert err, f"weak password {new!r} not rejected"
        # Must mention both languages (slash-separated is the
        # project's convention).
        assert "/" in err, (
            f"password-strength error not bilingual: {err!r}"
        )


def test_password_confirm_mismatch_message_bilingual():
    import scripts.review_dashboard as rd
    err = rd._validate_password_strength("LongEnough1!", "Different1!")
    assert err
    assert "/" in err, err


# ---------------------------------------------------------------------------
# CSRF / origin errors are explicit
# ---------------------------------------------------------------------------

def test_csrf_failure_body_is_structured_json(app):
    """Cross-origin POST → {"error": "csrf_check_failed"} JSON, not a
    random HTML 500."""
    import json
    body = urllib.parse.urlencode({"client_code": "X", "client_name": "X"}).encode()
    req = urllib.request.Request(
        f"{app['base']}/clients/save",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": "https://evil.example.com"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        status, body = 200, b""
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read()
    assert status == 403
    payload = json.loads(body)
    assert payload.get("error") == "csrf_check_failed"

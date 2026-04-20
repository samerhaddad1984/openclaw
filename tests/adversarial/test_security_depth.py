"""R3-Investigation 7 — deep security hardening.

Goes past the basic pentest coverage: TOCTOU, cookie flags, timing
side-channels, password-reset token lifecycle, secret-leak scanning.
"""
from __future__ import annotations

import hmac
import os
import re
import sqlite3
import sys
import threading
import time
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
    db = tmp_path / "sec.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "sec@det.com", "customer": "cus_SEC",
             "subscription": "sub_SEC", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Sec1!"), "sec@det.com"),
        )
        conn.commit()
    sess = rd.create_session("sec@det.com")
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}",
                "cookie": f"session_token={sess}",
                "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


def _req(url, method="GET", *, cookie=None, body=None, headers=None):
    hdrs = dict(headers or {})
    if cookie:
        hdrs["Cookie"] = cookie
    req = urllib.request.Request(url, data=body, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# Session cookie security
# ---------------------------------------------------------------------------

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_a, **_k):
        return None


_NO_REDIR_OPENER = urllib.request.build_opener(_NoRedirect())


def _login_set_cookie(app) -> str:
    """POST /login and capture the Set-Cookie from the 30x redirect
    response directly (urllib follows redirects by default, dropping
    the Set-Cookie; we disable that)."""
    body = urllib.parse.urlencode({"username": "sec@det.com",
                                    "password": "Sec1!"}).encode()
    p = urllib.parse.urlparse(app["base"])
    req = urllib.request.Request(
        f"{app['base']}/login",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    try:
        with _NO_REDIR_OPENER.open(req, timeout=10) as r:
            return "\n".join(r.headers.get_all("Set-Cookie") or [])
    except urllib.error.HTTPError as e:
        return "\n".join(e.headers.get_all("Set-Cookie") or [])


def test_session_cookie_sets_httponly_on_login(app):
    cookies = _login_set_cookie(app)
    assert "session_token=" in cookies, f"no session cookie: {cookies!r}"
    # session_token line must include HttpOnly.
    session_line = [line for line in cookies.split("\n")
                     if "session_token=" in line][0]
    assert "HttpOnly" in session_line, f"session missing HttpOnly: {session_line!r}"


def test_session_cookie_sets_samesite(app):
    cookies = _login_set_cookie(app)
    session_line = [line for line in cookies.split("\n")
                     if "session_token=" in line][0]
    # The dashboard sets SameSite=Lax (or Strict on HTTPS).
    assert "SameSite" in session_line, (
        f"session cookie missing SameSite: {session_line!r}"
    )


# ---------------------------------------------------------------------------
# Password reset lifecycle
# ---------------------------------------------------------------------------

def test_password_reset_token_single_use_or_documented_reuse(app):
    """A signed password-reset link should ideally be one-shot. The
    dashboard currently accepts the same token multiple times within
    its 72-hour window (no one-shot registry). This test pins down
    current behavior:

    - If reuse is rejected (hardened), the post-first-use password is
      "NewPass1!". That's the desired state.
    - If reuse succeeds, the post-second-use password is "EvenNewer1!".
      That's a LOW finding documented in the R3 report.

    Either way, the handler must NOT crash. Anything that breaks both
    passwords is a regression.
    """
    rd = app["rd"]
    token = rd._generate_password_link("sec@det.com")
    p = urllib.parse.urlparse(app["base"])
    # First: correct form field names are `new_password` and
    # `confirm_password` (checked via the live handler at
    # scripts/review_dashboard.py:19706-19707).
    body1 = urllib.parse.urlencode({
        "token": token, "new_password": "NewPass1!",
        "confirm_password": "NewPass1!",
    }).encode()
    status_a, _, _ = _req(
        f"{app['base']}/set-password", method="POST",
        body=body1,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    assert status_a in (200, 302, 303), (status_a,)
    body2 = urllib.parse.urlencode({
        "token": token, "new_password": "EvenNewer1!",
        "confirm_password": "EvenNewer1!",
    }).encode()
    status_b, _, _ = _req(
        f"{app['base']}/set-password", method="POST",
        body=body2,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    assert status_b < 500, (status_b,)

    with sqlite3.connect(str(app["db"])) as conn:
        conn.row_factory = sqlite3.Row
        hash_now = conn.execute(
            "SELECT password_hash FROM dashboard_users WHERE username='sec@det.com'",
        ).fetchone()["password_hash"]
    new_matches = rd.verify_password("NewPass1!", hash_now)
    newer_matches = rd.verify_password("EvenNewer1!", hash_now)
    assert new_matches or newer_matches, (
        "neither password matches — reset flow broken"
    )


def test_password_reset_token_expires_after_72h(app, monkeypatch):
    """Signed tokens include the issued-at timestamp. A token generated
    73 hours ago should be rejected by _verify_password_link."""
    rd = app["rd"]
    import time as _time
    real_time = _time.time
    # Generate a token "73 hours ago" by spoofing time.time().
    fake_now = real_time() - 73 * 3600
    with patch("scripts.review_dashboard.time.time",
                side_effect=lambda: fake_now):
        expired = rd._generate_password_link("sec@det.com", expires_hours=72)
    # Verify with real time.
    verified = rd._verify_password_link(expired)
    assert verified is None, (
        "73-hour-old password-reset token was accepted as valid — "
        "expiry check missing or broken"
    )


# ---------------------------------------------------------------------------
# Timing attacks
# ---------------------------------------------------------------------------

def test_forgot_password_does_not_reveal_valid_email(app):
    """Requesting a password reset for an unknown email must complete
    in about the same time as for a known email. A large delta leaks
    whether the email is registered."""
    base = app["base"]
    p = urllib.parse.urlparse(base)
    hdrs = {"Content-Type": "application/x-www-form-urlencoded",
            "Origin": f"{p.scheme}://{p.netloc}"}

    def _time_request(email: str) -> float:
        body = urllib.parse.urlencode({"username": email}).encode()
        t0 = time.perf_counter()
        req = urllib.request.Request(f"{base}/forgot",
                                       data=body, method="POST", headers=hdrs)
        try:
            urllib.request.urlopen(req, timeout=10).read()
        except urllib.error.HTTPError as e:
            e.read()
        return time.perf_counter() - t0

    # Run several iterations for stability.
    known_times = [_time_request("sec@det.com") for _ in range(5)]
    unknown_times = [_time_request("nonexistent@det.com") for _ in range(5)]
    avg_known = sum(known_times) / len(known_times)
    avg_unknown = sum(unknown_times) / len(unknown_times)
    ratio = max(avg_known, avg_unknown) / min(avg_known, avg_unknown)
    # Tolerate 3x ratio (CI noise can push either side); anything above
    # that is a real side-channel signal.
    assert ratio < 3.0, (
        f"/forgot timing side-channel: known={avg_known*1000:.1f}ms "
        f"unknown={avg_unknown*1000:.1f}ms ratio={ratio:.2f}x"
    )


def test_login_response_constant_time_on_wrong_password(app):
    """Known email + wrong password vs unknown email + any password.
    Both should run through the password verification path so timings
    are similar."""
    base = app["base"]
    p = urllib.parse.urlparse(base)
    hdrs = {"Content-Type": "application/x-www-form-urlencoded",
            "Origin": f"{p.scheme}://{p.netloc}"}

    def _time(u, pw):
        body = urllib.parse.urlencode({"username": u, "password": pw}).encode()
        t0 = time.perf_counter()
        req = urllib.request.Request(f"{base}/login",
                                       data=body, method="POST", headers=hdrs)
        try:
            urllib.request.urlopen(req, timeout=10).read()
        except urllib.error.HTTPError as e:
            e.read()
        return time.perf_counter() - t0

    known_wrong = [_time("sec@det.com", "WrongPass9!") for _ in range(3)]
    unknown = [_time("nosuch@det.com", "WrongPass9!") for _ in range(3)]
    a = sum(known_wrong) / len(known_wrong)
    b = sum(unknown) / len(unknown)
    ratio = max(a, b) / min(a, b)
    # 4x tolerance — bcrypt is slow and rate-limiting can add noise,
    # but we still catch "if user exists, call bcrypt; else return fast".
    assert ratio < 4.0, (
        f"login timing side-channel: known-wrong={a*1000:.1f}ms "
        f"unknown={b*1000:.1f}ms ratio={ratio:.2f}x"
    )


# ---------------------------------------------------------------------------
# Secret-leak scans
# ---------------------------------------------------------------------------

def test_health_endpoint_does_not_leak_db_path_or_secrets(app):
    status, _, body = _req(f"{app['base']}/health")
    assert status == 200
    text = body.decode("utf-8", errors="replace")
    for pat in (r"/opt/otocpa/data/otocpa_agent\.db",
                r"PGPASSWORD", r"STRIPE_",
                r"password_hash", r"bcrypt", r"sha256"):
        assert not re.search(pat, text, re.IGNORECASE), (
            f"/health leaked pattern {pat!r}"
        )


def test_error_response_does_not_leak_traceback_on_production_path(app):
    """A 500 on a known crash point must render a user-facing message,
    not a Python traceback."""
    # Trigger a crash by requesting a path that might explode on
    # session cookie tampering. The dashboard's outer exception handler
    # catches and flash-redirects.
    status, _, body = _req(
        f"{app['base']}/",
        cookie="session_token=\x00\xff\x01",
    )
    text = body.decode("utf-8", errors="replace")
    for pat in ("Traceback (most recent call last)",
                "OperationalError at",
                "File \"/opt/otocpa/", "line 2"):
        assert pat not in text, f"error response leaked {pat!r}"


def test_referrer_policy_set_on_portal_pages(app):
    rd = app["rd"]
    # Create an active client + token.
    tok = rd.generate_portal_token()
    with sqlite3.connect(str(app["db"])) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, active, "
            "portal_token, portal_token_created_at) "
            "VALUES ('P1', 'OWNER', 1, ?, datetime('now'))", (tok,),
        )
        conn.commit()
    status, hdrs, _ = _req(f"{app['base']}/c/{tok}")
    assert status == 200
    assert hdrs.get("Referrer-Policy", "").lower() == "no-referrer", (
        f"portal missing Referrer-Policy: no-referrer. Got: "
        f"{hdrs.get('Referrer-Policy')!r}"
    )

"""R3-Investigation 5 — UI state corruption.

Attacker manipulates the client-side state: cookie tampering, hidden-
field tampering, URL-param attacks. Every route must treat the input
as untrusted and reject or sanitize.
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
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1
        );
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "ui.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "ui@det.com", "customer": "cus_UI",
             "subscription": "sub_UI", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Ui1!"), "ui@det.com"),
        )
        conn.commit()
    sess = rd.create_session("ui@det.com")
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}",
                "cookie": f"session_token={sess}",
                "rd": rd, "db": db}
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


def _assert_no_traceback(body: bytes, where: str):
    text = body.decode("utf-8", errors="replace")
    for pat in ("Traceback (most recent call last)", "OperationalError",
                 "KeyError: ", "AttributeError: "):
        assert pat not in text, f"{where}: leaked {pat!r}"


# ---------------------------------------------------------------------------
# Session cookie corruption
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tag,cookie", [
    ("malformed_json", "session_token={not-valid-json}"),
    ("empty_token", "session_token="),
    ("super_long", "session_token=" + "Z" * 10_000),
    ("null_byte", "session_token=abc\x00def"),
    ("unicode", "session_token=üñîçødé"),
    ("extra_attrs", "session_token=abc; admin=true; role=owner"),
    ("multiple_tokens", "session_token=one; session_token=two"),
])
def test_dashboard_rejects_tampered_session_cookie(app, tag, cookie):
    status, _, body = _get(f"{app['base']}/", cookie=cookie)
    assert status < 500, f"tampered cookie ({tag}) crashed: {status}"
    text = body.decode("utf-8", errors="replace")
    _assert_no_traceback(body, f"session_cookie/{tag}")
    # Must land on /login, not the dashboard.
    if status == 200:
        assert ("type=\"password\"" in text or "Connexion" in text
                or "Login" in text), f"{tag}: returned 200 with non-login body"


# ---------------------------------------------------------------------------
# URL parameter attacks
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path,qs", [
    ("/document", {"id": "../../../etc/passwd"}),
    ("/document", {"id": "99999999999"}),
    ("/document", {"id": "'; DROP TABLE documents;--"}),
    ("/document", {"id": "\x00\x01\x02"}),
    ("/document", {"id": "'"}),
    ("/clients/edit", {"code": "../../etc/passwd"}),
    ("/", {"q": "' OR 1=1--"}),
    ("/", {"status": "x" * 10_000}),
    ("/aging", {"client_code": "' UNION SELECT password_hash FROM dashboard_users--"}),
    ("/financial_statements", {"client_code": "NONEXISTENT", "period": "2099-99"}),
    ("/audit/evidence", {"client_code": "' OR '1'='1", "period": "2024-Q4"}),
])
def test_url_param_attack_does_not_crash_or_leak(app, path, qs):
    url = f"{app['base']}{path}?{urllib.parse.urlencode(qs)}"
    status, _, body = _get(url, cookie=app["cookie"])
    assert status < 500, (
        f"URL attack on {path} crashed: {status}\nbody: {body[:200]!r}"
    )
    _assert_no_traceback(body, f"url/{path}")


# ---------------------------------------------------------------------------
# Form with extra unknown fields — handlers should ignore, not reflect.
# ---------------------------------------------------------------------------

def test_extra_unknown_form_fields_are_ignored(app):
    body = urllib.parse.urlencode({
        "client_code": "XTRA", "client_name": "X", "contact_email": "a@b.com",
        "language": "en", "active": "1",
        # Attacker-smuggled fields hoping the handler copies them to DB.
        "role": "owner",
        "password_hash": "pwn",
        "firm_code": "OWNER",  # attempt to bypass firm scoping
        "id": "1 OR 1=1",
    }).encode()
    p = urllib.parse.urlparse(app["base"])
    req = urllib.request.Request(
        f"{app['base']}/clients/save",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}",
                 "Cookie": app["cookie"]},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError:
        pass
    # Client was created but firm_code is scoped to the firm_admin's own
    # firm, NOT the attacker's claimed 'OWNER'.
    with sqlite3.connect(str(app["db"])) as conn:
        row = conn.execute(
            "SELECT firm_code FROM clients WHERE client_code='XTRA'",
        ).fetchone()
    if row is not None:
        assert row[0] != "OWNER", (
            "firm_admin smuggled firm_code=OWNER into /clients/save — "
            "handler didn't scope firm_code to the caller's own firm"
        )


# ---------------------------------------------------------------------------
# Double-submit of the same form.
# ---------------------------------------------------------------------------

def test_double_submit_client_save_is_safe(app):
    """Two identical POSTs shouldn't produce two rows with same
    client_code (PK) or corrupt state."""
    body = urllib.parse.urlencode({
        "client_code": "DBL", "client_name": "Double Tap",
        "contact_email": "a@b.com", "language": "en", "active": "1",
    }).encode()
    p = urllib.parse.urlparse(app["base"])
    hdrs = {"Content-Type": "application/x-www-form-urlencoded",
            "Origin": f"{p.scheme}://{p.netloc}", "Cookie": app["cookie"]}
    for _ in range(2):
        req = urllib.request.Request(
            f"{app['base']}/clients/save",
            data=body, method="POST", headers=hdrs,
        )
        try:
            urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError:
            pass
    with sqlite3.connect(str(app["db"])) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM clients WHERE client_code='DBL'",
        ).fetchone()[0]
    assert n == 1, f"double-submit produced {n} rows; PK was not enforced"


# ---------------------------------------------------------------------------
# Malformed or oversized headers.
# ---------------------------------------------------------------------------

def test_extremely_long_cookie_header_does_not_crash(app):
    """8 KB cookie header — some reverse proxies cap at 8 KB, python's
    http.server handles it up to ~65 KB. Must not 500."""
    cookie = "session_token=" + "A" * (8 * 1024)
    status, _, body = _get(f"{app['base']}/login", cookie=cookie)
    assert status < 500, status
    _assert_no_traceback(body, "long_cookie")


def test_extremely_long_url_does_not_crash(app):
    """64 KB URL. Some systems reject at TCP; the dashboard should not
    panic either way."""
    long_q = "a" * (64 * 1024)
    try:
        status, _, body = _get(f"{app['base']}/login?q={long_q}")
    except Exception:
        # Network-level rejection is fine; crash of server is not.
        return
    assert status < 500

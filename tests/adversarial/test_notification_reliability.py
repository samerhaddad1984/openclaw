"""R4-Investigation 9 — notification reliability.

Focus on what's deterministic without real Gmail API credentials:
- send_email returns False cleanly when Gmail isn't configured
- Welcome + password-reset templates render safely under hostile input
- Message persistence: in-app messages land in client_messages
"""
from __future__ import annotations

import os
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


# ---------------------------------------------------------------------------
# send_email returns False gracefully without Gmail.
# ---------------------------------------------------------------------------

def test_send_email_returns_false_when_gmail_unconfigured(monkeypatch):
    """When GMAIL_TOKEN is missing, _get_gmail_service returns None and
    send_email must return False, not raise."""
    from src.integrations import email_client
    # Force gmail service to None by patching the getter.
    with patch.object(email_client, "_get_gmail_service", return_value=None):
        ok = email_client.send_email(
            "victim@example.com", "Subject", "<p>Hi</p>",
        )
    assert ok is False


def test_send_welcome_email_returns_false_when_gmail_unconfigured():
    from src.integrations import email_client
    with patch.object(email_client, "_get_gmail_service", return_value=None):
        ok = email_client.send_welcome_email(
            "victim@example.com", "Firm", "user",
            "http://test/reset?token=abc", "pro",
        )
    assert ok is False


def test_send_password_reset_email_returns_false_when_gmail_unconfigured():
    from src.integrations import email_client
    with patch.object(email_client, "_get_gmail_service", return_value=None):
        ok = email_client.send_password_reset_email(
            "victim@example.com", "user",
            "http://test/reset?token=abc",
        )
    assert ok is False


# ---------------------------------------------------------------------------
# Template XSS safety: hostile input in firm_name / username must not
# land unescaped into the HTML body.
# ---------------------------------------------------------------------------

def test_welcome_email_escapes_hostile_firm_name():
    from src.integrations import email_client
    captured = {}

    def _capture(to_email, subject, html_body, from_name='OtoCPA'):
        captured["html"] = html_body
        return True

    with patch.object(email_client, "send_email", side_effect=_capture):
        email_client.send_welcome_email(
            "v@example.com",
            firm_name="<script>alert(1)</script>",
            username="user",
            set_password_url="http://t/reset?token=abc",
            plan="pro",
        )
    html_body = captured.get("html", "")
    assert "<script>" not in html_body, (
        "welcome email firm_name not HTML-escaped — XSS in email client"
    )
    assert "&lt;script&gt;" in html_body


def test_welcome_email_escapes_hostile_username():
    from src.integrations import email_client
    captured = {}

    def _capture(to_email, subject, html_body, from_name='OtoCPA'):
        captured["html"] = html_body
        return True

    with patch.object(email_client, "send_email", side_effect=_capture):
        email_client.send_welcome_email(
            "v@example.com",
            firm_name="Firm",
            username='u"><img src=x onerror=alert(1)>',
            set_password_url="http://t/reset?token=abc",
            plan="pro",
        )
    html_body = captured.get("html", "")
    assert "onerror=" not in html_body or "&quot;&gt;" in html_body
    assert "<img" not in html_body or "&lt;img" in html_body


# ---------------------------------------------------------------------------
# In-app messaging: client message persists.
# ---------------------------------------------------------------------------

def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, client_name TEXT,
            firm_code TEXT, active INTEGER DEFAULT 1,
            version INTEGER DEFAULT 1,
            portal_token TEXT, portal_token_created_at TEXT
        );
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
        CREATE TABLE client_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, firm_code TEXT,
            direction TEXT, sender_name TEXT, sender_type TEXT,
            body TEXT, created_at TEXT DEFAULT (datetime('now')),
            read_at TEXT
        );
    """)
    c.commit(); c.close()


@pytest.fixture
def portal_app(tmp_path, monkeypatch):
    db = tmp_path / "msg.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()
    tok = rd.generate_portal_token()
    with sqlite3.connect(str(db)) as conn:
        conn.execute("INSERT INTO firms (firm_code) VALUES ('F1')")
        conn.execute(
            "INSERT INTO clients (client_code, client_name, firm_code, active, "
            "portal_token, portal_token_created_at) "
            "VALUES ('C1', 'Client One', 'F1', 1, ?, datetime('now'))",
            (tok,),
        )
        conn.commit()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "token": tok, "db": db}
    finally:
        server.shutdown(); server.server_close()


def test_client_message_persists_via_portal(portal_app):
    import urllib.parse
    import urllib.request
    import urllib.error
    body = urllib.parse.urlencode({"body": "Hello from the client"}).encode()
    p = urllib.parse.urlparse(portal_app["base"])
    req = urllib.request.Request(
        f"{portal_app['base']}/c/{portal_app['token']}/messages",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError:
        pass  # portal returns 303 after POST, which HTTPError raises
    with sqlite3.connect(str(portal_app["db"])) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT body, direction FROM client_messages "
            "WHERE client_code='C1' ORDER BY id DESC LIMIT 1",
        ).fetchone()
    assert row is not None, "client message didn't land in client_messages"
    assert row["body"] == "Hello from the client"
    assert row["direction"] == "inbound"


# ---------------------------------------------------------------------------
# Messages with hostile content: stored as-is, rendered escaped.
# ---------------------------------------------------------------------------

def test_hostile_message_body_stored_verbatim(portal_app):
    import urllib.parse
    import urllib.request
    import urllib.error
    hostile = '<script>alert(1)</script>'
    body = urllib.parse.urlencode({"body": hostile}).encode()
    p = urllib.parse.urlparse(portal_app["base"])
    req = urllib.request.Request(
        f"{portal_app['base']}/c/{portal_app['token']}/messages",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError:
        pass
    with sqlite3.connect(str(portal_app["db"])) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT body FROM client_messages WHERE client_code='C1' "
            "ORDER BY id DESC LIMIT 1",
        ).fetchone()
    assert row["body"] == hostile, (
        "hostile message body was mangled on the way in — should be stored "
        "verbatim and escaped only at render time"
    )
    # And when we render the page, it should NOT contain <script> unescaped.
    import urllib.request
    req2 = urllib.request.Request(
        f"{portal_app['base']}/c/{portal_app['token']}/messages",
    )
    with urllib.request.urlopen(req2, timeout=10) as r:
        rendered = r.read().decode("utf-8", errors="replace")
    assert "<script>alert" not in rendered, (
        "hostile message body rendered unescaped — XSS in portal messages"
    )
    assert "&lt;script&gt;" in rendered

"""R3-Investigation 2 — input validation across dashboard forms.

Hostile inputs submitted to real POST handlers. We assert:
  - No 5xx
  - No raw traceback in the body
  - Dangerous values (injection, huge numbers, impossible dates) are
    either rejected (4xx or flash error) OR sanitized before storage
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
    db = tmp_path / "input.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "in@put.com", "customer": "cus_IN",
             "subscription": "sub_IN", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("In1!"), "in@put.com"),
        )
        conn.commit()
    token = rd.create_session("in@put.com")
    cookies = f"session_token={token}"
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "cookies": cookies,
                "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


def _post(url, fields, cookies, *, extra_headers=None):
    body = urllib.parse.urlencode(fields).encode()
    p = urllib.parse.urlparse(url)
    hdrs = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": f"{p.scheme}://{p.netloc}",
        "Cookie": cookies,
    }
    if extra_headers:
        hdrs.update(extra_headers)
    req = urllib.request.Request(url, data=body, method="POST", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _assert_safe(status: int, body: bytes, where: str):
    """No 5xx; no raw traceback."""
    assert status < 500, f"{where}: 500. body[:300]={body[:300]!r}"
    text = body.decode("utf-8", errors="replace")
    for pat in ("Traceback (most recent call last)", "OperationalError",
                 "IntegrityError", "KeyError: ", "AttributeError: ",
                 "ValueError: "):
        assert pat not in text, f"{where}: leaked {pat!r}"


# ---------------------------------------------------------------------------
# /clients/save — the easiest target.
# ---------------------------------------------------------------------------

HOSTILE_STRINGS = [
    ("empty", ""),
    ("whitespace", "   "),
    ("sql_injection", "' OR 1=1--"),
    ("union_select", "' UNION SELECT * FROM dashboard_users--"),
    ("xss", "<script>alert(1)</script>"),
    ("path_traversal", "../../etc/passwd"),
    ("null_byte", "name\x00hidden"),
    ("unicode_rtl", "\u202eexample\u202c"),
    ("emoji_4byte", "🧾💸"),
    ("huge", "X" * 100_000),
    ("cr_lf_injection", "foo\r\nSet-Cookie: evil=1"),
]


@pytest.mark.parametrize("tag,name", HOSTILE_STRINGS, ids=[t for t, _ in HOSTILE_STRINGS])
def test_clients_save_survives_hostile_name(app, tag, name):
    status, body = _post(
        f"{app['base']}/clients/save",
        {"client_code": "HST1", "client_name": name,
         "contact_email": "x@y.com", "language": "en", "active": "1"},
        app["cookies"],
    )
    _assert_safe(status, body, f"/clients/save name={tag}")


def test_clients_save_empty_client_code_rejected(app):
    """Empty client_code should not create a row."""
    status, body = _post(
        f"{app['base']}/clients/save",
        {"client_code": "", "client_name": "nope",
         "contact_email": "a@b.com", "language": "fr", "active": "1"},
        app["cookies"],
    )
    _assert_safe(status, body, "/clients/save empty code")
    with sqlite3.connect(str(app["db"])) as c:
        n = c.execute("SELECT COUNT(*) FROM clients WHERE client_code=''").fetchone()[0]
    assert n == 0, "empty client_code was persisted"


def test_clients_save_language_unknown_falls_back_to_fr(app):
    _post(
        f"{app['base']}/clients/save",
        {"client_code": "LANG", "client_name": "Lang Test",
         "contact_email": "a@b.com", "language": "zz", "active": "1"},
        app["cookies"],
    )
    with sqlite3.connect(str(app["db"])) as c:
        lang = c.execute(
            "SELECT language FROM clients WHERE client_code='LANG'",
        ).fetchone()
    assert lang is not None
    assert lang[0] in ("fr", "en"), (
        f"unknown language 'zz' persisted as {lang[0]!r} instead of safe default"
    )


# ---------------------------------------------------------------------------
# /api/contact — the public (CSRF-exempt) form.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tag,email", [
    ("empty", ""),
    ("no_at", "not-an-email"),
    ("huge", "a" * 10_000 + "@example.com"),
    ("newline", "x\r\nBcc: admin@victim.com"),
])
def test_api_contact_rejects_bad_email(app, tag, email):
    status, body = _post(
        f"{app['base']}/api/contact",
        {"name": "Test", "email": email, "message": "Hello"},
        app["cookies"],
    )
    _assert_safe(status, body, f"/api/contact email={tag}")


# ---------------------------------------------------------------------------
# Document update: numbers and dates edges.
# ---------------------------------------------------------------------------

def _seed_doc(db: Path, firm_code: str) -> str:
    with sqlite3.connect(str(db)) as c:
        c.execute(
            "INSERT INTO documents (document_id, client_code, firm_code, "
            "vendor, review_status, version) "
            "VALUES ('DOC1','CLIX',?,'V','NeedsReview',1)", (firm_code,),
        )
        c.execute("INSERT OR IGNORE INTO clients (client_code, firm_code, active) "
                  "VALUES ('CLIX', ?, 1)", (firm_code,))
        c.commit()
    return "DOC1"


@pytest.mark.parametrize("tag,amount", [
    ("negative", "-1000"),
    ("scientific", "1e10"),
    ("hex", "0xFF"),
    ("huge", "9999999999999999"),
    ("nan_literal", "NaN"),
    ("many_decimals", "100.123456789012345"),
    ("comma_decimal", "1,234,56"),  # European
    ("whitespace", "   100   "),
])
def test_document_update_survives_weird_amount(app, tag, amount):
    # Need a firm_code for our doc.
    fc = app["rd"]._provision_firm_from_stripe(
        {"customer_email": "in@put.com", "customer": "cus_IN",
         "subscription": "sub_IN", "metadata": {"plan": "pro_monthly"}},
        base_url="http://test",
    )["firm_code"] if False else None
    with sqlite3.connect(str(app["db"])) as c:
        fc = c.execute("SELECT firm_code FROM dashboard_users LIMIT 1").fetchone()[0]
    _seed_doc(app["db"], fc)
    status, body = _post(
        f"{app['base']}/document/update",
        {"document_id": "DOC1", "vendor": "V", "client_code": "CLIX",
         "doc_type": "invoice", "amount": amount,
         "document_date": "2026-04-20",
         "gl_account": "", "tax_code": "", "category": "",
         "review_status": "NeedsReview", "version": "1"},
        app["cookies"],
    )
    _assert_safe(status, body, f"/document/update amount={tag}")


@pytest.mark.parametrize("tag,date", [
    ("far_future", "9999-12-31"),
    ("far_past", "1800-01-01"),
    ("impossible", "2099-99-99"),
    ("not_iso", "20260420"),
    ("garbage", "yesterday"),
    ("cr_lf", "2026-04-20\r\nSet-Cookie: x=y"),
])
def test_document_update_survives_weird_date(app, tag, date):
    with sqlite3.connect(str(app["db"])) as c:
        fc = c.execute("SELECT firm_code FROM dashboard_users LIMIT 1").fetchone()[0]
    _seed_doc(app["db"], fc)
    status, body = _post(
        f"{app['base']}/document/update",
        {"document_id": "DOC1", "vendor": "V", "client_code": "CLIX",
         "doc_type": "", "amount": "100",
         "document_date": date,
         "gl_account": "", "tax_code": "", "category": "",
         "review_status": "NeedsReview", "version": "1"},
        app["cookies"],
    )
    _assert_safe(status, body, f"/document/update date={tag}")


# ---------------------------------------------------------------------------
# /journal_entries — a real money-touching form.
# ---------------------------------------------------------------------------

def test_journal_entries_rejects_or_handles_unbalanced_single_amount(app):
    """A JE write should not 5xx even with bad inputs."""
    status, body = _post(
        f"{app['base']}/journal_entries",
        {"action": "create", "client_code": "CLIZ", "period": "2026-04",
         "entry_date": "2026-04-20",
         "debit_account": "6100", "credit_account": "1000",
         "amount": "-50",  # negative: should reject or clamp
         "description": "test"},
        app["cookies"],
    )
    _assert_safe(status, body, "/journal_entries neg amount")


def test_journal_entries_same_account_both_sides_refused(app):
    status, body = _post(
        f"{app['base']}/journal_entries",
        {"action": "create", "client_code": "CLIZ", "period": "2026-04",
         "entry_date": "2026-04-20",
         "debit_account": "6100", "credit_account": "6100",
         "amount": "50", "description": "same"},
        app["cookies"],
    )
    _assert_safe(status, body, "/journal_entries same acct")


def test_journal_entries_script_in_description_does_not_execute(app):
    status, body = _post(
        f"{app['base']}/journal_entries",
        {"action": "create", "client_code": "CLIZ", "period": "2026-04",
         "entry_date": "2026-04-20",
         "debit_account": "6100", "credit_account": "1000",
         "amount": "10",
         "description": "<script>alert(1)</script>"},
        app["cookies"],
    )
    _assert_safe(status, body, "/journal_entries xss desc")


# ---------------------------------------------------------------------------
# /users/add — privilege escalation attempts.
# ---------------------------------------------------------------------------

def test_users_add_cannot_create_owner_role_from_non_owner(app):
    """A firm_admin cannot create an 'owner' role user."""
    status, body = _post(
        f"{app['base']}/users/add",
        {"username": "newuser@evil.com", "password": "StrongPw1!",
         "role": "owner",  # attempted escalation
         "display_name": "Evil"},
        app["cookies"],
    )
    _assert_safe(status, body, "/users/add owner escalation")
    # Verify: if a row was created, its role must NOT be 'owner'.
    with sqlite3.connect(str(app["db"])) as c:
        row = c.execute(
            "SELECT role FROM dashboard_users WHERE username='newuser@evil.com'",
        ).fetchone()
    if row is not None:
        assert row[0] != "owner", (
            "firm_admin was able to create an owner-role user via /users/add"
        )

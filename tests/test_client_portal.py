"""
tests/test_client_portal.py
============================
Sprint 4: client portal via QR token.

Covers:
  - DB migration + backfill of portal_token
  - resolve_portal_token / rotate_portal_token / generate_portal_token
  - Firm isolation on rotate
  - Rate limiting (per token, per IP)
  - Portal route rendering (invalid/valid token paths)
  - Upload via portal -> document row
  - Documents view scoped to the client
  - Bank link uses client_code as Plaid client_user_id
  - Messages: inbound/outbound directions, CPA read marking
  - Access log entries
  - QR generator portal URL builder
  - Legacy /upload still works + shows deprecation banner
  - Portal invalid page + referrer-policy header + min token length
"""
from __future__ import annotations

import sqlite3
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode
import urllib.request
import urllib.error
import time

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Fixture: isolated SQLite DB + monkey-patched module.
# ---------------------------------------------------------------------------

@pytest.fixture
def portal_env(tmp_path, monkeypatch):
    """Boot review_dashboard with a fresh DB under tmp_path."""
    db_path = tmp_path / "otocpa_agent.db"
    # Pre-create minimal clients table (bootstrap_schema expects it).
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS clients (
            client_code TEXT PRIMARY KEY,
            client_name TEXT,
            contact_email TEXT,
            language TEXT DEFAULT 'fr',
            active INTEGER DEFAULT 1,
            whatsapp_number TEXT,
            firm_code TEXT DEFAULT 'OWNER'
        );
        CREATE TABLE IF NOT EXISTS client_whatsapp_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT,
            whatsapp_number TEXT,
            contact_name TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT DEFAULT 'New',
            confidence REAL, raw_result TEXT,
            created_at TEXT, updated_at TEXT,
            assigned_to TEXT, manual_hold_reason TEXT,
            submitted_by TEXT, client_note TEXT
        );
        CREATE TABLE IF NOT EXISTS bank_connections (
            id TEXT PRIMARY KEY,
            client_code TEXT,
            plaid_access_token TEXT,
            plaid_item_id TEXT,
            institution_name TEXT,
            account_name TEXT,
            account_type TEXT,
            last_sync TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            firm_code TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            id TEXT PRIMARY KEY, document_id TEXT
        );
    """)
    conn.executemany(
        "INSERT INTO clients (client_code, client_name, firm_code) VALUES (?,?,?)",
        [
            ("ACME", "Acme Corp", "OWNER"),
            ("BETA", "Beta Ltd",  "OWNER"),
            ("FIRM1_C1", "Firm1 Client 1", "FIRM1"),
            ("INACTIVE", "Inactive Co", "OWNER"),
        ],
    )
    conn.execute("UPDATE clients SET active=0 WHERE client_code='INACTIVE'")
    conn.commit()
    conn.close()

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db_path)
    rd.bootstrap_schema()
    # Reset any in-memory rate-limit state between tests.
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()
    yield rd


def _get_token(rd, client_code: str) -> str:
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT portal_token FROM clients WHERE client_code=?",
            (client_code,),
        ).fetchone()
    return row["portal_token"] if row else ""


# ---------------------------------------------------------------------------
# Migration / backfill
# ---------------------------------------------------------------------------

def test_portal_token_generated_for_all_clients(portal_env):
    rd = portal_env
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT client_code, portal_token FROM clients"
        ).fetchall()
    assert len(rows) == 4
    for r in rows:
        assert r["portal_token"]
        assert len(r["portal_token"]) >= 30


def test_portal_token_resolves_to_correct_client(portal_env):
    rd = portal_env
    token = _get_token(rd, "ACME")
    row = rd.resolve_portal_token(token)
    assert row is not None
    assert row["client_code"] == "ACME"


def test_portal_token_invalid_returns_invalid_page(portal_env):
    rd = portal_env
    assert rd.resolve_portal_token("") is None
    assert rd.resolve_portal_token("short") is None
    assert rd.resolve_portal_token("a" * 40) is None
    # Ensure the invalid page renderer works and is bilingual.
    html_str = rd.render_portal_invalid_page()
    assert "Invalid link" in html_str
    assert "invalide" in html_str.lower() or "Lien invalide" in html_str


def test_portal_token_of_inactive_client_rejected(portal_env):
    rd = portal_env
    token = _get_token(rd, "INACTIVE")
    assert token  # backfilled even for inactive
    assert rd.resolve_portal_token(token) is None


def test_portal_token_minimum_length_enforced(portal_env):
    rd = portal_env
    # Tokens shorter than 30 chars are rejected before DB lookup.
    assert rd.resolve_portal_token("x" * 29) is None
    assert rd.resolve_portal_token("x" * 30) is None  # 30 chars but not in DB


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------

def test_rotate_portal_token_invalidates_old(portal_env):
    rd = portal_env
    token_old = _get_token(rd, "ACME")
    ctx = {"role": "owner", "firm_code": "OWNER"}
    token_new = rd.rotate_portal_token("ACME", ctx)
    assert token_new
    assert token_new != token_old
    assert rd.resolve_portal_token(token_old) is None
    assert rd.resolve_portal_token(token_new) is not None


def test_rotate_portal_token_requires_firm_isolation(portal_env):
    rd = portal_env
    firm2_ctx = {"role": "firm_admin", "firm_code": "FIRM2"}
    # FIRM2 tries to rotate FIRM1_C1 token — must fail.
    result = rd.rotate_portal_token("FIRM1_C1", firm2_ctx)
    assert result is None
    # FIRM1 admin can rotate their own client.
    firm1_ctx = {"role": "firm_admin", "firm_code": "FIRM1"}
    assert rd.rotate_portal_token("FIRM1_C1", firm1_ctx) is not None


def test_rotate_increments_counter(portal_env):
    rd = portal_env
    ctx = {"role": "owner", "firm_code": "OWNER"}
    for _ in range(3):
        rd.rotate_portal_token("ACME", ctx)
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT portal_token_rotated_count FROM clients WHERE client_code='ACME'"
        ).fetchone()
    assert row["portal_token_rotated_count"] == 3


# ---------------------------------------------------------------------------
# Rate limits
# ---------------------------------------------------------------------------

def test_portal_rate_limit_per_token(portal_env):
    rd = portal_env
    token = _get_token(rd, "ACME")
    for _ in range(100):
        assert rd._portal_rate_allowed(token, "1.2.3.4")
    # 101st call is rejected.
    assert rd._portal_rate_allowed(token, "1.2.3.4") is False


def test_portal_rate_limit_per_ip(portal_env):
    rd = portal_env
    # No token => per-IP bucket limited to 20.
    for _ in range(20):
        assert rd._portal_rate_allowed("", "9.9.9.9")
    assert rd._portal_rate_allowed("", "9.9.9.9") is False


# ---------------------------------------------------------------------------
# HTTP integration — spin up a server against the real handler.
# ---------------------------------------------------------------------------

@pytest.fixture
def http_server(portal_env):
    rd = portal_env
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


def _get(url, *, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _post(url, data, *, headers=None, method="POST"):
    if isinstance(data, dict):
        body = urlencode(data).encode()
        default_ct = "application/x-www-form-urlencoded"
    elif isinstance(data, (bytes, bytearray)):
        body = bytes(data)
        default_ct = "application/octet-stream"
    else:
        body = str(data).encode()
        default_ct = "text/plain"
    hdrs = {"Content-Type": default_ct, **(headers or {})}
    req = urllib.request.Request(url, data=body, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def test_portal_cookie_set_on_first_scan(portal_env, http_server):
    rd = portal_env
    token = _get_token(rd, "ACME")
    status, headers, body = _get(f"{http_server}/c/{token}")
    assert status == 200
    # Set-Cookie header carries the portal token.
    sc = headers.get("Set-Cookie", "")
    assert "otocpa_portal_token=" in sc
    assert "HttpOnly" in sc
    assert "SameSite=Lax" in sc
    assert "Acme Corp" in body.decode("utf-8")


def test_portal_referrer_policy_header_set(portal_env, http_server):
    rd = portal_env
    token = _get_token(rd, "ACME")
    status, headers, _ = _get(f"{http_server}/c/{token}")
    assert status == 200
    # Header is set on portal responses to prevent token leakage.
    assert headers.get("Referrer-Policy", "").lower() == "no-referrer"


def test_portal_invalid_token_returns_404(portal_env, http_server):
    status, _headers, body = _get(f"{http_server}/c/this_is_a_bogus_token_but_long_enough_xx")
    assert status == 404
    # Bilingual invalid-link body.
    text = body.decode("utf-8")
    assert "Invalid" in text or "invalide" in text.lower()


def test_portal_access_logged(portal_env, http_server):
    rd = portal_env
    token = _get_token(rd, "ACME")
    _get(f"{http_server}/c/{token}")
    _get(f"{http_server}/c/{token}/documents")
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT action, portal_token_prefix, client_code FROM client_portal_access "
            "WHERE client_code='ACME' ORDER BY id"
        ).fetchall()
    actions = [r["action"] for r in rows]
    assert "view_upload" in actions
    assert "view_documents" in actions
    assert rows[0]["portal_token_prefix"] == token[:8]


# ---------------------------------------------------------------------------
# Upload + documents scoping
# ---------------------------------------------------------------------------

def test_portal_upload_creates_document(portal_env, http_server, monkeypatch):
    rd = portal_env
    token = _get_token(rd, "ACME")

    # Stub the OCR pipeline so the test stays offline and deterministic.
    import src.engines.ocr_engine as ocr
    import secrets as _secrets
    def _fake_process_file(fbytes, fname, *, client_code, **kwargs):
        doc_id = "doc_" + _secrets.token_hex(6)
        with sqlite3.connect(rd.DB_PATH) as conn:
            conn.execute(
                "INSERT INTO documents (document_id, file_name, client_code, review_status, created_at) "
                "VALUES (?,?,?,?,datetime('now'))",
                (doc_id, fname, client_code, "New"),
            )
            conn.commit()
        return {"ok": True, "document_id": doc_id, "file_name": fname}
    monkeypatch.setattr(ocr, "process_file", _fake_process_file)

    # Multipart upload.
    boundary = "----testboundary123"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="files"; filename="receipt.pdf"\r\n'
        "Content-Type: application/pdf\r\n\r\n"
        "%PDF-fake\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    status, _headers, _b = _post(
        f"{http_server}/c/{token}/upload",
        body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert status in (200, 303)
    with sqlite3.connect(rd.DB_PATH) as conn:
        rows = conn.execute(
            "SELECT document_id, client_code, file_name FROM documents"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "ACME"
    assert rows[0][2] == "receipt.pdf"


def test_portal_documents_only_shows_own_client(portal_env, http_server):
    rd = portal_env
    # Seed a document for BETA; ensure ACME portal does not render it.
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, file_name TEXT, client_code TEXT,
            review_status TEXT, vendor TEXT, amount REAL,
            manual_hold_reason TEXT, created_at TEXT
        )""")
        conn.execute(
            "INSERT INTO documents (document_id, file_name, client_code, review_status, "
            "vendor, amount, created_at) VALUES (?,?,?,?,?,?,datetime('now'))",
            ("doc_beta", "beta_secret.pdf", "BETA", "Ready", "Vendeur B", 99.0),
        )
        conn.commit()
    acme_token = _get_token(rd, "ACME")
    status, _headers, body = _get(f"{http_server}/c/{acme_token}/documents")
    assert status == 200
    assert b"beta_secret.pdf" not in body


# ---------------------------------------------------------------------------
# Bank: link-token uses client_code as Plaid client_user_id
# ---------------------------------------------------------------------------

def test_portal_bank_link_uses_client_code_as_plaid_user(portal_env, http_server, monkeypatch):
    rd = portal_env
    token = _get_token(rd, "ACME")
    # Stub Plaid credentials + create_link_token.
    monkeypatch.setenv("PLAID_CLIENT_ID", "fake")
    monkeypatch.setenv("PLAID_SECRET", "fake")
    seen = {}
    import src.integrations.plaid_client as pc
    def _fake(client_code, user_id):
        seen["client_code"] = client_code
        seen["user_id"] = user_id
        return "link-sandbox-faketoken"
    monkeypatch.setattr(pc, "create_link_token", _fake)
    status, _h, body = _post(f"{http_server}/c/{token}/bank/link-token", data=b"")
    assert status == 200
    # The identity bug fix: Plaid's user_id must be the client_code, not a CPA username.
    assert seen["client_code"] == "ACME"
    assert seen["user_id"] == "ACME"


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

def test_portal_messages_inbound_direction(portal_env, http_server):
    rd = portal_env
    token = _get_token(rd, "ACME")
    _post(f"{http_server}/c/{token}/messages", {"body": "Hello from client"})
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT direction, sender_type, body FROM client_messages "
            "WHERE client_code='ACME' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row["direction"] == "inbound"
    assert row["sender_type"] == "client"
    assert row["body"] == "Hello from client"


def test_cpa_messages_outbound_direction(portal_env):
    rd = portal_env
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.execute(
            "INSERT INTO client_messages (client_code, firm_code, direction, sender_name, sender_type, body) "
            "VALUES (?,?,?,?,?,?)",
            ("ACME", "OWNER", "outbound", "Sam CPA", "cpa", "Hi, welcome."),
        )
        conn.commit()
        row = conn.execute(
            "SELECT direction, sender_type FROM client_messages WHERE client_code='ACME'"
        ).fetchone()
    assert row[0] == "outbound"
    assert row[1] == "cpa"


def test_portal_messages_marks_cpa_messages_read(portal_env, http_server):
    rd = portal_env
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.execute(
            "INSERT INTO client_messages (client_code, firm_code, direction, sender_name, sender_type, body) "
            "VALUES (?,?,?,?,?,?)",
            ("ACME", "OWNER", "outbound", "Sam", "cpa", "Please send March receipts."),
        )
        conn.commit()
    token = _get_token(rd, "ACME")
    status, _h, _b = _get(f"{http_server}/c/{token}/messages")
    assert status == 200
    with sqlite3.connect(rd.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT read_at FROM client_messages WHERE client_code='ACME' AND direction='outbound'"
        ).fetchone()
    assert row["read_at"] is not None


# ---------------------------------------------------------------------------
# QR generator
# ---------------------------------------------------------------------------

def test_qr_generator_builds_portal_url():
    from src.integrations.qr_generator import build_portal_url
    url = build_portal_url("https://portal.otocpa.com/", "tok123")
    assert url == "https://portal.otocpa.com/c/tok123"


def test_qr_generator_pdf_uses_portal_token():
    from src.integrations.qr_generator import generate_all_qr_pdf
    clients = [
        {"client_code": "ACME", "client_name": "Acme",
         "portal_token": "TESTTOKEN-abc123"},
    ]
    pdf = generate_all_qr_pdf(clients, "https://portal.example.com")
    assert pdf[:4] == b"%PDF"
    assert b"/c/TESTTOKEN-abc123" in pdf


# ---------------------------------------------------------------------------
# Legacy /upload route
# ---------------------------------------------------------------------------

def test_legacy_upload_route_still_works(portal_env, http_server):
    status, _h, body = _get(f"{http_server}/upload?client_code=ACME")
    assert status == 200
    # The legacy page renders client name or fallback message.
    assert b"Acme Corp" in body or b"Upload" in body


def test_legacy_upload_shows_deprecation_banner(portal_env, http_server):
    _status, _h, body = _get(f"{http_server}/upload?client_code=ACME")
    text = body.decode("utf-8")
    assert "deprecated" in text.lower() or "d\u00e9pr\u00e9ci\u00e9" in text.lower()


# ---------------------------------------------------------------------------
# Generation helpers
# ---------------------------------------------------------------------------

def test_generate_portal_token_returns_long_random_string(portal_env):
    rd = portal_env
    a = rd.generate_portal_token()
    b = rd.generate_portal_token()
    assert a != b
    assert len(a) >= 30 and len(b) >= 30

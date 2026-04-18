"""
tests/test_e2e_cpa_journey.py
==============================
End-to-end CPA journey: signup -> onboarding -> client portal -> QBO post.

Scenario: Jean Tremblay signs up to run "Tremblay CPA", onboards two clients,
a client uploads a receipt via QR portal, Jean reviews and posts to QBO.

All external services (Stripe, Plaid, Intuit, email, OCR) are mocked. This
file never touches the network or the user's DB.

The flask-ish app is scripts/review_dashboard.py (BaseHTTPRequestHandler). We
use ThreadingHTTPServer to exercise real routes, and call internal helpers
directly where HTTP would just be noise (e.g. _provision_firm_from_stripe).
"""
from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Shared schema bootstrap
# ---------------------------------------------------------------------------

def _prebootstrap_tables(db: Path) -> None:
    """Pre-create tables that bootstrap_schema() ALTER-patches but does not
    CREATE itself (notably documents + clients).

    NOTE: bootstrap_schema() creates ``firms`` WITHOUT the Stripe billing
    columns (stripe_customer_id, stripe_subscription_id, subscription_status,
    billing_email). _provision_firm_from_stripe needs them, so we pre-create
    a ``firms`` table that already has them. This is a product gap worth
    fixing: bootstrap_schema should migrate existing installs.
    """
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS firms (
            firm_code TEXT PRIMARY KEY,
            firm_name TEXT NOT NULL,
            contact_email TEXT,
            contact_phone TEXT,
            billing_email TEXT,
            language TEXT DEFAULT 'fr',
            plan TEXT DEFAULT 'basic',
            active INTEGER DEFAULT 1,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            subscription_status TEXT,
            trial_ends_at TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS clients (
            client_code TEXT PRIMARY KEY,
            client_name TEXT,
            contact_email TEXT,
            language TEXT DEFAULT 'fr',
            active INTEGER DEFAULT 1,
            whatsapp_number TEXT,
            firm_code TEXT DEFAULT 'OWNER'
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
            submitted_by TEXT, client_note TEXT,
            fraud_flags TEXT, substance_flags TEXT
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
            posting_id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            target_system TEXT NOT NULL,
            entry_kind TEXT NOT NULL,
            posting_status TEXT NOT NULL,
            approval_state TEXT NOT NULL,
            reviewer TEXT,
            external_id TEXT,
            payload_json TEXT NOT NULL,
            error_text TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contact_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT,
            contacted INTEGER DEFAULT 0,
            archived INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    """Build a clean review_dashboard pointed at a new SQLite DB + secret."""
    db_path = tmp_path / "e2e.db"
    secret_file = tmp_path / "password_link_secret"
    secret_file.write_text("e" * 48)
    _prebootstrap_tables(db_path)

    # Reload qbo_oauth first so its DB_PATH monkeypatch sticks.
    import src.agents.tools.qbo_oauth as qbo_oauth
    importlib.reload(qbo_oauth)
    monkeypatch.setattr(qbo_oauth, "DB_PATH", db_path)
    monkeypatch.setattr(qbo_oauth, "CONFIG_PATH", tmp_path / "does_not_exist.json")

    # Reload the adapter so it picks up the reloaded oauth module.
    import src.agents.tools.qbo_online_adapter as adapter
    importlib.reload(adapter)
    monkeypatch.setattr(adapter, "DB_PATH", db_path)
    monkeypatch.setattr(adapter, "_oauth_get_qbo_tokens", qbo_oauth.get_qbo_tokens)
    monkeypatch.setattr(adapter, "_oauth_refresh_access_token", qbo_oauth.refresh_access_token)

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db_path)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret_file))
    rd.bootstrap_schema()
    # Reset rate-limit counters for each test.
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()
    return {"rd": rd, "db": db_path, "qbo_oauth": qbo_oauth, "adapter": adapter}


@pytest.fixture
def http_server(fresh_db):
    """Spawn a ThreadingHTTPServer bound to the real ReviewDashboardHandler."""
    rd = fresh_db["rd"]
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_a, **_kw):
        return None  # urlopen will surface the redirect as a 3xx HTTPError


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _request(url, *, data=None, headers=None, method=None, follow_redirects=False):
    hdrs = dict(headers or {})
    if data is not None and not isinstance(data, (bytes, bytearray)):
        raise TypeError("data must be bytes")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    opener = urllib.request.urlopen if follow_redirects else _NO_REDIRECT_OPENER.open
    try:
        with opener(req, timeout=5) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        # 3xx surfaces as HTTPError when we disable redirects; treat as normal.
        return e.code, dict(e.headers), e.read()


def _form_post(url, fields, *, headers=None, method="POST"):
    from urllib.parse import urlencode, urlparse
    body = urlencode(fields).encode()
    # Set a same-origin Origin header so the dashboard's CSRF check passes
    # (tests simulate a browser that would send this automatically).
    parsed = urlparse(url)
    same_origin = f"{parsed.scheme}://{parsed.netloc}"
    hdrs = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": same_origin,
        **(headers or {}),
    }
    return _request(url, data=body, headers=hdrs, method=method)


def _get(url, *, headers=None):
    return _request(url, method="GET", headers=headers)


def _multipart_post(url, filename, content, *, headers=None):
    boundary = "----e2eboundary42"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="files"; filename="{filename}"\r\n'
        "Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    hdrs = {"Content-Type": f"multipart/form-data; boundary={boundary}", **(headers or {})}
    return _request(url, data=body, headers=hdrs, method="POST")


# ---------------------------------------------------------------------------
# Fixtures that build on fresh_db
# ---------------------------------------------------------------------------

def _fake_stripe_session(email, customer_id, plan="pro_monthly", firm_code=None):
    meta = {"plan": plan}
    if firm_code:
        meta["firm_code"] = firm_code
    return {
        "customer_email": email,
        "customer": customer_id,
        "subscription": f"sub_{customer_id}",
        "metadata": meta,
    }


@pytest.fixture
def provisioned_firm(fresh_db):
    """Jean signs up — Stripe checkout.session.completed fires
    _provision_firm_from_stripe, which creates the firm + firm_admin."""
    rd = fresh_db["rd"]
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        result = rd._provision_firm_from_stripe(
            _fake_stripe_session("jean@tremblaycpa.com", "cus_TREMBLAY"),
            base_url="http://test",
        )
    return {**fresh_db, "provision": result}


def _seed_login(rd, username, password, firm_code, role="firm_admin"):
    """Overwrite a user's password with a known strong one so we can call
    create_session() directly."""
    with sqlite3.connect(str(rd.DB_PATH)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?",
            (rd.hash_password(password), username),
        )
        conn.commit()


def _issue_session_cookie(rd, username):
    token = rd.create_session(username)
    return f"session_token={token}"


# ---------------------------------------------------------------------------
# PHASE 1 — CPA signup via Stripe webhook
# ---------------------------------------------------------------------------

class TestE2ECpaJourney:

    # ------- Phase 1 -------

    def test_01_stripe_webhook_creates_firm_and_user(self, fresh_db):
        rd = fresh_db["rd"]
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True) as sent:
            result = rd._provision_firm_from_stripe(
                _fake_stripe_session("jean@tremblaycpa.com", "cus_TREM",
                                     plan="pro_monthly"),
                base_url="http://test",
            )
        assert result["existing"] is False
        assert result["set_password_url"].startswith(
            "http://test/set-password?token=")
        # firm row created with status active
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            firm = conn.execute(
                "SELECT * FROM firms WHERE stripe_customer_id=?", ("cus_TREM",)
            ).fetchone()
            user = conn.execute(
                "SELECT * FROM dashboard_users WHERE username=?",
                ("jean@tremblaycpa.com",),
            ).fetchone()
        assert firm is not None
        assert firm["subscription_status"] == "active"
        assert firm["active"] == 1
        assert user is not None
        assert user["role"] == "firm_admin"
        assert user["must_reset_password"] == 1
        assert user["firm_code"] == firm["firm_code"]
        # The signed token actually resolves back to the username.
        tok = result["set_password_url"].split("token=", 1)[1]
        assert rd._verify_password_link(tok) == "jean@tremblaycpa.com"
        sent.assert_called_once()

    # ------- Phase 2 -------

    def test_02_set_password_completes_onboarding(self, provisioned_firm):
        rd = provisioned_firm["rd"]
        result = provisioned_firm["provision"]
        token = result["set_password_url"].split("token=", 1)[1]
        # Simulate /set-password POST: strong password, valid token.
        username = rd._verify_password_link(token)
        assert username == "jean@tremblaycpa.com"
        err = rd._validate_password_strength("Strong-Pass-9", "Strong-Pass-9")
        assert err == ""
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
                "WHERE username=?",
                (rd.hash_password("Strong-Pass-9"), username),
            )
            conn.commit()
            row = conn.execute(
                "SELECT password_hash, must_reset_password FROM dashboard_users "
                "WHERE username=?", (username,),
            ).fetchone()
        assert row[1] == 0
        assert rd.verify_password("Strong-Pass-9", row[0])
        # Session cookie issuance works.
        sess = rd.create_session(username)
        assert sess and len(sess) > 20

    def test_03_firm_admin_sees_only_own_firm(self, provisioned_firm, http_server):
        rd = provisioned_firm["rd"]
        username = provisioned_firm["provision"]["admin_username"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        _seed_login(rd, username, "Strong-Pass-9", firm_code)

        # Plant a second firm with a client to prove isolation.
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO firms (firm_code, firm_name, active, subscription_status) "
                "VALUES (?,?,1,'active')", ("CPA_OTHER", "Rival CPA"))
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)",
                ("OTHER_CLIENT", "Rival's Client", "CPA_OTHER"))
            conn.commit()

        cookie = _issue_session_cookie(rd, username)
        status, _h, body = _get(f"{http_server}/clients", headers={"Cookie": cookie})
        assert status == 200
        assert b"OTHER_CLIENT" not in body
        assert b"Rival" not in body

        # /firms and /leads are owner-only.
        status, _h, _b = _get(f"{http_server}/firms", headers={"Cookie": cookie})
        assert status == 403
        status, _h, hdrs_body = _get(f"{http_server}/leads", headers={"Cookie": cookie})
        # /leads redirects non-owners to / (303) — also non-access.
        assert status in (303, 302, 403)

    # ------- Phase 3 -------

    def test_04_firm_admin_can_create_client(self, provisioned_firm, http_server):
        rd = provisioned_firm["rd"]
        username = provisioned_firm["provision"]["admin_username"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        _seed_login(rd, username, "Strong-Pass-9", firm_code)
        cookie = _issue_session_cookie(rd, username)
        # POST new client.
        status, _h, _b = _form_post(
            f"{http_server}/clients/save",
            {"client_code": "ACME1", "client_name": "Acme One",
             "contact_email": "ops@acme1.com", "language": "en", "active": "1"},
            headers={"Cookie": cookie},
        )
        assert status in (200, 303)
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT firm_code, portal_token FROM clients WHERE client_code=?",
                ("ACME1",),
            ).fetchone()
        assert row is not None
        assert row["firm_code"] == firm_code
        # Regenerate portal tokens for any new clients (bootstrap runs on startup).
        rd.bootstrap_schema()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT portal_token FROM clients WHERE client_code=?",
                ("ACME1",),
            ).fetchone()
        assert row["portal_token"]
        assert len(row["portal_token"]) >= 30
        # QR URL builder points at the portal token.
        from src.integrations.qr_generator import build_portal_url
        assert build_portal_url("https://portal.example.com",
                                row["portal_token"]).endswith(f"/c/{row['portal_token']}")

    def test_05_portal_token_is_unique_and_long(self, provisioned_firm, http_server):
        rd = provisioned_firm["rd"]
        username = provisioned_firm["provision"]["admin_username"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        _seed_login(rd, username, "Strong-Pass-9", firm_code)
        cookie = _issue_session_cookie(rd, username)
        for code in ("C1", "C2"):
            _form_post(
                f"{http_server}/clients/save",
                {"client_code": code, "client_name": f"Client {code}",
                 "language": "fr", "active": "1"},
                headers={"Cookie": cookie},
            )
        rd.bootstrap_schema()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT client_code, portal_token FROM clients "
                "WHERE client_code IN ('C1','C2')"
            ).fetchall()
        tokens = {r["portal_token"] for r in rows}
        assert len(tokens) == 2
        for t in tokens:
            assert t and len(t) >= 32

    # ------- Phase 4 -------

    def test_06_client_scans_qr_accesses_portal(self, provisioned_firm, http_server):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        # Insert a client with a token.
        tok = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) VALUES (?,?,?,1,?,datetime('now'))",
                ("ACME", "Acme Corp", firm_code, tok))
            conn.commit()
        status, headers, body = _get(f"{http_server}/c/{tok}")
        assert status == 200
        sc = headers.get("Set-Cookie", "")
        assert "otocpa_portal_token=" in sc
        assert "HttpOnly" in sc
        assert "SameSite=Lax" in sc
        assert headers.get("Referrer-Policy", "").lower() == "no-referrer"
        assert b"Acme Corp" in body
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            logrow = conn.execute(
                "SELECT action, portal_token_prefix FROM client_portal_access "
                "WHERE client_code='ACME'"
            ).fetchone()
        assert logrow is not None
        assert logrow["action"] == "view_upload"
        assert logrow["portal_token_prefix"] == tok[:8]

    def test_07_client_uploads_receipt_via_portal(self, provisioned_firm,
                                                   http_server, monkeypatch):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        tok = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) VALUES (?,?,?,1,?,datetime('now'))",
                ("ACME", "Acme Corp", firm_code, tok))
            conn.commit()
        # Stub OCR pipeline — async flow pre-inserts a placeholder row and the
        # worker invokes process_file, which should UPDATE that same row.
        import src.engines.ocr_engine as ocr

        def _fake(fbytes, fname, *, document_id, client_code, **kwargs):
            with sqlite3.connect(str(rd.DB_PATH)) as conn:
                conn.execute(
                    "UPDATE documents SET review_status='New' WHERE document_id=?",
                    (document_id,))
                conn.commit()
            return {"ok": True, "document_id": document_id, "file_name": fname}
        monkeypatch.setattr(ocr, "process_file", _fake)

        status, _h, _b = _multipart_post(
            f"{http_server}/c/{tok}/upload", "receipt.pdf", b"%PDF-fake")
        assert status in (200, 303)

        # Wait for the background worker to finish.
        from src.engines import upload_queue as _uq
        _uq.get_upload_queue().wait_idle()

        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            rows = conn.execute(
                "SELECT client_code, review_status FROM documents"
            ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "ACME"
        assert rows[0][1] == "New"

    def test_08_client_sees_only_own_documents(self, provisioned_firm,
                                                http_server, monkeypatch):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        tok_a = rd.generate_portal_token()
        tok_b = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.executemany(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) "
                "VALUES (?,?,?,1,?,datetime('now'))",
                [("CA", "Client A", firm_code, tok_a),
                 ("CB", "Client B", firm_code, tok_b)])
            conn.execute(
                "INSERT INTO documents (document_id, file_name, client_code, "
                "review_status, created_at) VALUES (?,?,?,?,datetime('now'))",
                ("doc_ca", "a_secret.pdf", "CA", "Ready"))
            conn.commit()
        # Client A sees their doc.
        _s, _h, body_a = _get(f"{http_server}/c/{tok_a}/documents")
        assert b"a_secret.pdf" in body_a
        # Client B must NOT see A's doc.
        _s, _h, body_b = _get(f"{http_server}/c/{tok_b}/documents")
        assert b"a_secret.pdf" not in body_b

    # ------- Phase 5 -------

    def test_09_client_bank_link_uses_client_code(self, provisioned_firm,
                                                    http_server, monkeypatch):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        tok = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) VALUES (?,?,?,1,?,datetime('now'))",
                ("ACME", "Acme", firm_code, tok))
            conn.commit()
        monkeypatch.setenv("PLAID_CLIENT_ID", "fake")
        monkeypatch.setenv("PLAID_SECRET", "fake")
        seen = {}
        import src.integrations.plaid_client as pc

        def _fake(client_code, user_id):
            seen["client_code"] = client_code
            seen["user_id"] = user_id
            return "link-sandbox-fake"
        monkeypatch.setattr(pc, "create_link_token", _fake)
        status, _h, _b = _request(f"{http_server}/c/{tok}/bank/link-token",
                                  data=b"", method="POST",
                                  headers={"Content-Type": "application/octet-stream"})
        assert status == 200
        assert seen["client_code"] == "ACME"
        # Plaid's client_user_id MUST be the client_code, not the CPA username.
        assert seen["user_id"] == "ACME"

    def test_10_bank_connection_stored_per_client(self, provisioned_firm,
                                                   http_server, monkeypatch):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        tok_a = rd.generate_portal_token()
        tok_b = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.executemany(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) "
                "VALUES (?,?,?,1,?,datetime('now'))",
                [("BA1", "Bank A", firm_code, tok_a),
                 ("BA2", "Bank B", firm_code, tok_b)])
            conn.commit()
        import src.integrations.plaid_client as pc
        monkeypatch.setenv("PLAID_CLIENT_ID", "fake")
        monkeypatch.setenv("PLAID_SECRET", "fake")
        monkeypatch.setattr(pc, "exchange_public_token",
                            lambda pt: (f"access-{pt}", f"item-{pt}"))
        # Client A exchange.
        import json as _json
        payload_a = _json.dumps({
            "public_token": "public-A",
            "metadata": {
                "institution": {"name": "Bank A"},
                "accounts": [{"name": "Checking A", "subtype": "checking"}],
            },
        }).encode()
        s, _h, _b = _request(f"{http_server}/c/{tok_a}/bank/exchange",
                             data=payload_a, method="POST",
                             headers={"Content-Type": "application/json"})
        assert s == 200
        payload_b = _json.dumps({
            "public_token": "public-B",
            "metadata": {"institution": {"name": "Bank B"},
                         "accounts": [{"name": "Checking B", "subtype": "checking"}]},
        }).encode()
        s, _h, _b = _request(f"{http_server}/c/{tok_b}/bank/exchange",
                             data=payload_b, method="POST",
                             headers={"Content-Type": "application/json"})
        assert s == 200
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT client_code, firm_code, plaid_access_token "
                "FROM bank_connections ORDER BY client_code"
            ).fetchall()
        assert [r["client_code"] for r in rows] == ["BA1", "BA2"]
        # Each row carries the right firm + a distinct access token.
        assert rows[0]["plaid_access_token"] == "access-public-A"
        assert rows[1]["plaid_access_token"] == "access-public-B"
        for r in rows:
            assert r["firm_code"] == firm_code

    # ------- Phase 6 -------

    def test_11_cpa_sees_client_document_in_queue(self, provisioned_firm, http_server):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        username = provisioned_firm["provision"]["admin_username"]
        _seed_login(rd, username, "Strong-Pass-9", firm_code)
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)", ("MINE", "Mine Corp", firm_code))
            conn.execute(
                "INSERT INTO firms (firm_code, firm_name, active, subscription_status) "
                "VALUES (?,?,1,'active')", ("CPA_OTHER", "Other Firm"))
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)", ("THEIRS", "Their Corp", "CPA_OTHER"))
            conn.executemany(
                "INSERT INTO documents (document_id, file_name, client_code, "
                "review_status, vendor, amount, created_at) "
                "VALUES (?,?,?,?,?,?,datetime('now'))",
                [("d_mine", "my_receipt.pdf", "MINE", "New", "AcmeVendor", 100.0),
                 ("d_their", "their_receipt.pdf", "THEIRS", "New", "OtherVendor", 50.0)])
            conn.commit()
        cookie = _issue_session_cookie(rd, username)
        status, _h, body = _get(f"{http_server}/", headers={"Cookie": cookie})
        assert status in (200, 303)
        # Mine Corp's doc appears; other firm's does NOT.
        assert b"my_receipt.pdf" in body or b"Mine Corp" in body
        assert b"their_receipt.pdf" not in body
        assert b"OtherVendor" not in body

    def test_12_cpa_can_approve_document(self, provisioned_firm):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        # Seed a doc in jean's firm + a doc in another firm.
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)", ("M2", "Mine 2", firm_code))
            conn.execute(
                "INSERT INTO firms (firm_code, firm_name, active, subscription_status) "
                "VALUES (?,?,1,'active')", ("CPA_Z", "Z"))
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)", ("OZ", "Z Client", "CPA_Z"))
            conn.execute(
                "INSERT INTO documents (document_id, file_name, client_code, "
                "review_status, created_at) VALUES (?,?,?,?,datetime('now'))",
                ("mine_doc", "mine.pdf", "M2", "New"))
            conn.execute(
                "INSERT INTO documents (document_id, file_name, client_code, "
                "review_status, created_at) VALUES (?,?,?,?,datetime('now'))",
                ("their_doc", "theirs.pdf", "OZ", "New"))
            conn.commit()
        jean_ctx = {"role": "firm_admin", "firm_code": firm_code,
                    "can_view_all_clients": True, "can_post_qbo": True}
        # Jean owns mine_doc; firm isolation helper should agree.
        assert rd._require_document_in_firm("mine_doc", jean_ctx) is True
        # Jean must not own their_doc.
        assert rd._require_document_in_firm("their_doc", jean_ctx) is False

    # ------- Phase 7 -------

    def test_13_qbo_connect_uses_client_code(self, provisioned_firm,
                                              http_server, monkeypatch):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        username = provisioned_firm["provision"]["admin_username"]
        _seed_login(rd, username, "Strong-Pass-9", firm_code)
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)", ("QC1", "QBO Client 1", firm_code))
            conn.commit()
        monkeypatch.setenv("QBO_CLIENT_ID", "fake_id")
        monkeypatch.setenv("QBO_CLIENT_SECRET", "fake_secret")
        monkeypatch.setenv("QBO_REDIRECT_URI", "http://test/qbo/callback")
        cookie = _issue_session_cookie(rd, username)
        status, headers, _b = _get(
            f"{http_server}/qbo/connect?client_code=QC1",
            headers={"Cookie": cookie})
        assert status in (302, 303)
        loc = headers.get("Location", "")
        assert "appcenter.intuit.com" in loc
        # State must encode our client_code.
        from urllib.parse import urlparse, parse_qs
        state = parse_qs(urlparse(loc).query).get("state", [""])[0]
        from src.agents.tools.qbo_oauth import decode_state
        client, nonce = decode_state(state)
        assert client == "QC1"
        assert nonce
        # CSRF nonce cookie is issued.
        set_cookie = headers.get("Set-Cookie", "")
        assert f"qbo_oauth_csrf={nonce}" in set_cookie

    def test_14_qbo_callback_stores_per_client_tokens(self, provisioned_firm,
                                                       http_server, monkeypatch):
        rd = provisioned_firm["rd"]
        qbo_oauth = provisioned_firm["qbo_oauth"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        username = provisioned_firm["provision"]["admin_username"]
        _seed_login(rd, username, "Strong-Pass-9", firm_code)
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.executemany(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)",
                [("CALL1", "Call Client 1", firm_code),
                 ("CALL2", "Call Client 2", firm_code)])
            conn.commit()
        monkeypatch.setenv("QBO_CLIENT_ID", "id")
        monkeypatch.setenv("QBO_CLIENT_SECRET", "sec")
        monkeypatch.setenv("QBO_REDIRECT_URI", "http://test/qbo/callback")

        def _mock_exchange(code, redirect_uri=None):
            return {"access_token": f"at-{code}",
                    "refresh_token": f"rt-{code}",
                    "expires_in": 3600}
        monkeypatch.setattr(qbo_oauth, "exchange_code_for_token", _mock_exchange)
        # Also patch the reference the review_dashboard module picks up at import-time.
        import scripts.review_dashboard as _rdmod
        # Ensure review_dashboard's local import picks up the patched function.
        monkeypatch.setattr("src.agents.tools.qbo_oauth.exchange_code_for_token",
                            _mock_exchange, raising=False)

        sess_cookie = _issue_session_cookie(rd, username)
        # Round 1: connect CALL1 to realm-AAA.
        from urllib.parse import urlparse, parse_qs
        s, h, _ = _get(f"{http_server}/qbo/connect?client_code=CALL1",
                       headers={"Cookie": sess_cookie})
        state_1 = parse_qs(urlparse(h["Location"]).query)["state"][0]
        nonce_1 = h["Set-Cookie"].split("qbo_oauth_csrf=")[1].split(";")[0]
        cbs, cbh, _ = _get(
            f"{http_server}/qbo/callback?code=code1&realmId=realm-AAA&state={state_1}",
            headers={"Cookie": f"{sess_cookie}; qbo_oauth_csrf={nonce_1}"})
        assert cbs in (302, 303)
        # Round 2: connect CALL2 to realm-BBB.
        s, h, _ = _get(f"{http_server}/qbo/connect?client_code=CALL2",
                       headers={"Cookie": sess_cookie})
        state_2 = parse_qs(urlparse(h["Location"]).query)["state"][0]
        nonce_2 = h["Set-Cookie"].split("qbo_oauth_csrf=")[1].split(";")[0]
        cbs2, _cbh2, _ = _get(
            f"{http_server}/qbo/callback?code=code2&realmId=realm-BBB&state={state_2}",
            headers={"Cookie": f"{sess_cookie}; qbo_oauth_csrf={nonce_2}"})
        assert cbs2 in (302, 303)
        # Tokens stored with distinct realms.
        t1 = qbo_oauth.get_qbo_tokens(firm_code, "CALL1")
        t2 = qbo_oauth.get_qbo_tokens(firm_code, "CALL2")
        assert t1["realm_id"] == "realm-AAA"
        assert t2["realm_id"] == "realm-BBB"
        assert t1["access_token"] != t2["access_token"]

    def test_15_document_post_uses_correct_realm(self, provisioned_firm):
        rd = provisioned_firm["rd"]
        qbo_oauth = provisioned_firm["qbo_oauth"]
        adapter = provisioned_firm["adapter"]
        db = provisioned_firm["db"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        # Two clients, two realms.
        with sqlite3.connect(str(db)) as conn:
            conn.executemany(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)",
                [("PA", "Post A", firm_code), ("PB", "Post B", firm_code)])
            conn.commit()
        qbo_oauth.store_qbo_tokens(firm_code, "PA", "realm-100", "tok-A", "rt-A", 3600)
        qbo_oauth.store_qbo_tokens(firm_code, "PB", "realm-200", "tok-B", "rt-B", 3600)

        def _seed_job(pid, cc):
            with sqlite3.connect(str(db)) as conn:
                payload = (
                    '{"client_code":"' + cc + '","entry_kind":"expense","amount":10,'
                    '"document_date":"2026-04-18","vendor":"V","gl_account":"5440"}'
                )
                conn.execute(
                    "INSERT INTO posting_jobs (posting_id, document_id, target_system, "
                    "entry_kind, posting_status, approval_state, payload_json, "
                    "created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
                    (pid, f"doc_{pid}", "qbo", "expense",
                     "ready_to_post", "approved_for_posting", payload))
                conn.commit()
        _seed_job("J_A", "PA")
        _seed_job("J_B", "PB")
        calls = []

        def _fake_post_json(*, url, access_token, payload):
            calls.append({"url": url, "token": access_token})
            return {"Purchase": {"Id": "ext-" + access_token}}
        with patch.object(adapter, "post_json", side_effect=_fake_post_json), \
             patch.object(adapter, "build_qbo_api_payload", return_value={"Line": []}):
            r_a = adapter.post_one_ready_job("J_A", db_path=db)
            r_b = adapter.post_one_ready_job("J_B", db_path=db)
        assert r_a["status"] == "posted"
        assert r_b["status"] == "posted"
        assert "realm-100" in calls[0]["url"]
        assert calls[0]["token"] == "tok-A"
        assert "realm-200" in calls[1]["url"]
        assert calls[1]["token"] == "tok-B"

    # ------- Phase 8 -------

    def test_16_stripe_subscription_deleted_deactivates_firm(self, provisioned_firm):
        rd = provisioned_firm["rd"]
        # Fake a subscription.deleted event referencing our customer.
        event = {
            "type": "customer.subscription.deleted",
            "data": {"object": {"customer": "cus_TREMBLAY",
                                 "id": "sub_TREMBLAY"}},
        }
        rd._handle_stripe_event(event)
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            firm = conn.execute(
                "SELECT active, subscription_status FROM firms "
                "WHERE stripe_customer_id='cus_TREMBLAY'").fetchone()
        assert firm is not None
        assert firm["active"] == 0
        assert firm["subscription_status"] == "canceled"

    def test_17_forgot_password_flow_works(self, provisioned_firm):
        rd = provisioned_firm["rd"]
        username = provisioned_firm["provision"]["admin_username"]
        # Simulate forgot POST lookup (same query the POST handler runs).
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = rd._dict_factory
            row = conn.execute(
                "SELECT username, email FROM dashboard_users "
                "WHERE active=1 AND (username=? OR email=?) LIMIT 1",
                (username, username),
            ).fetchone()
        assert row and row["username"] == username
        token = rd._generate_password_link(row["username"], expires_hours=72)
        assert rd._verify_password_link(token) == username
        # Simulate /set-password POST with a NEW password.
        err = rd._validate_password_strength("Brand-New-Pass-1",
                                              "Brand-New-Pass-1")
        assert err == ""
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
                "WHERE username=?",
                (rd.hash_password("Brand-New-Pass-1"), username))
            conn.commit()
            stored = conn.execute(
                "SELECT password_hash FROM dashboard_users WHERE username=?",
                (username,)).fetchone()[0]
        assert rd.verify_password("Brand-New-Pass-1", stored)

    # ------- Phase 9 -------

    def test_18_invalid_portal_token_rejected(self, fresh_db, http_server):
        status, _h, body = _get(
            f"{http_server}/c/bogus-token-12345-but-long-enough-for-len-check")
        assert status == 404
        text = body.decode("utf-8").lower()
        assert "invalid" in text or "invalide" in text

    def test_19_rotated_token_invalidates_old(self, provisioned_firm):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        tok_old = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) "
                "VALUES (?,?,?,1,?,datetime('now'))",
                ("ROT", "Rot Client", firm_code, tok_old))
            conn.commit()
        ctx = {"role": "firm_admin", "firm_code": firm_code}
        tok_new = rd.rotate_portal_token("ROT", ctx)
        assert tok_new and tok_new != tok_old
        assert rd.resolve_portal_token(tok_old) is None
        row = rd.resolve_portal_token(tok_new)
        assert row is not None
        assert row["client_code"] == "ROT"

    def test_20_portal_rate_limit_enforced(self, provisioned_firm, http_server):
        rd = provisioned_firm["rd"]
        firm_code = provisioned_firm["provision"]["firm_code"]
        tok = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) "
                "VALUES (?,?,?,1,?,datetime('now'))",
                ("RL", "Rate Ltd", firm_code, tok))
            conn.commit()
        # Hit the in-memory per-token limiter directly (100/min cap).
        for _ in range(100):
            assert rd._portal_rate_allowed(tok, "1.1.1.1")
        assert rd._portal_rate_allowed(tok, "1.1.1.1") is False
        # A fresh IP with no token also falls under the per-IP cap (20/min).
        for _ in range(20):
            assert rd._portal_rate_allowed("", "2.2.2.2")
        assert rd._portal_rate_allowed("", "2.2.2.2") is False

    # ------- Phase 10 -------

    def test_21_firm_a_cannot_access_firm_b_anything(self, fresh_db, http_server):
        rd = fresh_db["rd"]
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            a = rd._provision_firm_from_stripe(
                _fake_stripe_session("alice@a.com", "cus_A"), base_url="http://t")
            b = rd._provision_firm_from_stripe(
                _fake_stripe_session("bob@b.com", "cus_B"), base_url="http://t")
        _seed_login(rd, a["admin_username"], "Strong-Pass-9", a["firm_code"])
        _seed_login(rd, b["admin_username"], "Strong-Pass-9", b["firm_code"])
        # Seed a client under firm B with a portal token.
        tok_b1 = rd.generate_portal_token()
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.executemany(
                "INSERT INTO clients (client_code, client_name, firm_code, active, "
                "portal_token, portal_token_created_at) "
                "VALUES (?,?,?,1,?,datetime('now'))",
                [("A1", "A's Client 1", a["firm_code"], rd.generate_portal_token()),
                 ("B1", "B's Client 1", b["firm_code"], tok_b1)])
            conn.execute(
                "INSERT INTO documents (document_id, file_name, client_code, "
                "review_status, created_at) VALUES (?,?,?,?,datetime('now'))",
                ("doc_b", "secret.pdf", "B1", "New"))
            conn.commit()
        cookie_a = _issue_session_cookie(rd, a["admin_username"])
        # /clients/edit?code=B1 must not render.
        status, _h, body = _get(
            f"{http_server}/clients/edit?code=B1",
            headers={"Cookie": cookie_a})
        # We accept either a forbidden page or a redirect back to /clients.
        assert status in (303, 302, 403) or b"B's Client" not in body

        # Helper-level firm isolation on document + client.
        ctx_a = {"role": "firm_admin", "firm_code": a["firm_code"],
                 "can_post_qbo": True, "can_view_all_clients": True}
        assert rd._require_document_in_firm("doc_b", ctx_a) is False
        assert rd._require_client_in_firm("B1", ctx_a) is False

        # /qbo/connect?client_code=B1 must 303 back to status with error.
        # We verify at the helper level since the HTTP route redirects.
        # POST /c/{B1_token}/upload WITHOUT the token path element should be
        # rejected by /upload (which requires session for firm_admin) — here we
        # verify a random bogus portal path is 404.
        s, _h, _b = _get(f"{http_server}/c/this_is_not_token_but_long_enough_1234")
        assert s == 404

    def test_22_employee_cannot_escalate(self, fresh_db, http_server):
        rd = fresh_db["rd"]
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            prov = rd._provision_firm_from_stripe(
                _fake_stripe_session("boss@firm.com", "cus_BOSS"),
                base_url="http://t")
        firm_code = prov["firm_code"]
        # Create manager + employee users.
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role, "
                "firm_code, email, active, must_reset_password, created_at) "
                "VALUES (?,?,?,?,?,1,0,?)",
                ("mgr@firm.com", rd.hash_password("Strong-Pass-9"),
                 "manager", firm_code, "mgr@firm.com", rd.utc_now_iso()))
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role, "
                "firm_code, email, active, must_reset_password, created_at) "
                "VALUES (?,?,?,?,?,1,0,?)",
                ("emp@firm.com", rd.hash_password("Strong-Pass-9"),
                 "employee", firm_code, "emp@firm.com", rd.utc_now_iso()))
            conn.execute(
                "INSERT INTO clients (client_code, client_name, firm_code, active) "
                "VALUES (?,?,?,1)", ("EC", "Employee Client", firm_code))
            conn.execute(
                "INSERT INTO documents (document_id, file_name, client_code, "
                "review_status, created_at) VALUES (?,?,?,?,datetime('now'))",
                ("emp_doc", "x.pdf", "EC", "New"))
            conn.commit()
        cookie = _issue_session_cookie(rd, "emp@firm.com")

        # /qbo/post — employees can't post.
        status, _h, _b = _form_post(
            f"{http_server}/qbo/post", {"document_id": "emp_doc"},
            headers={"Cookie": cookie})
        assert status in (403, 404)

        # /journal_entries — needs manage_journal_entries capability.
        status, _h, _b = _get(f"{http_server}/journal_entries",
                              headers={"Cookie": cookie})
        assert status == 403

        # /users — needs manage_users capability.
        status, _h, _b = _get(f"{http_server}/users", headers={"Cookie": cookie})
        assert status == 403

        # /clients/new — employees lack can_view_all_clients.
        status, _h, _b = _get(f"{http_server}/clients/new",
                              headers={"Cookie": cookie})
        assert status == 403

"""
tests/test_product_bug_fixes.py
================================
Regression tests for three bugs surfaced by tests/test_e2e_cpa_journey.py:

1. ``firms`` table missing Stripe billing columns on fresh installs
   (review_dashboard.py:750-760). Signup broke with
   ``OperationalError: no such column: stripe_customer_id``.

2. ``/clients/save`` did not populate ``portal_token`` for new clients
   (review_dashboard.py:20379-20384). The QR-code portal flow silently
   broke for minutes/hours until the next ``bootstrap_schema()`` backfill.
   Worse: ``INSERT OR REPLACE`` was nuking the portal token on *edits* too.

3. The ``do_GET`` catch-all referenced ``traceback`` after an inner
   ``import traceback`` had turned the name into a function-local, so
   any unhandled GET crashed with ``UnboundLocalError`` and the client
   saw ``RemoteDisconnected`` (review_dashboard.py:17203).
"""
from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import threading
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
# Fixtures
# ---------------------------------------------------------------------------

STRIPE_FIRM_COLS = (
    "billing_email",
    "stripe_customer_id",
    "stripe_subscription_id",
    "subscription_status",
    "subscription_plan",
    "current_period_end",
)


def _prebootstrap_non_firms(db: Path) -> None:
    """Create the tables bootstrap_schema() ALTERs but never CREATEs — leaving
    ``firms`` out so we exercise the fresh CREATE path."""
    conn = sqlite3.connect(str(db))
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
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, client_code TEXT, review_status TEXT,
            vendor TEXT, updated_at TEXT, created_at TEXT,
            fraud_flags TEXT, substance_flags TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT
        );
        CREATE TABLE IF NOT EXISTS document_assignments (
            document_id TEXT PRIMARY KEY,
            assigned_to TEXT
        );
        CREATE TABLE IF NOT EXISTS bank_connections (
            id TEXT PRIMARY KEY,
            client_code TEXT,
            plaid_access_token TEXT,
            plaid_item_id TEXT,
            firm_code TEXT
        );
    """)
    conn.commit()
    conn.close()


@pytest.fixture
def rd(tmp_path, monkeypatch):
    """Review dashboard reloaded against a fresh DB + secret file."""
    db_path = tmp_path / "bug_fix.db"
    secret_file = tmp_path / "password_link_secret"
    secret_file.write_text("b" * 48)
    _prebootstrap_non_firms(db_path)

    import scripts.review_dashboard as _rd
    monkeypatch.setattr(_rd, "DB_PATH", db_path)
    monkeypatch.setattr(_rd, "PASSWORD_LINK_SECRET_FILE", str(secret_file))
    _rd.bootstrap_schema()
    _rd._portal_token_log.clear()
    _rd._portal_ip_log.clear()
    return _rd


@pytest.fixture
def http_server(rd):
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_a, **_kw):
        return None


_NO_REDIRECT = urllib.request.build_opener(_NoRedirect())


def _request(url, *, data=None, headers=None, method=None):
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=dict(headers or {}))
    try:
        with _NO_REDIRECT.open(req, timeout=5) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _form_post(url, fields, *, headers=None):
    from urllib.parse import urlencode, urlparse
    body = urlencode(fields).encode()
    parsed = urlparse(url)
    same_origin = f"{parsed.scheme}://{parsed.netloc}"
    hdrs = {"Content-Type": "application/x-www-form-urlencoded",
            "Origin": same_origin,
            **(headers or {})}
    return _request(url, data=body, headers=hdrs, method="POST")


def _login_as(rd, username, password, firm_code, role="firm_admin"):
    with sqlite3.connect(str(rd.DB_PATH)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_users "
            "(username, password_hash, role, firm_code, email, active, "
            " must_reset_password, created_at) "
            "VALUES (?,?,?,?,?,1,0,?)",
            (username, rd.hash_password(password), role, firm_code,
             username, rd.utc_now_iso()))
        conn.commit()
    return f"session_token={rd.create_session(username)}"


# ---------------------------------------------------------------------------
# BUG 1 — firms Stripe columns
# ---------------------------------------------------------------------------

class TestFirmsStripeColumns:
    def test_bootstrap_schema_creates_firms_with_stripe_columns(self, rd):
        """Fresh install: bootstrap_schema creates firms with all Stripe
        billing columns needed by _provision_firm_from_stripe."""
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(firms)")}
        for col in STRIPE_FIRM_COLS:
            assert col in cols, f"firms table missing column {col}"
        # _provision_firm_from_stripe is the original crash site — it must
        # succeed on the freshly-bootstrapped DB.
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            result = rd._provision_firm_from_stripe(
                {"customer_email": "new@firm.com",
                 "customer": "cus_NEW",
                 "subscription": "sub_NEW",
                 "metadata": {"plan": "pro"}},
                base_url="http://test")
        assert result["existing"] is False
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            row = conn.execute(
                "SELECT subscription_status, stripe_customer_id, billing_email "
                "FROM firms WHERE stripe_customer_id='cus_NEW'").fetchone()
        assert row is not None
        assert row[0] == "active"
        assert row[1] == "cus_NEW"
        assert row[2] == "new@firm.com"

    def test_firms_migration_adds_missing_columns_idempotent(self, tmp_path,
                                                              monkeypatch):
        """Legacy install: a firms table without any Stripe columns gets
        migrated. Running bootstrap twice must not fail or duplicate columns."""
        db = tmp_path / "legacy.db"
        secret = tmp_path / "password_link_secret"
        secret.write_text("c" * 48)
        # Legacy firms table — the old 8-column shape.
        conn = sqlite3.connect(str(db))
        conn.executescript("""
            CREATE TABLE firms (
                firm_code TEXT PRIMARY KEY,
                firm_name TEXT NOT NULL,
                contact_email TEXT,
                contact_phone TEXT,
                language TEXT DEFAULT 'fr',
                plan TEXT DEFAULT 'basic',
                active INTEGER DEFAULT 1,
                created_at TEXT
            );
            INSERT INTO firms (firm_code, firm_name) VALUES ('LEGACY','Legacy');
            CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT);
            CREATE TABLE documents (document_id TEXT PRIMARY KEY,
                client_code TEXT, review_status TEXT, vendor TEXT,
                updated_at TEXT, created_at TEXT,
                fraud_flags TEXT, substance_flags TEXT);
            CREATE TABLE posting_jobs (posting_id TEXT PRIMARY KEY,
                document_id TEXT);
            CREATE TABLE document_assignments (document_id TEXT PRIMARY KEY,
                assigned_to TEXT);
            CREATE TABLE bank_connections (id TEXT PRIMARY KEY, firm_code TEXT);
        """)
        conn.commit()
        conn.close()
        import scripts.review_dashboard as _rd
        monkeypatch.setattr(_rd, "DB_PATH", db)
        monkeypatch.setattr(_rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
        _rd.bootstrap_schema()
        # Second run is a no-op (idempotent).
        _rd.bootstrap_schema()
        with sqlite3.connect(str(db)) as conn2:
            cols = {r[1] for r in conn2.execute("PRAGMA table_info(firms)")}
            legacy = conn2.execute(
                "SELECT firm_name FROM firms WHERE firm_code='LEGACY'"
            ).fetchone()
        for col in STRIPE_FIRM_COLS:
            assert col in cols, f"migration did not add {col}"
        # Existing rows are preserved.
        assert legacy is not None and legacy[0] == "Legacy"


# ---------------------------------------------------------------------------
# BUG 2 — /clients/save portal_token at insert time
# ---------------------------------------------------------------------------

class TestClientPortalToken:
    def test_new_client_insert_generates_portal_token(self, rd, http_server):
        # Set up a firm + firm_admin.
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO firms (firm_code, firm_name, active, "
                "subscription_status) VALUES (?,?,1,'active')",
                ("FBUG2", "Bug2 Firm"))
            conn.commit()
        cookie = _login_as(rd, "admin@bug2.com", "Strong-Pass-9", "FBUG2")
        status, _h, _b = _form_post(
            f"{http_server}/clients/save",
            {"client_code": "NEWC", "client_name": "New Client",
             "language": "fr", "active": "1"},
            headers={"Cookie": cookie})
        assert status in (200, 303)
        # Portal token is populated IMMEDIATELY — no bootstrap backfill needed.
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT portal_token, portal_token_created_at, "
                "portal_token_rotated_count "
                "FROM clients WHERE client_code='NEWC'").fetchone()
        assert row is not None
        assert row["portal_token"]
        assert len(row["portal_token"]) >= 32
        assert row["portal_token_created_at"]
        assert row["portal_token_rotated_count"] == 0

    def test_new_client_portal_token_is_unique(self, rd, http_server):
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO firms (firm_code, firm_name, active, "
                "subscription_status) VALUES (?,?,1,'active')",
                ("FUNQ", "Unique Firm"))
            conn.commit()
        cookie = _login_as(rd, "admin@funq.com", "Strong-Pass-9", "FUNQ")
        for code in ("U1", "U2", "U3"):
            s, _h, _b = _form_post(
                f"{http_server}/clients/save",
                {"client_code": code, "client_name": f"Client {code}",
                 "language": "fr", "active": "1"},
                headers={"Cookie": cookie})
            assert s in (200, 303)
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            tokens = [r[0] for r in conn.execute(
                "SELECT portal_token FROM clients "
                "WHERE client_code IN ('U1','U2','U3')")]
        assert len(tokens) == 3
        assert len(set(tokens)) == 3  # all distinct
        for t in tokens:
            assert t and len(t) >= 32

    def test_edit_client_preserves_portal_token(self, rd, http_server):
        """Regression: the old ``INSERT OR REPLACE`` wiped portal_token and
        rotation count when a firm_admin edited an existing client."""
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO firms (firm_code, firm_name, active, "
                "subscription_status) VALUES (?,?,1,'active')",
                ("FEDIT", "Edit Firm"))
            conn.commit()
        cookie = _login_as(rd, "admin@fedit.com", "Strong-Pass-9", "FEDIT")
        # Create the client via the route so it gets a portal_token.
        _form_post(
            f"{http_server}/clients/save",
            {"client_code": "EDIT1", "client_name": "Before",
             "language": "fr", "active": "1"},
            headers={"Cookie": cookie})
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            original_token = conn.execute(
                "SELECT portal_token FROM clients WHERE client_code='EDIT1'"
            ).fetchone()[0]
        # Rotate once so the counter is > 0, then edit the client name.
        ctx = {"role": "firm_admin", "firm_code": "FEDIT"}
        rotated_token = rd.rotate_portal_token("EDIT1", ctx)
        assert rotated_token and rotated_token != original_token
        _form_post(
            f"{http_server}/clients/save",
            {"client_code": "EDIT1", "client_name": "After",
             "language": "en", "active": "1"},
            headers={"Cookie": cookie})
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT client_name, portal_token, portal_token_rotated_count "
                "FROM clients WHERE client_code='EDIT1'").fetchone()
        assert row["client_name"] == "After"
        # The rotated token survives the edit; count is preserved.
        assert row["portal_token"] == rotated_token
        assert row["portal_token_rotated_count"] == 1


# ---------------------------------------------------------------------------
# BUG 3 — error handler no longer crashes
# ---------------------------------------------------------------------------

class TestErrorHandler:
    def test_error_handler_renders_500_page_not_crash(self, rd, http_server,
                                                       monkeypatch):
        """Force do_GET to raise; the catch-all must reply with an HTML 500,
        not close the socket with UnboundLocalError."""
        def _boom(*_a, **_kw):
            raise RuntimeError("simulated outage")
        monkeypatch.setattr(rd, "get_session_user", _boom)
        # Any authenticated path works — /qbo/status hits get_session_user.
        status, _h, body = _request(f"{http_server}/qbo/status", method="GET")
        assert status == 500
        text = body.decode("utf-8", errors="replace")
        # A real HTML body — not an empty / closed-socket response.
        assert "Internal Server Error" in text or "Unhandled Error" in text
        assert len(body) > 100

    def test_error_handler_hides_traceback_when_not_debug(self, rd, http_server,
                                                          monkeypatch):
        monkeypatch.delenv("OTOCPA_DEBUG", raising=False)

        def _boom(*_a, **_kw):
            raise RuntimeError("secret-leaking-function-name")
        monkeypatch.setattr(rd, "get_session_user", _boom)
        status, _h, body = _request(f"{http_server}/qbo/status", method="GET")
        assert status == 500
        text = body.decode("utf-8", errors="replace")
        # The raw exception / traceback must not leak to the client in prod.
        assert "secret-leaking-function-name" not in text
        assert "Traceback" not in text
        assert "Internal Server Error" in text

    def test_error_handler_shows_traceback_when_debug(self, rd, http_server,
                                                      monkeypatch):
        monkeypatch.setenv("OTOCPA_DEBUG", "1")

        def _boom(*_a, **_kw):
            raise RuntimeError("visible-in-debug")
        monkeypatch.setattr(rd, "get_session_user", _boom)
        status, _h, body = _request(f"{http_server}/qbo/status", method="GET")
        assert status == 500
        text = body.decode("utf-8", errors="replace")
        assert "visible-in-debug" in text
        assert "Traceback" in text

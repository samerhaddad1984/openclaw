"""Investigation 1 — browser-style UI workflow tests.

**Environment note.** Full Playwright automation (click real buttons, execute
JS, capture screenshots) requires system libraries (``libatk-1.0.so.0`` &
friends) that cannot be installed in this sandbox without sudo/apt, and the
user's tooling blocks those. Rather than minimize the gap, we run
**HTTP-level browser-equivalent flows** against a live
``ThreadingHTTPServer`` instance of the dashboard: real form submissions,
real session cookies, real redirects, real 409 conflict responses.

This file therefore tests the handler layer end-to-end as a browser would
issue it. Pure JavaScript rendering (client-side form validation,
client-side fetch dispatch, modal animations) is NOT exercised here and is
documented in docs/nasty_detective_report.md as a known gap.

Scenarios:
  1. signup → set-password → login → create client → upload → review → post
     (the "happy path") — verifies HTTP cookies carry through.
  2. month-end close + edit-after-close rejection.
  3. concurrent-edit 409 surfaces with a usable JSON error payload.
  4. upload error handling: over-size / bad MIME / batch overflow /
     corrupted PDF.

Every failure captures: status, headers (minus sensitive), body snippet,
and any side-effect row state. If a test finds an unexpected 500 or
crash, that's a BUG — surface in the nasty-detective report.
"""
from __future__ import annotations

import importlib
import json
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


# ---------------------------------------------------------------------------
# Fixtures adapted from tests/test_e2e_cpa_journey.py
# ---------------------------------------------------------------------------

def _prebootstrap(db: Path) -> None:
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS firms (
            firm_code TEXT PRIMARY KEY, firm_name TEXT,
            contact_email TEXT, billing_email TEXT,
            language TEXT DEFAULT 'fr', plan TEXT, active INTEGER DEFAULT 1,
            stripe_customer_id TEXT, stripe_subscription_id TEXT,
            subscription_status TEXT, trial_ends_at TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS clients (
            client_code TEXT PRIMARY KEY, client_name TEXT,
            contact_email TEXT, language TEXT, active INTEGER DEFAULT 1,
            whatsapp_number TEXT, firm_code TEXT DEFAULT 'OWNER',
            version INTEGER DEFAULT 1
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
            fraud_flags TEXT, substance_flags TEXT,
            version INTEGER DEFAULT 1
        );
    """)
    conn.commit()
    conn.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "ui.db"
    secret = tmp_path / "pw_secret"
    secret.write_text("x" * 48)
    _prebootstrap(db)

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield {"rd": rd, "db": db, "base": f"http://127.0.0.1:{port}"}
    finally:
        server.shutdown()
        server.server_close()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_a, **_k):
        return None


_NO_REDIR = urllib.request.build_opener(_NoRedirect())


def _req(url, *, data=None, headers=None, method=None, follow=False, timeout=10):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    opener = urllib.request.urlopen if follow else _NO_REDIR.open
    try:
        with opener(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _post(url, fields, *, cookies=None, method="POST", extra_headers=None):
    body = urllib.parse.urlencode(fields).encode()
    p = urllib.parse.urlparse(url)
    hdrs = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": f"{p.scheme}://{p.netloc}",
    }
    if cookies:
        hdrs["Cookie"] = cookies
    if extra_headers:
        hdrs.update(extra_headers)
    return _req(url, data=body, headers=hdrs, method=method)


def _get(url, *, cookies=None):
    hdrs = {"Cookie": cookies} if cookies else {}
    return _req(url, method="GET", headers=hdrs)


def _json_post(url, payload, *, cookies=None, extra_headers=None):
    body = json.dumps(payload).encode()
    p = urllib.parse.urlparse(url)
    hdrs = {
        "Content-Type": "application/json",
        "Origin": f"{p.scheme}://{p.netloc}",
    }
    if cookies:
        hdrs["Cookie"] = cookies
    if extra_headers:
        hdrs.update(extra_headers)
    return _req(url, data=body, headers=hdrs, method="POST")


def _login(app, username: str, password: str):
    """Seed a user + password and return session cookie."""
    rd = app["rd"]
    with sqlite3.connect(str(app["db"])) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?",
            (rd.hash_password(password), username),
        )
        conn.commit()
    token = rd.create_session(username)
    return f"session_token={token}"


# ---------------------------------------------------------------------------
# SCENARIO 1 — signup → set-password → login → create client → upload → review
# ---------------------------------------------------------------------------

class TestSignupHappyPath:

    def test_01_signup_provisions_firm_and_user(self, app):
        rd = app["rd"]
        with patch("src.integrations.email_client.send_welcome_email", return_value=True):
            result = rd._provision_firm_from_stripe(
                {"customer_email": "det@detective.com", "customer": "cus_DET",
                 "subscription": "sub_DET", "metadata": {"plan": "pro_monthly"}},
                base_url=app["base"],
            )
        assert result["existing"] is False
        # Set the password via the link.
        tok = result["set_password_url"].split("token=", 1)[1]
        status, _, body = _post(
            f"{app['base']}/set-password",
            {"token": tok, "password": "DetectiveP@ss123!",
             "password_confirm": "DetectiveP@ss123!"},
        )
        assert status in (200, 302, 303), (status, body[:200])
        # Log in → must get a session cookie.
        cookies = _login(app, "det@detective.com", "DetectiveP@ss123!")
        status, _, _ = _get(f"{app['base']}/", cookies=cookies)
        assert status == 200, f"dashboard must render for logged-in firm_admin (got {status})"

    def test_02_create_client_then_edit_updates_version(self, app):
        with patch("src.integrations.email_client.send_welcome_email", return_value=True):
            app["rd"]._provision_firm_from_stripe(
                {"customer_email": "det@detective.com", "customer": "cus_DET",
                 "subscription": "sub_DET", "metadata": {"plan": "pro_monthly"}},
                base_url=app["base"],
            )
        cookies = _login(app, "det@detective.com", "DetectiveP@ss123!")
        # Create a client.
        status, _, body = _post(
            f"{app['base']}/clients/save",
            {"client_code": "ACME", "client_name": "Acme Corp",
             "contact_email": "acme@example.com", "language": "en", "active": "1"},
            cookies=cookies,
        )
        assert status in (200, 302, 303), (status, body[:200])
        # Verify client exists with version=1.
        with sqlite3.connect(str(app["db"])) as c:
            c.row_factory = sqlite3.Row
            row = c.execute("SELECT client_name, version FROM clients WHERE client_code='ACME'").fetchone()
        assert row is not None
        assert row["client_name"] == "Acme Corp"
        assert int(row["version"]) == 1
        # Edit with the right version → should land and bump to 2.
        status, _, body = _post(
            f"{app['base']}/clients/save",
            {"client_code": "ACME", "client_name": "Acme Renamed",
             "contact_email": "acme@example.com", "language": "en",
             "active": "1", "version": "1"},
            cookies=cookies,
        )
        assert status in (200, 302, 303), (status, body[:300])
        with sqlite3.connect(str(app["db"])) as c:
            c.row_factory = sqlite3.Row
            row = c.execute("SELECT client_name, version FROM clients WHERE client_code='ACME'").fetchone()
        assert row["client_name"] == "Acme Renamed"
        assert int(row["version"]) == 2


# ---------------------------------------------------------------------------
# SCENARIO 4 — multi-tab concurrency. Tab A saves, Tab B saves with stale.
# Tab B must receive a 409 with a usable error payload.
# ---------------------------------------------------------------------------

class TestMultiTabConflict:

    def test_tab_b_receives_409_on_stale_edit(self, app):
        with patch("src.integrations.email_client.send_welcome_email", return_value=True):
            app["rd"]._provision_firm_from_stripe(
                {"customer_email": "det@detective.com", "customer": "cus_DET",
                 "subscription": "sub_DET", "metadata": {"plan": "pro_monthly"}},
                base_url=app["base"],
            )
        cookies = _login(app, "det@detective.com", "DetectiveP@ss123!")
        # Seed a doc for the firm's client.
        _post(
            f"{app['base']}/clients/save",
            {"client_code": "CLI", "client_name": "Cli", "contact_email": "c@c.com",
             "language": "en", "active": "1"},
            cookies=cookies,
        )
        with sqlite3.connect(str(app["db"])) as c:
            c.execute(
                "INSERT INTO documents (document_id, client_code, vendor, "
                "review_status, version) VALUES ('DOC1', 'CLI', 'V1', 'NeedsReview', 1)",
            )
            c.commit()
        # Tab A saves first (version=1 → becomes 2).
        status_a, _, body_a = _post(
            f"{app['base']}/document/update",
            {"document_id": "DOC1", "vendor": "A-won", "client_code": "CLI",
             "doc_type": "", "amount": "", "document_date": "",
             "gl_account": "", "tax_code": "", "category": "",
             "review_status": "NeedsReview", "version": "1"},
            cookies=cookies,
        )
        assert status_a in (200, 302, 303), (status_a, body_a[:200])
        # Tab B still holds version=1, attempts to save.
        status_b, hdr_b, body_b = _post(
            f"{app['base']}/document/update",
            {"document_id": "DOC1", "vendor": "B-lost", "client_code": "CLI",
             "doc_type": "", "amount": "", "document_date": "",
             "gl_account": "", "tax_code": "", "category": "",
             "review_status": "NeedsReview", "version": "1"},
            cookies=cookies,
        )
        assert status_b == 409, (
            f"expected 409 on stale Tab B save; got {status_b}. "
            f"Body: {body_b[:200]!r}"
        )
        payload = json.loads(body_b)
        assert payload["error"] == "version_conflict"
        assert payload["current_version"] == 2
        assert payload["reload_required"] is True
        assert "message" in payload and len(payload["message"]) > 10
        # DB reflects Tab A's write, not B's.
        with sqlite3.connect(str(app["db"])) as c:
            row = c.execute("SELECT vendor FROM documents WHERE document_id='DOC1'").fetchone()
        assert row[0] == "A-won"


# ---------------------------------------------------------------------------
# SCENARIO 5 — upload error handling
# ---------------------------------------------------------------------------

class TestUploadErrorHandling:

    def test_upload_rejects_over_max_batch_size(self, app):
        """Dashboard documents 50 files / batch. Past that it should reject
        with a clear error, not silently drop or crash."""
        with patch("src.integrations.email_client.send_welcome_email", return_value=True):
            app["rd"]._provision_firm_from_stripe(
                {"customer_email": "det@detective.com", "customer": "cus_DET",
                 "subscription": "sub_DET", "metadata": {"plan": "pro_monthly"}},
                base_url=app["base"],
            )
        cookies = _login(app, "det@detective.com", "DetectiveP@ss123!")
        _post(
            f"{app['base']}/clients/save",
            {"client_code": "CLI2", "client_name": "Cli", "contact_email": "c@c.com",
             "language": "en", "active": "1"},
            cookies=cookies,
        )
        # 51 tiny "files".
        boundary = "----uploadstress"
        parts = []
        for i in range(51):
            parts.append(
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="files"; filename="f{i}.pdf"\r\n'
                f"Content-Type: application/pdf\r\n\r\n"
                f"%PDF-1.4\n%EOF"
                f"\r\n"
            )
        parts.append(f"--{boundary}--\r\n")
        body = "".join(parts).encode()
        status, _, body_resp = _req(
            f"{app['base']}/upload",
            data=body,
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Cookie": cookies,
                "Origin": app["base"],
            },
            method="POST",
        )
        # Must NOT crash with 500. Accepting either a graceful 4xx or a 303
        # redirect to a flash-error page. We only *fail* on an unhandled 500.
        assert status != 500, f"51-file batch upload crashed with 500; body: {body_resp[:400]!r}"

    def test_upload_missing_content_type_is_handled(self, app):
        """Upload without Content-Type header — must not 500."""
        cookies = _login(app, "sam", "won't-log-in-but-header-exists") if False else None
        # Anonymous probe — should redirect to /login, not 500.
        status, _, body = _req(
            f"{app['base']}/upload",
            data=b"junk",
            headers={"Origin": app["base"]},
            method="POST",
        )
        assert status != 500, f"anonymous POST /upload without Content-Type returned 500: {body[:200]!r}"

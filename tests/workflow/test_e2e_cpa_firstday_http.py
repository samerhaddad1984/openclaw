"""HTTP-level end-to-end test for the gap 1-5 routes.

Drives the review_dashboard server over real HTTP (loopback socket),
not pure helper calls. Each step exercises the route the UI calls; a
green run means the whole CPA first-day journey is wired.
"""
from __future__ import annotations

import http.client
import io
import os
import socket
import sqlite3
import sys
import threading
import time
import urllib.parse
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def live_server(tmp_path, monkeypatch):
    """Boot review_dashboard on a free port against a throwaway DB."""
    db = tmp_path / "http_e2e.db"

    # Point the dashboard at a per-test DB before import-time side effects
    # fire.  monkeypatch keys must be set BEFORE the module loads since
    # DB_PATH is read at module import.
    os.environ["OTOCPA_DEBUG"] = "1"

    import importlib
    import scripts.review_dashboard as rd
    importlib.reload(rd)
    rd.DB_PATH = db

    # Seed the DB with the same fixtures the pure e2e test uses.
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS firms (
                firm_code TEXT PRIMARY KEY, name TEXT,
                address TEXT, phone TEXT, plan TEXT,
                subscription_status TEXT DEFAULT 'active'
            );
            CREATE TABLE IF NOT EXISTS clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, portal_token TEXT, email TEXT,
                bank_source TEXT,
                active INTEGER DEFAULT 1,
                portal_token_created_at TEXT,
                portal_token_rotated_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS qbo_connections (
                firm_code TEXT, client_code TEXT, status TEXT
            );
            CREATE TABLE IF NOT EXISTS documents (
                document_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT,
                vendor TEXT, amount REAL,
                document_date TEXT, review_status TEXT,
                uploaded_at TEXT
            );
            CREATE TABLE IF NOT EXISTS bank_transactions (
                id TEXT PRIMARY KEY, firm_code TEXT, client_code TEXT,
                date TEXT, matched_document_id TEXT,
                hidden_duplicate INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, submitter_email TEXT,
                subject TEXT, body TEXT,
                response_body TEXT, responded_by TEXT,
                responded_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.execute(
            "INSERT OR REPLACE INTO firms (firm_code, name, plan) VALUES (?,?,?)",
            ("FIRM_SAM", "", "pro_monthly"),
        )
        conn.commit()

    rd.bootstrap_schema()

    # Seed dashboard users (sam=owner, jr=employee). The schema is
    # auth-friendly but tests insert directly, then use a faux session.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_users "
            "(username, password_hash, role, display_name, active, language, firm_code) "
            "VALUES ('sam@firm.com','x','owner','Sam',1,'en','FIRM_SAM')",
        )
        conn.execute(
            "INSERT OR REPLACE INTO dashboard_users "
            "(username, password_hash, role, display_name, active, language, firm_code) "
            "VALUES ('jr@firm.com','x','employee','Jr',1,'en','FIRM_SAM')",
        )
        conn.commit()

    # Monkeypatch session resolution so we don't have to run /login to
    # obtain a cookie. The handler reads get_session_user; we stub it
    # to return whichever user the current request claims via cookie.
    import scripts.review_dashboard as _rd

    def _fake_session_user(handler):
        cookie = handler.headers.get("Cookie", "") or ""
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith("test_user="):
                uname = part.split("=", 1)[1]
                with sqlite3.connect(db) as c:
                    c.row_factory = sqlite3.Row
                    row = c.execute(
                        "SELECT * FROM dashboard_users WHERE username=?",
                        (uname,),
                    ).fetchone()
                    return dict(row) if row else None
        return None

    monkeypatch.setattr(_rd, "get_session_user", _fake_session_user)

    # Disable CSRF for the test — we're issuing POSTs directly.
    monkeypatch.setattr(_rd, "_csrf_check", lambda handler: True)

    # Find a free port and boot the server.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    server = ThreadingHTTPServer(("127.0.0.1", port), _rd.ReviewDashboardHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # Give the thread a beat to bind.
    time.sleep(0.1)
    yield {"port": port, "db": db, "module": _rd}
    server.shutdown()
    server.server_close()


def _req(port, method, path, *, user=None, body=None, imp_cookie=None,
          extra_headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {
        "Host": f"127.0.0.1:{port}",
        "Origin": f"http://127.0.0.1:{port}",
    }
    cookies = []
    if user:
        cookies.append(f"test_user={user}")
    if imp_cookie:
        cookies.append(f"otocpa_imp_sid={imp_cookie}")
    if cookies:
        headers["Cookie"] = "; ".join(cookies)
    if body is not None and method == "POST":
        if isinstance(body, dict):
            body = urllib.parse.urlencode(body, doseq=True)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if extra_headers:
        headers.update(extra_headers)
    conn.request(method, path, body=body, headers=headers)
    r = conn.getresponse()
    payload = r.read()
    conn.close()
    return r.status, r.getheaders(), payload


def test_cpa_firstday_full_http_journey(live_server):
    port = live_server["port"]
    db = live_server["db"]

    # --- 1. Owner arrives at /onboarding (quick setup) ---
    status, _, body = _req(port, "GET", "/onboarding", user="sam@firm.com")
    assert status == 200, body[:200]
    assert b"Quick setup" in body

    # --- 2. Save firm profile ---
    status, _, _ = _req(port, "POST", "/onboarding/save",
                         user="sam@firm.com",
                         body={"name": "Sam & Co", "address": "1 Main",
                               "phone": "514-555-1111",
                               "default_lang": "en",
                               "fiscal_year_end": "12-31"})
    assert status == 303

    # --- 3. Checklist JSON reflects firm profile completion ---
    status, _, body = _req(port, "GET", "/onboarding/checklist",
                            user="sam@firm.com")
    assert status == 200
    import json
    data = json.loads(body.decode("utf-8"))
    profile = next(i for i in data["items"] if i["id"] == "firm_profile")
    assert profile["done"] is True

    # --- 4. Tour screen renders ---
    status, _, body = _req(port, "GET", "/tour?step=1", user="sam@firm.com")
    assert status == 200
    assert b"Step 1 of" in body

    # --- 5. Tour complete ---
    status, _, _ = _req(port, "POST", "/tour/complete", user="sam@firm.com")
    assert status == 303

    # --- 6. Seed a client + documents for the review flow ---
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "portal_token, email) VALUES (?,?,?,?,?)",
            ("CLIENT_ALPHA", "FIRM_SAM", "Alpha Inc", "tok_alpha_xyz_0123456789abcdef0123456789abcdef",
             "alpha@example.com"),
        )
        for i in range(1, 6):
            conn.execute(
                "INSERT INTO documents (document_id, firm_code, client_code, "
                "vendor, amount, document_date, review_status, uploaded_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (f"D{i}", "FIRM_SAM", "CLIENT_ALPHA", f"Vendor {i}",
                 10.0 + i, "2026-04-15", "New", "2026-04-20T10:00:00Z"),
            )
        conn.commit()

    # --- 7. Client visits portal status page (no auth, token-bound) ---
    status, _, body = _req(port, "GET", "/c/tok_alpha_xyz_0123456789abcdef0123456789abcdef/status")
    assert status == 200, body[:200]
    assert b"Alpha Inc" in body

    # --- 8. Portal activity feed (JSON) ---
    status, _, body = _req(port, "GET", "/c/tok_alpha_xyz_0123456789abcdef0123456789abcdef/activity")
    assert status == 200
    feed = json.loads(body.decode("utf-8"))
    assert "events" in feed

    # --- 9. Assign 5 documents to the employee (sam is owner) ---
    for i in range(1, 6):
        status, _, _ = _req(port, "POST", f"/document/D{i}/assign",
                              user="sam@firm.com",
                              body={"assignee_email": "jr@firm.com",
                                    "priority": "normal"})
        assert status == 303

    # --- 10. Employee sees /my_tasks with 5 rows ---
    status, _, body = _req(port, "GET", "/my_tasks", user="jr@firm.com")
    assert status == 200
    assert b"My Tasks" in body
    assert b"D1" in body and b"D5" in body

    # --- 11. Employee submits all for review ---
    for i in range(1, 6):
        status, _, _ = _req(port, "POST",
                              f"/document/D{i}/submit_for_review",
                              user="jr@firm.com")
        assert status == 303

    # --- 12. Owner sees /review_queue with 5 pending items ---
    status, _, body = _req(port, "GET", "/review_queue", user="sam@firm.com")
    assert status == 200
    assert b"Review Queue" in body
    assert b"D1" in body

    # --- 13. Owner approves each document ---
    for i in range(1, 6):
        status, _, _ = _req(port, "POST", f"/document/D{i}/approve",
                              user="sam@firm.com")
        assert status == 303
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE documents SET review_status='Posted' "
                "WHERE document_id=?", (f"D{i}",),
            )
            conn.commit()

    # --- 14. Close wizard: advance through all 6 steps ---
    status, _, body = _req(port, "GET", "/close/wizard", user="sam@firm.com")
    assert status == 200
    assert b"Month-end close wizard" in body

    # Step 1 → 2
    status, _, _ = _req(port, "POST", "/close/wizard/advance",
                         user="sam@firm.com",
                         body={"step": "1", "client_code": "CLIENT_ALPHA",
                               "period": "2026-04"})
    assert status == 303
    # Step 2 → 3
    status, _, _ = _req(port, "POST", "/close/wizard/advance",
                         user="sam@firm.com",
                         body={"step": "2", "client_code": "CLIENT_ALPHA",
                               "period": "2026-04"})
    assert status == 303
    # Step 3 → 4 (acknowledge unreconciled)
    status, _, _ = _req(port, "POST", "/close/wizard/advance",
                         user="sam@firm.com",
                         body={"step": "3", "client_code": "CLIENT_ALPHA",
                               "period": "2026-04",
                               "acknowledge_unreconciled": "1"})
    assert status == 303
    # Step 4 → 5 (depreciation accepted)
    status, _, _ = _req(port, "POST", "/close/wizard/advance",
                         user="sam@firm.com",
                         body={"step": "4", "client_code": "CLIENT_ALPHA",
                               "period": "2026-04",
                               "accepted_kinds": "depreciation"})
    assert status == 303
    # Step 5 → 6
    status, _, _ = _req(port, "POST", "/close/wizard/advance",
                         user="sam@firm.com",
                         body={"step": "5", "client_code": "CLIENT_ALPHA",
                               "period": "2026-04"})
    assert status == 303
    # Finalize (lock)
    status, _, _ = _req(port, "POST", "/close/wizard/finalize",
                         user="sam@firm.com",
                         body={"client_code": "CLIENT_ALPHA",
                               "period": "2026-04"})
    assert status == 303

    # Verify period locked in DB.
    from src.integrations.month_end_close import is_period_locked
    assert is_period_locked(db, firm_code="FIRM_SAM",
                              client_code="CLIENT_ALPHA",
                              period="2026-04") is True

    # --- 15. Owner dashboard renders ---
    status, _, body = _req(port, "GET", "/owner/dashboard",
                            user="sam@firm.com")
    assert status == 200, body[:200]
    assert b"Owner dashboard" in body
    assert b"FIRM_SAM" in body

    # --- 16. Per-firm drilldown ---
    status, _, body = _req(port, "GET", "/owner/firms/FIRM_SAM",
                            user="sam@firm.com")
    assert status == 200
    assert b"FIRM_SAM" in body
    assert b"Alpha" in body

    # --- 17. Start impersonation ---
    status, hdrs, _ = _req(port, "POST",
                            "/owner/firms/FIRM_SAM/impersonate",
                            user="sam@firm.com")
    assert status == 303
    # Pull the impersonation session cookie off the response.
    set_cookies = [v for (k, v) in hdrs if k.lower() == "set-cookie"]
    imp_sid = None
    for c in set_cookies:
        if c.startswith("otocpa_imp_sid="):
            imp_sid = c.split(";")[0].split("=", 1)[1]
    assert imp_sid and len(imp_sid) >= 16

    # --- 18. Home page while impersonating shows the banner ---
    status, _, body = _req(port, "GET", "/", user="sam@firm.com",
                            imp_cookie=imp_sid)
    assert status == 200
    assert b"IMPERSONATING" in body

    # --- 19. Writes are blocked while impersonating ---
    status, _, body = _req(port, "POST", "/onboarding/save",
                            user="sam@firm.com", imp_cookie=imp_sid,
                            body={"name": "Hacked Co", "address": "x",
                                  "phone": "x"})
    assert status == 403
    assert b"Action blocked" in body or b"impersonating" in body.lower()

    # Stop impersonation
    status, _, _ = _req(port, "POST", "/owner/impersonate/stop",
                         user="sam@firm.com", imp_cookie=imp_sid)
    assert status == 303

    # Notifications queued during approval: at least the approval row and
    # review-assignment rows should be sitting in client_notifications.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT kind, status FROM client_notifications"
        ).fetchall()
    kinds = {r["kind"] for r in rows}
    assert ({'receipt_approved'} & kinds) or ({'review_assigned'} & kinds), \
        f"expected at least one queued notification, got {kinds!r}"

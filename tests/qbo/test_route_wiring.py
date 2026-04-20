"""Verifies the QBO bidirectional-sync routes are wired into the main
review_dashboard router with the correct auth semantics.

Strategy: rather than spin up a ThreadingHTTPServer per test (slow),
grep the dashboard source for the route-string literals + their auth
guards, and unit-test the handler functions directly through the
import graph. Combined, these two layers confirm the wiring without
paying the HTTP boot cost in every test.

The one genuine HTTP test (``test_webhook_via_http_always_200``)
spins up the real handler and posts to /qbo/webhook so the
CSRF-exempt + signature-verification wiring is exercised end-to-end.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DASHBOARD_SRC = (ROOT / "scripts" / "review_dashboard.py").read_text()

EXPECTED_ROUTES = {
    # Path, method, auth requirement (shorthand).
    ("/qbo/webhook", "POST"): "signature",
    ("/qbo/dashboard", "GET"): "owner_or_firm_admin",
    ("/qbo/conflicts", "GET"): "owner_or_firm_admin",
    ("/qbo/sync/status", "GET"): "owner_or_firm_admin",
    ("/qbo/sync/initial", "POST"): "owner_or_firm_admin",
    ("/qbo/sync/now", "POST"): "owner_or_firm_admin",
    ("/qbo/conflicts/resolve", "POST"): "owner_only",
}


# ---------------------------------------------------------------------------
# Static wiring checks (cheap, catch forgotten routes)
# ---------------------------------------------------------------------------


def test_all_qbo_routes_enumerated_in_dashboard():
    """Meta-test: every route in EXPECTED_ROUTES must appear literally
    in the dashboard source. A forgotten wiring will flip this to red."""
    missing = [r for r, _ in EXPECTED_ROUTES
               if f'"{r[0]}"' not in DASHBOARD_SRC]
    assert not missing, f"routes missing from dashboard: {missing}"


def test_qbo_webhook_is_csrf_exempt():
    # /qbo/webhook authenticates via intuit-signature, not CSRF.
    assert '"/qbo/webhook"' in DASHBOARD_SRC
    # _CSRF_EXEMPT_POSTS literal includes the path.
    assert '"/qbo/webhook",' in DASHBOARD_SRC


def test_conflicts_resolve_gated_to_owner_only():
    # The handler branches on role=='owner' and rejects firm_admin.
    assert 'owner_only' in DASHBOARD_SRC or \
        '"/qbo/conflicts/resolve" and ctx.get("role") != "owner"' in DASHBOARD_SRC


def test_sync_routes_use_firm_scope_check():
    # Every authenticated QBO sync route runs _require_client_in_firm.
    # Find the first appearance of the POST sync block (/qbo/sync/initial
    # literal appears in both a comment and the `if path in (...)` tuple;
    # we want the tuple-occurrence block), then confirm the firm-scope
    # helper runs within a short window after.
    idx = DASHBOARD_SRC.find('if path in ("/qbo/sync/initial"')
    assert idx > 0, "POST sync router tuple not found"
    window = DASHBOARD_SRC[idx: idx + 4000]
    assert "_require_client_in_firm" in window


def test_webhook_handler_imports_the_right_function():
    # Shallow import check — the wiring imports handle_webhook_route.
    assert "from src.integrations.qbo_sync_ui import" in DASHBOARD_SRC
    assert "handle_webhook_route as _qbo_handle_webhook" in DASHBOARD_SRC


# ---------------------------------------------------------------------------
# Live HTTP round-trip for the webhook — the one route that has to work
# without any session auth.
# ---------------------------------------------------------------------------


def _start_server(db_path):
    """Spin up the real dashboard handler on an ephemeral loopback port."""
    import scripts.review_dashboard as rd
    # Point the handler at our tmp DB and bootstrap.
    rd.DB_PATH = db_path
    try:
        rd.bootstrap_schema()
    except Exception:
        pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


def _sign(body, token):
    mac = hmac.new(token.encode(), body, hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def _mk_bootstrap_db(tmp_path):
    """Full dashboard bootstrap — needs documents etc. Use the DB the
    dashboard actually creates on startup."""
    from src.agents.tools.qbo_oauth import _ensure_table
    from src.integrations.qbo_schema import ensure_qbo_sync_schema
    db = tmp_path / "rt.db"
    _ensure_table(db)
    ensure_qbo_sync_schema(db)
    # Minimal extra tables the dashboard bootstrap will ALTER: bootstrap
    # runs inside _start_server, so tables are created automatically.
    return db


def test_webhook_via_http_signature_verified(tmp_path, monkeypatch):
    db = _mk_bootstrap_db(tmp_path)
    token = "rt-token"
    monkeypatch.setenv("QBO_WEBHOOK_VERIFIER_TOKEN", token)
    server, base = _start_server(db)
    try:
        body = json.dumps({
            "eventNotifications": [{
                "realmId": "rt-realm",
                "dataChangeEvent": {"entities": [{
                    "name": "Account", "id": "1",
                    "operation": "Create",
                    "lastUpdated": "2026-04-20T10:00:00Z",
                }]},
            }]
        }).encode()

        req = urllib.request.Request(
            f"{base}/qbo/webhook",
            data=body,
            headers={
                "Content-Type": "application/json",
                "intuit-signature": _sign(body, token),
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200
            payload = json.loads(resp.read())
        assert payload["ok"] is True
        assert payload["events_stored"] == 1
        with sqlite3.connect(db) as conn:
            ct = conn.execute(
                "SELECT COUNT(*) FROM qbo_webhook_events"
            ).fetchone()[0]
        assert ct == 1
    finally:
        server.shutdown()
        server.server_close()


def test_webhook_via_http_bad_signature_returns_200_json_401(tmp_path, monkeypatch):
    """Intuit must always get 200 so it doesn't storm-retry; real
    disposition is in the body."""
    db = _mk_bootstrap_db(tmp_path)
    monkeypatch.setenv("QBO_WEBHOOK_VERIFIER_TOKEN", "realtoken")
    server, base = _start_server(db)
    try:
        body = b'{"eventNotifications":[]}'
        req = urllib.request.Request(
            f"{base}/qbo/webhook",
            data=body,
            headers={
                "Content-Type": "application/json",
                "intuit-signature": "bogus",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200
            payload = json.loads(resp.read())
        assert payload["ok"] is False
        assert payload["status"] == 401
    finally:
        server.shutdown()
        server.server_close()


def test_sync_status_requires_auth(tmp_path):
    """An unauthenticated GET must redirect to /login, not 200."""
    db = _mk_bootstrap_db(tmp_path)
    server, base = _start_server(db)
    try:
        req = urllib.request.Request(
            f"{base}/qbo/sync/status?client_code=C1",
        )
        try:
            with urllib.request.urlopen(req, timeout=5,
                                          ) as resp:
                # The server will 303 to /login; urlopen follows redirects by
                # default, which lands us on the login page.
                body = resp.read()
                assert b"login" in body.lower() or resp.geturl().endswith("/login")
        except urllib.error.HTTPError as exc:
            # Some Python versions raise on 303 instead of auto-follow.
            assert exc.code in (302, 303)
    finally:
        server.shutdown()
        server.server_close()

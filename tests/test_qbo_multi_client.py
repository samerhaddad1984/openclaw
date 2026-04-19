"""
tests/test_qbo_multi_client.py
==============================
Sprint 3: per-client QuickBooks Online connections.

Covers:
  - store / get / refresh / disconnect / list at the token-storage layer
  - firm isolation: two firms with the same client_code don't see each other
  - refresh-token auto-refresh + failure marking status='expired'
  - CSRF state encode/decode helpers
  - posting adapter: skips when no connection, uses correct realm per client,
    auto-refreshes on 401, migration-warning visibility
  - route-level guardrails: /qbo/connect + /qbo/disconnect require firm
    isolation, /qbo/status shows all firm clients

All Intuit HTTP calls are mocked — no real network traffic.
"""
from __future__ import annotations

import sqlite3
import sys
import time
import urllib.error
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_test_db(tmp_path: Path) -> Path:
    """Create a DB with the minimum tables qbo_oauth + adapter need."""
    db = tmp_path / "qbo_multi.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE firms (
            firm_code TEXT PRIMARY KEY,
            firm_name TEXT
        );
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY,
            client_name TEXT,
            firm_code TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE dashboard_users (
            username TEXT PRIMARY KEY,
            role TEXT,
            firm_code TEXT,
            active INTEGER DEFAULT 1,
            password_hash TEXT
        );
        CREATE TABLE qbo_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_code TEXT NOT NULL,
            client_code TEXT NOT NULL,
            realm_id TEXT NOT NULL,
            access_token TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            connected_at TEXT DEFAULT (datetime('now')),
            connected_by TEXT,
            last_refreshed_at TEXT,
            last_error TEXT,
            status TEXT DEFAULT 'active',
            UNIQUE(firm_code, client_code)
        );
        CREATE TABLE posting_jobs (
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
    """)
    conn.executemany(
        "INSERT INTO firms (firm_code, firm_name) VALUES (?, ?)",
        [("CPA111", "Tremblay CPA"), ("CPA222", "Bolduc CPA"), ("OWNER", "OtoCPA")],
    )
    # Note: production schema has client_code as PK (one firm per client_code).
    # We use distinct client_codes per firm here because the firm-isolation
    # test exercises tokens — same code vs different code doesn't matter at
    # the qbo_connections layer, which keys on (firm_code, client_code).
    conn.executemany(
        "INSERT INTO clients (client_code, client_name, firm_code, active) VALUES (?,?,?,1)",
        [
            ("A1", "Tremblay Inc", "CPA111"),
            ("B2", "SSQ Ltd", "CPA111"),
            ("Z9", "Bolduc Client", "CPA222"),
        ],
    )
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def qbo_oauth(tmp_path, monkeypatch):
    """Import qbo_oauth with DB_PATH redirected at the module level AND at
    call sites. Returns (module, db_path)."""
    db = _create_test_db(tmp_path)
    import importlib
    import src.agents.tools.qbo_oauth as _m
    importlib.reload(_m)
    monkeypatch.setattr(_m, "DB_PATH", db)
    # Also force the legacy config path to a non-existent tmp location so
    # tests can toggle it independently.
    monkeypatch.setattr(_m, "CONFIG_PATH", tmp_path / "does_not_exist.json")
    return _m, db


@pytest.fixture
def adapter(tmp_path, monkeypatch, qbo_oauth):
    """Import the adapter and point its DB_PATH + oauth functions at test db."""
    _oauth_mod, db = qbo_oauth
    import importlib
    import src.agents.tools.qbo_online_adapter as _ad
    importlib.reload(_ad)
    monkeypatch.setattr(_ad, "DB_PATH", db)

    # Rebind the adapter's references to the reloaded oauth module functions
    # so our DB_PATH monkeypatch takes effect when the adapter calls them.
    monkeypatch.setattr(_ad, "_oauth_get_qbo_tokens", _oauth_mod.get_qbo_tokens)
    monkeypatch.setattr(_ad, "_oauth_refresh_access_token", _oauth_mod.refresh_access_token)
    return _ad, db


# ---------------------------------------------------------------------------
# 1. Token storage round-trip
# ---------------------------------------------------------------------------

class TestStoreAndRetrieve:
    def test_store_and_retrieve_tokens_per_client(self, qbo_oauth):
        mod, db = qbo_oauth
        mod.store_qbo_tokens("CPA111", "A1", "9123", "atk", "rtk", 3600, "alice")
        tokens = mod.get_qbo_tokens("CPA111", "A1")
        assert tokens is not None
        assert tokens["access_token"] == "atk"
        assert tokens["realm_id"] == "9123"
        assert tokens["refresh_token"] == "rtk"
        assert tokens["status"] == "active"
        assert tokens["connected_by"] == "alice"

    def test_tokens_isolated_across_firms_same_client_code(self, qbo_oauth):
        mod, _ = qbo_oauth
        # Two different firms both call the same client "A1" — rows must not collide
        mod.store_qbo_tokens("CPA111", "A1", "realm-aaa", "t1", "r1", 3600)
        mod.store_qbo_tokens("CPA222", "A1", "realm-bbb", "t2", "r2", 3600)
        t1 = mod.get_qbo_tokens("CPA111", "A1")
        t2 = mod.get_qbo_tokens("CPA222", "A1")
        assert t1["realm_id"] == "realm-aaa"
        assert t2["realm_id"] == "realm-bbb"
        assert t1["access_token"] != t2["access_token"]


# ---------------------------------------------------------------------------
# 2. Refresh auto-refresh + failure
# ---------------------------------------------------------------------------

class TestRefreshToken:
    def test_refresh_token_updates_expires_at(self, qbo_oauth):
        mod, db = qbo_oauth
        # Seed a row that's already within the refresh window (expires in 60s)
        mod.store_qbo_tokens("CPA111", "A1", "9123", "old", "rtk", 60)

        with patch.object(mod, "_post_to_intuit_token_endpoint",
                          return_value={
                              "access_token": "NEW",
                              "refresh_token": "NEW_R",
                              "expires_in": 3600,
                          }) as mocked:
            got = mod.get_qbo_tokens("CPA111", "A1")

        mocked.assert_called_once()
        assert got["access_token"] == "NEW"
        # refreshed row has an expires_at comfortably in the future
        assert got["expires_at"] - int(time.time()) >= 3500

    def test_refresh_token_failure_marks_expired(self, qbo_oauth):
        mod, db = qbo_oauth
        mod.store_qbo_tokens("CPA111", "A1", "9123", "old", "rtk", 60)

        def _boom(*_a, **_kw):
            raise urllib.error.HTTPError(
                "u", 400, "bad refresh", {}, BytesIO(b"invalid_grant")
            )

        with patch.object(mod, "_post_to_intuit_token_endpoint", side_effect=_boom):
            got = mod.get_qbo_tokens("CPA111", "A1")
        # Still returns the row (so UI can surface the error state), but
        # status is now 'expired' and last_error is populated.
        assert got is not None
        assert got["status"] == "expired"
        assert got["last_error"]


# ---------------------------------------------------------------------------
# 3. Disconnect + list
# ---------------------------------------------------------------------------

class TestDisconnectAndList:
    def test_disconnect_removes_connection(self, qbo_oauth):
        mod, _ = qbo_oauth
        mod.store_qbo_tokens("CPA111", "A1", "9123", "atk", "rtk", 3600)
        assert mod.disconnect_qbo("CPA111", "A1") is True
        assert mod.get_qbo_tokens("CPA111", "A1") is None
        # Idempotent: deleting again returns False, does not raise
        assert mod.disconnect_qbo("CPA111", "A1") is False

    def test_list_connections_returns_only_firm_scope(self, qbo_oauth):
        mod, _ = qbo_oauth
        mod.store_qbo_tokens("CPA111", "A1", "r1", "t1", "rt1", 3600)
        mod.store_qbo_tokens("CPA111", "B2", "r2", "t2", "rt2", 3600)
        mod.store_qbo_tokens("CPA222", "Z9", "r3", "t3", "rt3", 3600)

        cpa111 = mod.list_qbo_connections("CPA111")
        assert {c["client_code"] for c in cpa111} == {"A1", "B2"}
        for row in cpa111:
            assert row["firm_code"] == "CPA111"

        cpa222 = mod.list_qbo_connections("CPA222")
        assert {c["client_code"] for c in cpa222} == {"Z9"}

        all_conns = mod.list_qbo_connections(None)
        assert len(all_conns) == 3


# ---------------------------------------------------------------------------
# 4. CSRF state
# ---------------------------------------------------------------------------

class TestCsrfState:
    def test_qbo_callback_verifies_csrf_state(self, qbo_oauth):
        mod, _ = qbo_oauth
        state = mod.build_state("A1", "nonce-abc")
        client, nonce = mod.decode_state(state)
        assert client == "A1"
        assert nonce == "nonce-abc"

    def test_qbo_state_rejects_garbage(self, qbo_oauth):
        mod, _ = qbo_oauth
        assert mod.decode_state("") == (None, None)
        assert mod.decode_state("not-b64!!!") == (None, None)
        # Valid b64 but wrong shape (no dot separator)
        import base64
        bad = base64.urlsafe_b64encode(b"nodot").decode().rstrip("=")
        assert mod.decode_state(bad) == (None, None)


# ---------------------------------------------------------------------------
# 5. Route-level firm isolation
# ---------------------------------------------------------------------------

def _rd(tmp_path, monkeypatch, db_path):
    """Import review_dashboard pointed at the test DB."""
    import importlib
    import scripts.review_dashboard as _rd
    monkeypatch.setattr(_rd, "DB_PATH", db_path)
    return _rd


class TestRouteGuardrails:
    def test_qbo_connect_requires_client_in_firm(self, tmp_path, monkeypatch, qbo_oauth):
        _, db = qbo_oauth
        rd = _rd(tmp_path, monkeypatch, db)
        # firm_admin at CPA111 — may NOT act on Z9 (belongs to CPA222)
        ctx = {"role": "firm_admin", "firm_code": "CPA111",
               "can_post_qbo": True}
        assert rd._require_client_in_firm("Z9", ctx) is False
        assert rd._require_client_in_firm("A1", ctx) is True
        # Owner sees everything
        owner_ctx = {"role": "owner", "firm_code": "OWNER"}
        assert rd._require_client_in_firm("Z9", owner_ctx) is True

    def test_disconnect_route_requires_firm_isolation(self, tmp_path, monkeypatch, qbo_oauth):
        mod, db = qbo_oauth
        mod.store_qbo_tokens("CPA222", "Z9", "realm-z9", "t", "r", 3600)
        rd = _rd(tmp_path, monkeypatch, db)
        # A CPA111 firm_admin must NOT be able to disconnect Z9
        ctx = {"role": "firm_admin", "firm_code": "CPA111",
               "can_post_qbo": True}
        assert rd._require_client_in_firm("Z9", ctx) is False
        # The row must still exist after the failed attempt
        assert mod.get_qbo_tokens("CPA222", "Z9") is not None


# ---------------------------------------------------------------------------
# 6. /qbo/status content
# ---------------------------------------------------------------------------

class TestQboStatusPage:
    def test_qbo_status_shows_all_firm_clients(self, tmp_path, monkeypatch, qbo_oauth):
        mod, db = qbo_oauth
        mod.store_qbo_tokens("CPA111", "A1", "9123", "atk", "rtk", 3600)
        rd = _rd(tmp_path, monkeypatch, db)

        ctx = {"role": "firm_admin", "firm_code": "CPA111",
               "can_post_qbo": True}
        user = {"username": "alice", "role": "firm_admin",
                "firm_code": "CPA111", "language": "fr"}
        html = rd.render_qbo_status_page(ctx, user, lang="fr")
        # Connected client renders
        assert "A1" in html
        assert "9123" in html
        assert "Active" in html
        # Not-connected sibling client renders with a Connect button
        assert "B2" in html
        assert "Connect" in html
        # Out-of-firm client does NOT appear
        assert "Z9" not in html


# ---------------------------------------------------------------------------
# 7. Legacy migration warning
# ---------------------------------------------------------------------------

class TestLegacyMigration:
    def test_legacy_config_migration_adds_warning(self, tmp_path, monkeypatch, qbo_oauth):
        mod, db = qbo_oauth
        # Put a legacy config file on disk and no qbo_connections rows.
        legacy = tmp_path / "legacy_qbo.json"
        legacy.write_text('{"access_token":"x","realm_id":"9999"}')
        monkeypatch.setattr(mod, "CONFIG_PATH", legacy)
        assert mod.legacy_config_exists() is True

        rd = _rd(tmp_path, monkeypatch, db)
        # Re-import qbo_oauth inside review_dashboard's lookups so the
        # status page sees our patched CONFIG_PATH.
        monkeypatch.setattr(
            "src.agents.tools.qbo_oauth.CONFIG_PATH", legacy, raising=False
        )

        ctx = {"role": "firm_admin", "firm_code": "CPA111",
               "can_post_qbo": True}
        user = {"username": "alice", "role": "firm_admin",
                "firm_code": "CPA111", "language": "fr"}
        html = rd.render_qbo_status_page(ctx, user, lang="fr")
        assert "Legacy QuickBooks connection detected" in html


# ---------------------------------------------------------------------------
# 8. Adapter: per-client posting
# ---------------------------------------------------------------------------

def _seed_posting_job(db: Path, posting_id: str, client_code: str) -> None:
    conn = sqlite3.connect(str(db))
    payload = (
        '{"client_code":"' + client_code + '","entry_kind":"expense",'
        '"amount":10.0,"document_date":"2026-04-18","vendor":"ACME",'
        '"gl_account":"5440"}'
    )
    conn.execute(
        "INSERT INTO posting_jobs (posting_id, document_id, target_system, "
        "entry_kind, posting_status, approval_state, payload_json, "
        "created_at, updated_at) VALUES (?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
        (posting_id, "doc-1", "qbo", "expense", "ready_to_post",
         "approved_for_posting", payload),
    )
    conn.commit()
    conn.close()


class TestAdapterPerClient:
    def test_qbo_post_skips_document_without_connection(self, adapter):
        mod, db = adapter
        _seed_posting_job(db, "P-NO-CONN", "A1")  # no connection for CPA111/A1

        result = mod.post_one_ready_job("P-NO-CONN", db_path=db)
        assert result["status"] == "skipped_no_connection"
        assert "No QBO connection" in (result.get("error") or "")

        # Sprint C BUG #9 — the job is now marked 'skipped_no_connection'
        # rather than 'post_failed'. Missing-connection is a setup gap, not
        # a posting failure, so the dashboard can show a "Connect QBO"
        # prompt instead of a red failure badge.
        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT posting_status, error_text FROM posting_jobs "
            "WHERE posting_id='P-NO-CONN'"
        ).fetchone()
        conn.close()
        assert row[0] == "skipped_no_connection"
        assert "No QBO connection" in (row[1] or "")

    def test_qbo_post_uses_correct_realm_for_client(self, adapter, qbo_oauth):
        mod, db = adapter
        oauth, _ = qbo_oauth

        # Two clients, two different realms. Posting for A1 must hit realm A,
        # posting for B2 must hit realm B.
        oauth.store_qbo_tokens("CPA111", "A1", "realm-A", "tok-A", "rt-A", 3600)
        oauth.store_qbo_tokens("CPA111", "B2", "realm-B", "tok-B", "rt-B", 3600)

        _seed_posting_job(db, "P-A1", "A1")
        _seed_posting_job(db, "P-B2", "B2")

        calls = []

        def fake_post_json(*, url, access_token, payload):
            calls.append({"url": url, "token": access_token})
            return {"Purchase": {"Id": "ext-" + access_token}}

        # Minimal build_qbo_api_payload stub so we don't touch qbo_reference_cache
        fake_payload = {"Line": []}

        with patch.object(mod, "post_json", side_effect=fake_post_json), \
             patch.object(mod, "build_qbo_api_payload", return_value=fake_payload):
            r1 = mod.post_one_ready_job("P-A1", db_path=db)
            r2 = mod.post_one_ready_job("P-B2", db_path=db)

        assert r1["status"] == "posted"
        assert r2["status"] == "posted"
        assert "realm-A" in calls[0]["url"]
        assert calls[0]["token"] == "tok-A"
        assert "realm-B" in calls[1]["url"]
        assert calls[1]["token"] == "tok-B"

    def test_expired_token_auto_refresh_before_post(self, adapter, qbo_oauth):
        mod, db = adapter
        oauth, _ = qbo_oauth
        # Seed a token that's within the 5-minute refresh window so
        # get_qbo_tokens() will auto-refresh on the first lookup.
        oauth.store_qbo_tokens("CPA111", "A1", "realm-A", "stale", "rt", 60)

        with patch.object(oauth, "_post_to_intuit_token_endpoint",
                          return_value={"access_token": "fresh",
                                        "refresh_token": "rt-new",
                                        "expires_in": 3600}), \
             patch.object(mod, "post_json",
                          return_value={"Purchase": {"Id": "ext-1"}}), \
             patch.object(mod, "build_qbo_api_payload", return_value={"Line": []}):
            _seed_posting_job(db, "P-REFRESH", "A1")
            r = mod.post_one_ready_job("P-REFRESH", db_path=db)

        assert r["status"] == "posted"
        # The stored token is now the refreshed one
        tokens = oauth.get_qbo_tokens("CPA111", "A1")
        assert tokens["access_token"] == "fresh"

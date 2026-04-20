"""R4 Phase-1-2 — /ingest/openclaw now requires a per-firm API key.

Before: auth was sender-id lookup only. A forger who knew a valid
WhatsApp number could spoof an ingest. After: every firm gets a
per-firm secret minted at provisioning; `/ingest/openclaw` requires
an ``X-API-Key`` (or ``X-Openclaw-Key``) header that matches.
"""
from __future__ import annotations

import json
import secrets
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
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1,
            whatsapp_number TEXT);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "ingest.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "ia@det.com", "customer": "cus_IA",
             "subscription": "sub_IA", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        firm_row = conn.execute(
            "SELECT firm_code, ingest_api_key FROM firms "
            "WHERE stripe_customer_id='cus_IA'",
        ).fetchone()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "rd": rd, "db": db,
                "firm_code": firm_row["firm_code"],
                "api_key": firm_row["ingest_api_key"]}
    finally:
        server.shutdown(); server.server_close()


def _post_ingest(base, payload_obj: dict, *, api_key: str | None):
    body = json.dumps(payload_obj).encode()
    hdrs = {"Content-Type": "application/json"}
    if api_key is not None:
        hdrs["X-API-Key"] = api_key
    req = urllib.request.Request(
        f"{base}/ingest/openclaw", data=body, method="POST", headers=hdrs,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"_raw": body.decode("utf-8", errors="replace")[:200]}


# ---------------------------------------------------------------------------
# Positive path
# ---------------------------------------------------------------------------

def test_firm_provisioning_auto_generates_api_key(app):
    """Every new firm from Stripe checkout gets an ingest_api_key."""
    assert app["api_key"], "firm provisioned without an ingest_api_key"
    assert len(app["api_key"]) >= 30, (
        f"ingest_api_key looks too short: {app['api_key']!r}"
    )


def test_bootstrap_backfills_missing_keys(tmp_path, monkeypatch):
    """A firm that pre-existed without an ingest_api_key should have
    one backfilled on bootstrap_schema."""
    db = tmp_path / "bf.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    # Pre-seed a firm row without a key.
    with sqlite3.connect(str(db)) as conn:
        conn.execute("INSERT INTO firms (firm_code) VALUES ('LEGACY')")
        conn.commit()
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ingest_api_key FROM firms WHERE firm_code='LEGACY'"
        ).fetchone()
    assert row["ingest_api_key"], "legacy firm not backfilled with ingest_api_key"


def test_keys_unique_across_firms(tmp_path, monkeypatch):
    """Two firms should get different keys — not a single shared one."""
    db = tmp_path / "uniq.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "u1@det.com", "customer": "cus_U1",
             "subscription": "sub_U1", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
        rd._provision_firm_from_stripe(
            {"customer_email": "u2@det.com", "customer": "cus_U2",
             "subscription": "sub_U2", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT firm_code, ingest_api_key FROM firms "
            "WHERE stripe_customer_id IN ('cus_U1','cus_U2')",
        ).fetchall()
    keys = {r["ingest_api_key"] for r in rows}
    assert len(keys) == 2, f"keys duplicated: {keys}"


# ---------------------------------------------------------------------------
# HTTP behavior
# ---------------------------------------------------------------------------

def test_request_without_api_key_rejected(app):
    status, payload = _post_ingest(
        app["base"],
        {"platform": "whatsapp", "sender_id": "+14165550000",
         "media_type": "image/jpeg"},
        api_key=None,
    )
    assert status == 401, (status, payload)
    assert payload.get("error") == "invalid_or_missing_api_key"


def test_request_with_wrong_key_rejected(app):
    status, payload = _post_ingest(
        app["base"],
        {"platform": "whatsapp", "sender_id": "+14165550000",
         "media_type": "image/jpeg"},
        api_key="not-the-right-key-12345",
    )
    assert status == 401, (status, payload)


def test_request_with_correct_key_accepted_even_for_unknown_sender(app):
    """Key matches → auth OK. Unknown sender_id then gets a
    unknown_sender response (the original gate), but 401 is ruled out."""
    status, payload = _post_ingest(
        app["base"],
        {"platform": "whatsapp", "sender_id": "+14165550000",
         "media_type": "image/jpeg"},
        api_key=app["api_key"],
    )
    assert status != 401, (status, payload)
    # Unknown sender → engine returns status='unknown_sender'.
    assert payload.get("status") == "unknown_sender", payload


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------

def test_key_rotation_invalidates_old(app):
    rd = app["rd"]
    old_key = app["api_key"]
    new_key = rd._rotate_firm_ingest_key(app["firm_code"])
    assert new_key and new_key != old_key, new_key
    # Old key no longer works.
    status_old, _ = _post_ingest(
        app["base"],
        {"platform": "whatsapp", "sender_id": "+1555",
         "media_type": "image/jpeg"},
        api_key=old_key,
    )
    assert status_old == 401, "old key still accepted after rotation"
    # New key works.
    status_new, payload_new = _post_ingest(
        app["base"],
        {"platform": "whatsapp", "sender_id": "+1555",
         "media_type": "image/jpeg"},
        api_key=new_key,
    )
    assert status_new != 401, (status_new, payload_new)


def test_rotate_nonexistent_firm_returns_none(app):
    rd = app["rd"]
    result = rd._rotate_firm_ingest_key("NO-SUCH-FIRM")
    assert result is None

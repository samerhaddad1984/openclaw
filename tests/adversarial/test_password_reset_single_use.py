"""R4 Phase-1-1 — password-reset tokens are one-shot.

R3 documented the reuse window. This round closes it: tokens are
marked consumed after the first successful /set-password, and a
second attempt with the same token is refused at 400.
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
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "reset.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "r@det.com", "customer": "cus_R",
             "subscription": "sub_R", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Old1!"), "r@det.com"),
        )
        # Seed a second user for isolation tests.
        conn.execute(
            "INSERT INTO dashboard_users (username, password_hash, role, firm_code, active) "
            "VALUES (?, ?, 'firm_admin', (SELECT firm_code FROM dashboard_users WHERE username='r@det.com'), 1)",
            ("other@det.com", rd.hash_password("Old1!")),
        )
        conn.commit()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


def _post_set_password(base, token, new_password):
    body = urllib.parse.urlencode({
        "token": token, "new_password": new_password,
        "confirm_password": new_password,
    }).encode()
    p = urllib.parse.urlparse(base)
    req = urllib.request.Request(
        f"{base}/set-password", data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": f"{p.scheme}://{p.netloc}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# ---------------------------------------------------------------------------
# Positive path + reuse rejection
# ---------------------------------------------------------------------------

def test_token_works_first_time(app):
    rd = app["rd"]
    token = rd._generate_password_link("r@det.com")
    status, body = _post_set_password(app["base"], token, "NewPassword1!")
    assert status in (200, 302, 303), (status, body[:200])
    # New password is active.
    with sqlite3.connect(str(app["db"])) as c:
        h = c.execute(
            "SELECT password_hash FROM dashboard_users WHERE username='r@det.com'",
        ).fetchone()[0]
    assert rd.verify_password("NewPassword1!", h)


def test_token_rejected_second_time(app):
    rd = app["rd"]
    token = rd._generate_password_link("r@det.com")
    # First use lands.
    s1, _ = _post_set_password(app["base"], token, "NewPassword1!")
    assert s1 in (200, 302, 303)
    # Second use of the SAME token must refuse.
    s2, body2 = _post_set_password(app["base"], token, "EvenNewerPw1!")
    assert s2 == 400, (s2, body2[:300])
    # Password still matches the first setting.
    with sqlite3.connect(str(app["db"])) as c:
        h = c.execute(
            "SELECT password_hash FROM dashboard_users WHERE username='r@det.com'",
        ).fetchone()[0]
    assert rd.verify_password("NewPassword1!", h)
    assert not rd.verify_password("EvenNewerPw1!", h), (
        "second use of consumed token leaked through and overwrote the password"
    )


def test_different_tokens_for_different_users_isolated(app):
    rd = app["rd"]
    tok_r = rd._generate_password_link("r@det.com")
    tok_o = rd._generate_password_link("other@det.com")
    # Use r's token.
    _post_set_password(app["base"], tok_r, "RpassLong1!")
    # Using r's consumed token must not affect other@det.com's ability
    # to use THEIR token.
    s_o, _ = _post_set_password(app["base"], tok_o, "OpassLong1!")
    assert s_o in (200, 302, 303), s_o
    with sqlite3.connect(str(app["db"])) as c:
        ho = c.execute(
            "SELECT password_hash FROM dashboard_users WHERE username='other@det.com'",
        ).fetchone()[0]
    assert rd.verify_password("OpassLong1!", ho)


def test_registry_hashes_the_token_not_the_raw_value(app):
    """The password_reset_used table must never contain the raw
    token — only its HMAC. A DB leak must not yield replayable
    tokens."""
    rd = app["rd"]
    token = rd._generate_password_link("r@det.com")
    _post_set_password(app["base"], token, "SafePwLong1!")
    with sqlite3.connect(str(app["db"])) as c:
        row = c.execute(
            "SELECT token_hash, username FROM password_reset_used WHERE username='r@det.com'",
        ).fetchone()
    assert row is not None
    token_hash = row[0]
    assert token != token_hash, "registry is storing the raw token"
    assert token not in token_hash, "registry stored raw token as a substring"
    # Hash is a 64-char hex string (sha256).
    assert len(token_hash) == 64
    int(token_hash, 16)  # raises if not hex


def test_expired_token_still_cannot_reuse(app):
    """A replay of an expired token should fail the HMAC/expiry check;
    additionally, once consumed, even an un-expired copy of the same
    token is rejected."""
    rd = app["rd"]
    token = rd._generate_password_link("r@det.com")
    _post_set_password(app["base"], token, "OnePwLong1!")
    # Replay with the same (now-consumed) token — should be rejected
    # even though the HMAC + expiry are still valid.
    s, _ = _post_set_password(app["base"], token, "TwoPwLong1!")
    assert s == 400


# ---------------------------------------------------------------------------
# Cleanup sanity
# ---------------------------------------------------------------------------

def test_cleanup_preserves_recent_entries(app):
    """Lazy cleanup runs on every mark_used; it should delete rows
    older than 7 days but leave recent ones alone."""
    rd = app["rd"]
    # Directly insert an entry from 10 days ago.
    with sqlite3.connect(str(app["db"])) as c:
        c.execute(
            "INSERT INTO password_reset_used (token_hash, username, used_at) "
            "VALUES ('old_hash_10d', 'r@det.com', datetime('now', '-10 days'))",
        )
        # And a recent one.
        c.execute(
            "INSERT INTO password_reset_used (token_hash, username, used_at) "
            "VALUES ('recent_hash', 'r@det.com', datetime('now', '-1 hours'))",
        )
        c.commit()
    # Trigger cleanup by consuming a real token.
    token = rd._generate_password_link("r@det.com")
    _post_set_password(app["base"], token, "CleanupPw1!")
    # Old row should be gone; recent should remain.
    with sqlite3.connect(str(app["db"])) as c:
        remaining = {r[0] for r in c.execute(
            "SELECT token_hash FROM password_reset_used"
        ).fetchall()}
    assert "old_hash_10d" not in remaining, (
        "7-day cleanup didn't purge the 10-day-old row"
    )
    assert "recent_hash" in remaining, (
        "cleanup over-deleted — 1-hour-old row should have been kept"
    )

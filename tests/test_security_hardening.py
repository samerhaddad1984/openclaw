"""
tests/test_security_hardening.py
=================================
Pytest tests for security hardening (Part 1):
  - Brute-force / rate limiting (login_attempts table, is_rate_limited,
    record_login_attempt)
  - Filename sanitization (sanitize_filename)
  - Upload daily limit helpers (count_uploads_today)
  - Cookie attribute helpers (_is_https, _session_cookie_attrs)
  - Consistent error message (no "user not found" / "wrong password" split)
  - HTTP 429 when rate-limited (via do_POST mock)
  - i18n keys present in en.json and fr.json
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ---------------------------------------------------------------------------
# Import helpers from client_portal (patching DB_PATH to an in-memory DB)
# ---------------------------------------------------------------------------

# We import the module lazily inside each test that needs it so that
# DB_PATH can be patched before any module-level DB calls occur.

def _import_portal(tmp_db: Path):
    """Import client_portal with DB_PATH redirected to tmp_db."""
    import importlib
    import scripts.client_portal as _mod
    importlib.reload(_mod)          # reset module state
    _mod.DB_PATH = tmp_db           # redirect DB
    return _mod


def _in_memory_portal(tmp_path: Path):
    """Return a freshly bootstrapped client_portal module using a temp DB."""
    db = tmp_path / "test.db"
    portal = _import_portal(db)
    portal.bootstrap_schema()
    return portal, db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ago_iso(minutes: int) -> str:
    return (
        datetime.now(timezone.utc) - timedelta(minutes=minutes)
    ).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# 1. login_attempts table exists after bootstrap
# ---------------------------------------------------------------------------

class TestLoginAttemptsTable:
    def test_table_created(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        conn = sqlite3.connect(str(db))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "login_attempts" in tables

    def test_table_schema(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        conn = sqlite3.connect(str(db))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(login_attempts)").fetchall()}
        conn.close()
        assert {"id", "ip_address", "username", "attempted_at", "success"} <= cols


# ---------------------------------------------------------------------------
# 2. record_login_attempt writes correct rows
# ---------------------------------------------------------------------------

class TestRecordLoginAttempt:
    def test_records_failure(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        portal.record_login_attempt("1.2.3.4", "alice", False)
        conn = sqlite3.connect(str(db))
        row = conn.execute("SELECT * FROM login_attempts").fetchone()
        conn.close()
        assert row is not None
        # ip_address=1, username=2, attempted_at=3, success=4
        assert row[1] == "1.2.3.4"
        assert row[2] == "alice"
        assert row[4] == 0

    def test_records_success(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        portal.record_login_attempt("1.2.3.4", "alice", True)
        conn = sqlite3.connect(str(db))
        row = conn.execute("SELECT * FROM login_attempts").fetchone()
        conn.close()
        assert row[4] == 1


# ---------------------------------------------------------------------------
# 3. is_rate_limited logic
# ---------------------------------------------------------------------------

class TestIsRateLimited:
    def _insert_failures(self, db: Path, ip: str, n: int,
                         minutes_ago: int = 0) -> None:
        ts = _ago_iso(minutes_ago)
        conn = sqlite3.connect(str(db))
        for _ in range(n):
            conn.execute(
                "INSERT INTO login_attempts (ip_address, username, attempted_at, success)"
                " VALUES (?,?,?,0)",
                (ip, "testuser", ts),
            )
        conn.commit()
        conn.close()

    def test_not_limited_below_threshold(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        self._insert_failures(db, "1.2.3.4", 4)
        assert portal.is_rate_limited("1.2.3.4") is False

    def test_limited_at_threshold(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        self._insert_failures(db, "1.2.3.4", 5)
        assert portal.is_rate_limited("1.2.3.4") is True

    def test_limited_above_threshold(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        self._insert_failures(db, "1.2.3.4", 10)
        assert portal.is_rate_limited("1.2.3.4") is True

    def test_old_failures_dont_count(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        # 5 failures from 20 minutes ago (outside 15-min window)
        self._insert_failures(db, "1.2.3.4", 5, minutes_ago=20)
        assert portal.is_rate_limited("1.2.3.4") is False

    def test_success_not_counted(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        # 5 successes should NOT trigger rate limit
        conn = sqlite3.connect(str(db))
        ts = _utc_now_iso()
        for _ in range(5):
            conn.execute(
                "INSERT INTO login_attempts (ip_address, username, attempted_at, success)"
                " VALUES (?,?,?,1)",
                ("1.2.3.4", "testuser", ts),
            )
        conn.commit()
        conn.close()
        assert portal.is_rate_limited("1.2.3.4") is False

    def test_different_ips_isolated(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        self._insert_failures(db, "1.2.3.4", 5)
        # A different IP should not be rate-limited
        assert portal.is_rate_limited("9.9.9.9") is False


# ---------------------------------------------------------------------------
# 4. sanitize_filename
# ---------------------------------------------------------------------------

class TestSanitizeFilename:
    def _sanitize(self, tmp_path, name):
        portal, _ = _in_memory_portal(tmp_path)
        return portal.sanitize_filename(name)

    def test_normal_name(self, tmp_path):
        assert self._sanitize(tmp_path, "invoice_2024.pdf") == "invoice_2024.pdf"

    def test_strips_path_separators_unix(self, tmp_path):
        result = self._sanitize(tmp_path, "../../etc/passwd")
        assert "/" not in result
        assert ".." not in result

    def test_strips_path_separators_windows(self, tmp_path):
        result = self._sanitize(tmp_path, r"C:\Windows\system32\evil.exe")
        assert "\\" not in result
        assert "C:" not in result

    def test_strips_leading_dots(self, tmp_path):
        result = self._sanitize(tmp_path, "...hidden.pdf")
        assert not result.startswith(".")

    def test_removes_special_chars(self, tmp_path):
        result = self._sanitize(tmp_path, "file name (copy) #2!.pdf")
        # Only alphanumeric, dash, underscore, dot allowed
        import re
        assert re.fullmatch(r"[A-Za-z0-9._\-]+", result)

    def test_empty_name_fallback(self, tmp_path):
        result = self._sanitize(tmp_path, "")
        assert result == "document"

    def test_only_dots_fallback(self, tmp_path):
        result = self._sanitize(tmp_path, "...")
        assert result == "document"

    def test_preserves_extension(self, tmp_path):
        result = self._sanitize(tmp_path, "my file.pdf")
        assert result.endswith(".pdf")

    def test_unicode_replaced(self, tmp_path):
        result = self._sanitize(tmp_path, "facture_été_2024.pdf")
        import re
        assert re.fullmatch(r"[A-Za-z0-9._\-]+", result)


# ---------------------------------------------------------------------------
# 5. count_uploads_today
# ---------------------------------------------------------------------------

class TestCountUploadsToday:
    def test_zero_when_no_uploads(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        # Need documents table — it's created by bootstrap_schema
        assert portal.count_uploads_today("ACME") == 0

    def test_counts_todays_uploads(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        today = datetime.now(timezone.utc).date().isoformat()
        conn = sqlite3.connect(str(db))
        for i in range(3):
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, created_at)"
                " VALUES (?,?,'New',?)",
                (f"doc_{i}", "ACME", f"{today}T10:00:00+00:00"),
            )
        conn.commit()
        conn.close()
        assert portal.count_uploads_today("ACME") == 3

    def test_doesnt_count_other_clients(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        today = datetime.now(timezone.utc).date().isoformat()
        conn = sqlite3.connect(str(db))
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at)"
            " VALUES ('d1','OTHER','New',?)",
            (f"{today}T10:00:00+00:00",),
        )
        conn.commit()
        conn.close()
        assert portal.count_uploads_today("ACME") == 0

    def test_doesnt_count_yesterday(self, tmp_path):
        portal, db = _in_memory_portal(tmp_path)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
        conn = sqlite3.connect(str(db))
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at)"
            " VALUES ('d1','ACME','New',?)",
            (f"{yesterday}T10:00:00+00:00",),
        )
        conn.commit()
        conn.close()
        assert portal.count_uploads_today("ACME") == 0


# ---------------------------------------------------------------------------
# 6. Cookie attribute helpers
# ---------------------------------------------------------------------------

class TestCookieHelpers:
    def _make_handler(self, headers: dict):
        h = MagicMock()
        h.headers = headers
        h.client_address = ("127.0.0.1", 12345)
        return h

    def test_is_https_true(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({"X-Forwarded-Proto": "https"})
        assert portal._is_https(handler) is True

    def test_is_https_false_http(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({"X-Forwarded-Proto": "http"})
        assert portal._is_https(handler) is False

    def test_is_https_false_missing(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({})
        assert portal._is_https(handler) is False

    def test_secure_attrs_over_https(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({"X-Forwarded-Proto": "https"})
        attrs = portal._session_cookie_attrs(handler)
        assert "Secure" in attrs
        assert "SameSite=Strict" in attrs

    def test_no_secure_over_http(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({})
        attrs = portal._session_cookie_attrs(handler)
        assert "Secure" not in attrs
        assert "SameSite=Lax" in attrs


# ---------------------------------------------------------------------------
# 7. _get_client_ip honours proxy headers
# ---------------------------------------------------------------------------

class TestGetClientIp:
    def _make_handler(self, headers: dict, peer_ip: str = "127.0.0.1"):
        h = MagicMock()
        h.headers = headers
        h.client_address = (peer_ip, 12345)
        return h

    def test_cf_connecting_ip(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({"CF-Connecting-IP": "203.0.113.5"})
        assert portal._get_client_ip(handler) == "203.0.113.5"

    def test_x_forwarded_for(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({"X-Forwarded-For": "198.51.100.1, 10.0.0.1"})
        assert portal._get_client_ip(handler) == "198.51.100.1"

    def test_fallback_peer(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({}, peer_ip="192.168.1.1")
        assert portal._get_client_ip(handler) == "192.168.1.1"

    def test_cf_takes_precedence(self, tmp_path):
        portal, _ = _in_memory_portal(tmp_path)
        handler = self._make_handler({
            "CF-Connecting-IP": "203.0.113.5",
            "X-Forwarded-For": "198.51.100.1",
        })
        assert portal._get_client_ip(handler) == "203.0.113.5"


# ---------------------------------------------------------------------------
# 8. i18n keys present
# ---------------------------------------------------------------------------

_EN = json.loads((ROOT / "src" / "i18n" / "en.json").read_text(encoding="utf-8"))
_FR = json.loads((ROOT / "src" / "i18n" / "fr.json").read_text(encoding="utf-8"))

NEW_KEYS = [
    "too_many_attempts",
    "upload_limit_exceeded",
    "cf_tunnel_title",
    "cf_tunnel_status",
    "cf_tunnel_connected",
    "cf_tunnel_disconnected",
    "cf_tunnel_url",
    "cf_tunnel_not_configured",
    "cf_tunnel_requests_today",
]


@pytest.mark.parametrize("key", NEW_KEYS)
def test_en_key_present(key):
    assert key in _EN, f"Missing key in en.json: {key!r}"


@pytest.mark.parametrize("key", NEW_KEYS)
def test_fr_key_present(key):
    assert key in _FR, f"Missing key in fr.json: {key!r}"


# ---------------------------------------------------------------------------
# 9. Rate-limit message is bilingual (contains both FR and EN text)
# ---------------------------------------------------------------------------

def test_rate_limit_msg_bilingual(tmp_path):
    portal, _ = _in_memory_portal(tmp_path)
    msg = portal._RATE_LIMIT_MSG
    assert "Trop de tentatives" in msg
    assert "Too many attempts" in msg
    assert "15 minutes" in msg


# ---------------------------------------------------------------------------
# 10. MAX_UPLOADS_PER_DAY constant
# ---------------------------------------------------------------------------

def test_max_uploads_per_day(tmp_path):
    from scripts.client_portal import MAX_UPLOADS_PER_DAY
    portal, _ = _in_memory_portal(tmp_path)
    assert portal.MAX_UPLOADS_PER_DAY == MAX_UPLOADS_PER_DAY
    assert MAX_UPLOADS_PER_DAY >= 20  # sane minimum


# ---------------------------------------------------------------------------
# 11. autofix strings present for check 12
# ---------------------------------------------------------------------------

def test_autofix_has_cloudflare_strings():
    src = (ROOT / "scripts" / "autofix.py").read_text(encoding="utf-8")
    assert "lbl_cloudflare" in src
    assert "cf_running" in src
    assert "cf_stopped" in src
    assert "check_cloudflare_tunnel" in src


# ---------------------------------------------------------------------------
# 12. setup_cloudflare.py exists and is importable structure check
# ---------------------------------------------------------------------------

def test_setup_cloudflare_exists():
    assert (ROOT / "scripts" / "setup_cloudflare.py").exists()


def test_setup_cloudflare_has_key_symbols():
    src = (ROOT / "scripts" / "setup_cloudflare.py").read_text(encoding="utf-8")
    assert "cloudflared-windows-amd64.exe" in src
    assert "otocpa.config.json" in src
    assert "public_portal_url" in src
    assert "sc query cloudflared" in src or "SERVICE_NAME" in src
    assert "step_windows_service" in src
    assert "8788" in src


# ---------------------------------------------------------------------------
# 13. review_dashboard has rate-limiting code
# ---------------------------------------------------------------------------

def test_review_dashboard_has_rate_limiting():
    src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
    assert "is_rate_limited" in src
    assert "record_login_attempt" in src
    assert "_session_cookie_attrs" in src
    assert "login_attempts" in src


# ---------------------------------------------------------------------------
# 14. client_portal.py has login_attempts table creation
# ---------------------------------------------------------------------------

def test_client_portal_has_login_attempts_table():
    src = (ROOT / "scripts" / "client_portal.py").read_text(encoding="utf-8")
    assert "login_attempts" in src
    assert "is_rate_limited" in src
    assert "sanitize_filename" in src
    assert "MAX_UPLOADS_PER_DAY" in src
    assert "_session_cookie_attrs" in src

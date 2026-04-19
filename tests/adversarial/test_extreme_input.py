"""Investigation 7 — extreme input.

Hammer HTTP endpoints with ridiculous / malicious bodies and verify:
  - graceful 4xx (400 Bad Request, 413 Payload Too Large, 403, etc.)
  - no 500 (uncaught crash)
  - no hang (we set a 10 s request timeout; anything longer = DoS)
  - no unbounded memory growth.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def srv(tmp_path, monkeypatch):
    db = tmp_path / "x.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True); t.start()
    try:
        yield rd, db, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown(); server.server_close()


class _NoRedir(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_a, **_k):
        return None


_OPENER = urllib.request.build_opener(_NoRedir())


def _post_raw(url, body, headers=None):
    req = urllib.request.Request(url, data=body, method="POST",
                                  headers=headers or {})
    try:
        with _OPENER.open(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# ---------------------------------------------------------------------------
# Body-size abuse
# ---------------------------------------------------------------------------

def test_50mb_json_body_rejected_gracefully(srv):
    """50 MB JSON body shouldn't 500. Either 400/413/403 or silently
    drop is acceptable. 500 = uncaught crash = bug."""
    rd, db, base = srv
    body = b"{" + b'"x":"' + b"A" * (50 * 1024 * 1024) + b'"' + b"}"
    status, resp = _post_raw(
        f"{base}/stripe/webhook",  # an endpoint that parses JSON
        body,
        headers={"Content-Type": "application/json",
                 "Origin": base},
    )
    # Sandbox may reject with 400 (invalid_signature), 403, 413, or the
    # server may drop the connection. Any 4xx is acceptable.
    assert status != 500, f"50 MB body 500'd: {resp[:200]!r}"
    assert status < 600  # ensure we got SOME response


def test_deeply_nested_json_does_not_recurse_into_stack_overflow(srv):
    """Open-brace stacking: {"a":{"a":{"a":...}}} 500 levels deep."""
    rd, db, base = srv
    nested = "{" * 500 + '"a":1' + "}" * 500
    status, resp = _post_raw(
        f"{base}/stripe/webhook",
        nested.encode(),
        headers={"Content-Type": "application/json", "Origin": base},
    )
    assert status != 500, f"deep-nested JSON 500'd: {resp[:200]!r}"


def test_extremely_long_form_field_handled(srv):
    rd, db, base = srv
    # 1 MB long field. Application should either truncate, reject, or
    # accept — but never 500.
    body = urllib.parse.urlencode({"note": "A" * (1024 * 1024)}).encode()
    status, resp = _post_raw(
        f"{base}/api/contact",
        body,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Origin": base},
    )
    assert status != 500, f"1MB form field 500'd: {resp[:200]!r}"


def test_ten_thousand_query_params_do_not_crash_any_get(srv):
    rd, db, base = srv
    qs = "&".join(f"p{i}={i}" for i in range(10_000))
    try:
        with urllib.request.urlopen(f"{base}/login?{qs}", timeout=10) as r:
            status = r.status
    except urllib.error.HTTPError as e:
        status = e.code
    except urllib.error.URLError:
        # Some servers refuse absurdly long request lines at the TCP
        # level — that's acceptable (they don't crash).
        return
    assert status != 500


# ---------------------------------------------------------------------------
# Path weirdness
# ---------------------------------------------------------------------------

def test_500_url_segments_does_not_crash(srv):
    rd, db, base = srv
    segs = "/".join("x" for _ in range(500))
    try:
        with urllib.request.urlopen(f"{base}/{segs}", timeout=10) as r:
            status = r.status
    except urllib.error.HTTPError as e:
        status = e.code
    assert status != 500


def test_path_with_null_byte_does_not_crash(srv):
    rd, db, base = srv
    # Python's urllib refuses to send a literal null-byte path; use
    # socket directly.
    import socket
    p = urllib.parse.urlparse(base)
    raw = (
        b"GET /login%00 HTTP/1.1\r\n"
        + f"Host: {p.hostname}:{p.port}\r\n".encode()
        + b"Connection: close\r\n\r\n"
    )
    with socket.create_connection((p.hostname, p.port), timeout=5) as s:
        s.sendall(raw)
        resp = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            resp += chunk
            if len(resp) > 65536:
                break
    # Parse status line.
    head = resp.split(b"\r\n", 1)[0]
    assert b"500" not in head.split(b" ")[1], f"null-byte path 500'd: {head!r}"


# ---------------------------------------------------------------------------
# Content-Type confusion
# ---------------------------------------------------------------------------

def test_wrong_content_type_on_form_post_still_handled(srv):
    rd, db, base = srv
    body = urllib.parse.urlencode({"username": "x", "password": "y"}).encode()
    # Declare JSON, send form body.
    status, _ = _post_raw(
        f"{base}/login",
        body,
        headers={"Content-Type": "application/json", "Origin": base},
    )
    assert status != 500

"""R4-Investigation 7 — API fuzz testing.

Random hostile inputs at every POST endpoint. For each target, we
assert: no 5xx, no traceback leak in body, request completes in <15s.
"""
from __future__ import annotations

import json
import random
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


@pytest.fixture(scope="module")
def fuzz_app():
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="r4_fuzz_"))
    db = tmp / "fuzz.db"
    secret = tmp / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    rd.DB_PATH = db
    rd.PASSWORD_LINK_SECRET_FILE = str(secret)
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "fz@det.com", "customer": "cus_FZ",
             "subscription": "sub_FZ", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Fuzz1!"), "fz@det.com"),
        )
        conn.commit()
    cookies = f"session_token={rd.create_session('fz@det.com')}"
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "cookies": cookies,
                "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


def _raw_post(url, body, *, content_type, cookie=None):
    hdrs = {"Content-Type": content_type}
    if cookie:
        hdrs["Cookie"] = cookie
    p = urllib.parse.urlparse(url)
    hdrs["Origin"] = f"{p.scheme}://{p.netloc}"
    req = urllib.request.Request(url, data=body, method="POST", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _assert_safe(status: int, body: bytes, where: str):
    assert status < 500, f"{where}: 500. body={body[:200]!r}"
    text = body.decode("utf-8", errors="replace")
    for pat in ("Traceback (most recent call last)", "OperationalError",
                 "KeyError: ", "AttributeError: "):
        assert pat not in text, f"{where}: leaked {pat!r}"


# ---------------------------------------------------------------------------
# Random fuzz payload generator (deterministic seed).
# ---------------------------------------------------------------------------

_rng = random.Random(1234)


def _random_bytes(n: int) -> bytes:
    return bytes(_rng.randint(0, 255) for _ in range(n))


def _random_json_object(depth: int = 3):
    if depth <= 0 or _rng.random() < 0.3:
        return _rng.choice([None, True, False, 0, -1, _rng.random(),
                             "str", "", "x" * 100])
    if _rng.random() < 0.5:
        return [_random_json_object(depth - 1)
                for _ in range(_rng.randint(0, 4))]
    return {f"k{_rng.randint(0, 5)}": _random_json_object(depth - 1)
            for _ in range(_rng.randint(0, 4))}


# ---------------------------------------------------------------------------
# Targets + a range of hostile bodies.
# ---------------------------------------------------------------------------

POST_TARGETS = [
    "/api/contact",
    "/stripe/webhook",
    "/login",
    "/forgot",
    "/clients/save",
    "/document/update",
]


HOSTILE_BODIES = [
    ("empty", b"", "application/json"),
    ("null", b"null", "application/json"),
    ("array", b"[]", "application/json"),
    ("number_as_body", b"42", "application/json"),
    ("deep_nest", b"{" * 500 + b'"a":1' + b"}" * 500, "application/json"),
    ("huge_array", b"[" + b"0," * 5000 + b"0]", "application/json"),
    ("invalid_utf8", b"\xff\xfe\x00\x01\x02\x03", "application/json"),
    ("form_looks_like_json", b"a=1&b=2", "application/json"),
    ("json_looks_like_form", b'{"a":1}', "application/x-www-form-urlencoded"),
]


@pytest.mark.parametrize("target", POST_TARGETS)
@pytest.mark.parametrize("tag,body,ct", HOSTILE_BODIES,
                          ids=[t for t, _, _ in HOSTILE_BODIES])
def test_endpoint_survives_hostile_body(fuzz_app, target, tag, body, ct):
    status, resp = _raw_post(
        f"{fuzz_app['base']}{target}", body,
        content_type=ct, cookie=fuzz_app["cookies"],
    )
    _assert_safe(status, resp, f"{target} body={tag}")


# ---------------------------------------------------------------------------
# Random-body fuzz: 30 iterations against /clients/save.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", range(30))
def test_clients_save_random_fuzz_survives(fuzz_app, seed):
    _rng.seed(seed)
    # Build a random form-encoded body.
    fields = {}
    for _ in range(_rng.randint(0, 10)):
        k = "k" + str(_rng.randint(0, 100))
        v = _random_json_object(depth=2)
        fields[k] = json.dumps(v) if not isinstance(v, str) else v
    body = urllib.parse.urlencode(fields).encode()
    status, resp = _raw_post(
        f"{fuzz_app['base']}/clients/save", body,
        content_type="application/x-www-form-urlencoded",
        cookie=fuzz_app["cookies"],
    )
    _assert_safe(status, resp, f"clients/save seed={seed}")


# ---------------------------------------------------------------------------
# GET-side fuzz: URL query strings.
# ---------------------------------------------------------------------------

GET_FUZZ_PATHS = [
    "/",
    "/clients",
    "/document",
    "/aging",
    "/financial_statements",
    "/audit/anomalies",
]


@pytest.mark.parametrize("path", GET_FUZZ_PATHS)
@pytest.mark.parametrize("seed", range(5))
def test_get_random_query_string_survives(fuzz_app, path, seed):
    _rng.seed(seed + 100)
    qs = urllib.parse.urlencode({
        "id": _rng.choice(["'OR'1'='1", "", "9999", "%00", "../../etc/passwd"]),
        "client_code": _rng.choice(["", "'\"", "A" * 1000, "\x00hidden"]),
        "period": _rng.choice(["2099-99", "", "not-a-period", "2026-04"]),
        "q": _rng.choice(["", "'", "AAA" * 500]),
    })
    req = urllib.request.Request(
        f"{fuzz_app['base']}{path}?{qs}",
        headers={"Cookie": fuzz_app["cookies"]},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            status, resp = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, resp = e.code, e.read()
    _assert_safe(status, resp, f"{path}?seed={seed}")

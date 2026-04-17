"""Sprint 1 — multi-tenant isolation and firm_admin role tests.

These tests exercise the permission helpers and route-level gates added in
review_dashboard.py. They do NOT boot a real HTTP server — they either call
helpers directly with a ctx dict, or simulate a do_POST by constructing a
handler with a mock wfile/headers and pointing the module's DB_PATH at a
temp SQLite file.
"""
from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from io import BytesIO
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dash(tmp_path, monkeypatch):
    """Import review_dashboard with DB_PATH redirected to a temp DB.

    Always reloads the module so the Sprint 1 helpers are present even if
    an earlier test imported the module before this one; uses monkeypatch
    so DB_PATH is restored at teardown, preventing bleed into tests that
    touch the real DB via the same module instance.
    """
    mod = importlib.import_module("scripts.review_dashboard")
    if not hasattr(mod, "_require_client_in_firm"):
        importlib.reload(mod)
    tmp_db = tmp_path / "otocpa_isolation.db"
    monkeypatch.setattr(mod, "DB_PATH", tmp_db)
    # Create minimum schema needed by helpers + routes under test
    conn = sqlite3.connect(str(tmp_db))
    conn.executescript(
        """
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY,
            client_name TEXT,
            active INTEGER DEFAULT 1,
            firm_code TEXT
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            vendor TEXT,
            review_status TEXT
        );
        CREATE TABLE firms (
            firm_code TEXT PRIMARY KEY,
            firm_name TEXT NOT NULL
        );
        CREATE TABLE dashboard_users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'employee',
            display_name TEXT,
            active INTEGER DEFAULT 1,
            language TEXT DEFAULT 'fr',
            firm_code TEXT
        );
        """
    )
    # Two firms, two clients each, one document each
    conn.executemany(
        "INSERT INTO firms (firm_code, firm_name) VALUES (?, ?)",
        [("FIRM_A", "Firm A"), ("FIRM_B", "Firm B"), ("OWNER", "HQ")],
    )
    conn.executemany(
        "INSERT INTO clients (client_code, client_name, firm_code, active) VALUES (?, ?, ?, 1)",
        [
            ("A_CLIENT1", "A Client 1", "FIRM_A"),
            ("A_CLIENT2", "A Client 2", "FIRM_A"),
            ("B_CLIENT1", "B Client 1", "FIRM_B"),
        ],
    )
    conn.executemany(
        "INSERT INTO documents (document_id, client_code, vendor, review_status) VALUES (?, ?, ?, 'Needs Review')",
        [
            ("DOC_A1", "A_CLIENT1", "VendorA1"),
            ("DOC_A2", "A_CLIENT2", "VendorA2"),
            ("DOC_B1", "B_CLIENT1", "VendorB1"),
        ],
    )
    conn.commit()
    conn.close()
    return mod


def ctx_for(dash, role: str, firm: str, username: str = "u") -> dict:
    """Build a user context dict as build_user_context would."""
    user = {
        "username": username,
        "display_name": username,
        "role": role,
        "firm_code": firm,
        "active": 1,
        "language": "fr",
    }
    return dash.build_user_context(user)


# ---------------------------------------------------------------------------
# 1. _require_client_in_firm
# ---------------------------------------------------------------------------

def test_firm_a_user_cannot_read_firm_b_client(dash):
    """A firm_admin for FIRM_A must NOT access B_CLIENT1."""
    ctx = ctx_for(dash, "firm_admin", "FIRM_A")
    assert dash._require_client_in_firm("B_CLIENT1", ctx) is False
    assert dash._require_client_in_firm("A_CLIENT1", ctx) is True


def test_manager_from_firm_a_cannot_read_firm_b_client(dash):
    """A manager belongs to a single firm; same isolation rule applies."""
    ctx = ctx_for(dash, "manager", "FIRM_A")
    assert dash._require_client_in_firm("B_CLIENT1", ctx) is False


def test_owner_can_read_any_client(dash):
    ctx = ctx_for(dash, "owner", "OWNER")
    assert dash._require_client_in_firm("A_CLIENT1", ctx) is True
    assert dash._require_client_in_firm("B_CLIENT1", ctx) is True


def test_unknown_client_rejected(dash):
    ctx = ctx_for(dash, "firm_admin", "FIRM_A")
    assert dash._require_client_in_firm("NOPE", ctx) is False
    assert dash._require_client_in_firm("", ctx) is False
    assert dash._require_client_in_firm(None, ctx) is False


# ---------------------------------------------------------------------------
# 2. _require_document_in_firm
# ---------------------------------------------------------------------------

def test_firm_a_user_cannot_mutate_firm_b_document(dash):
    ctx = ctx_for(dash, "firm_admin", "FIRM_A")
    assert dash._require_document_in_firm("DOC_B1", ctx) is False
    assert dash._require_document_in_firm("DOC_A1", ctx) is True


def test_owner_can_mutate_any_document(dash):
    ctx = ctx_for(dash, "owner", "OWNER")
    assert dash._require_document_in_firm("DOC_A1", ctx) is True
    assert dash._require_document_in_firm("DOC_B1", ctx) is True


# ---------------------------------------------------------------------------
# 3. Role config capabilities
# ---------------------------------------------------------------------------

def test_employee_cannot_post_qbo(dash):
    ctx = ctx_for(dash, "employee", "FIRM_A")
    assert dash._can_do(ctx, "post_qbo") is False
    assert dash._can_do(ctx, "view_all_clients") is False


def test_manager_can_post_qbo_but_not_manage_users(dash):
    ctx = ctx_for(dash, "manager", "FIRM_A")
    assert dash._can_do(ctx, "post_qbo") is True
    assert dash._can_do(ctx, "manage_users") is False


def test_firm_admin_gets_manage_users_cap(dash):
    ctx = ctx_for(dash, "firm_admin", "FIRM_A")
    assert dash._can_do(ctx, "manage_users") is True
    assert dash._can_do(ctx, "view_all_clients") is True
    assert dash._can_do(ctx, "export") is True


def test_owner_has_every_capability(dash):
    ctx = ctx_for(dash, "owner", "OWNER")
    for cap in [
        "view_all_clients", "view_all_assignments", "assign", "post",
        "manage_team", "manage_users", "post_qbo",
        "manage_journal_entries", "export",
    ]:
        assert dash._can_do(ctx, cap) is True, f"owner should have {cap}"


# ---------------------------------------------------------------------------
# 4. Role validation at insert time
# ---------------------------------------------------------------------------

def test_role_validation_accepts_known_roles(dash):
    for r in ("owner", "firm_admin", "manager", "employee"):
        assert dash.validate_role(r) == r


def test_role_validation_rejects_invalid(dash):
    for bad in ("god", "admin", "root", "super", "", None):
        with pytest.raises(ValueError):
            dash.validate_role(bad)


# ---------------------------------------------------------------------------
# 5. Export route denies employee
# ---------------------------------------------------------------------------

def test_employee_cannot_export_firm_data(dash):
    """Employees don't have the 'can_export' capability."""
    ctx = ctx_for(dash, "employee", "FIRM_A")
    assert dash._can_do(ctx, "export") is False


def test_firm_a_user_cannot_export_firm_b_data(dash):
    """Cross-firm export: the helper must reject FIRM_B's client."""
    ctx = ctx_for(dash, "firm_admin", "FIRM_A")
    assert dash._require_client_in_firm("B_CLIENT1", ctx) is False


# ---------------------------------------------------------------------------
# 6. _get_qr_clients scoping
# ---------------------------------------------------------------------------

def test_qr_download_scoped_to_firm(dash):
    ctx_a = ctx_for(dash, "firm_admin", "FIRM_A")
    ctx_b = ctx_for(dash, "firm_admin", "FIRM_B")
    ctx_owner = ctx_for(dash, "owner", "OWNER")

    codes_a = {c["client_code"] for c in dash._get_qr_clients(ctx_a)}
    codes_b = {c["client_code"] for c in dash._get_qr_clients(ctx_b)}
    codes_owner = {c["client_code"] for c in dash._get_qr_clients(ctx_owner)}

    assert codes_a == {"A_CLIENT1", "A_CLIENT2"}
    assert codes_b == {"B_CLIENT1"}
    assert codes_owner == {"A_CLIENT1", "A_CLIENT2", "B_CLIENT1"}
    # Passing no ctx returns empty — callers must scope
    assert dash._get_qr_clients(None) == []


# ---------------------------------------------------------------------------
# 7. Public upload rate limit
# ---------------------------------------------------------------------------

def test_public_upload_rate_limited(dash):
    # Reset bucket
    dash._public_upload_log.clear()
    for _ in range(dash._PUBLIC_UPLOAD_MAX_PER_MIN):
        assert dash._public_upload_allowed("ACME", "9.9.9.9") is True
    # 21st call in the same minute must be rejected
    assert dash._public_upload_allowed("ACME", "9.9.9.9") is False
    # Different (client, ip) has its own bucket
    assert dash._public_upload_allowed("OTHER", "9.9.9.9") is True
    assert dash._public_upload_allowed("ACME", "1.1.1.1") is True


# ---------------------------------------------------------------------------
# 8. render_user_management scoping
# ---------------------------------------------------------------------------

def test_firm_admin_can_access_users_of_their_firm(dash):
    # Add users for both firms
    with dash.open_db() as conn:
        conn.executemany(
            "INSERT INTO dashboard_users (username, password_hash, role, firm_code) "
            "VALUES (?, 'x', ?, ?)",
            [
                ("alice", "firm_admin", "FIRM_A"),
                ("bob",   "employee",   "FIRM_A"),
                ("carol", "firm_admin", "FIRM_B"),
            ],
        )
        conn.commit()
    ctx = ctx_for(dash, "firm_admin", "FIRM_A", username="alice")
    user = {"username": "alice", "role": "firm_admin", "firm_code": "FIRM_A",
            "display_name": "Alice", "language": "fr"}
    html_out = dash.render_user_management(ctx, user, "", "", lang="en")
    assert "alice" in html_out
    assert "bob" in html_out
    # Scope check: carol (FIRM_B) must not appear in FIRM_A's user list
    assert "carol" not in html_out


def test_firm_admin_cannot_see_other_firms_users(dash):
    with dash.open_db() as conn:
        conn.executemany(
            "INSERT INTO dashboard_users (username, password_hash, role, firm_code) "
            "VALUES (?, 'x', ?, ?)",
            [
                ("alice", "firm_admin", "FIRM_A"),
                ("dave",  "manager",    "FIRM_B"),
            ],
        )
        conn.commit()
    ctx = ctx_for(dash, "firm_admin", "FIRM_A", username="alice")
    user = {"username": "alice", "role": "firm_admin", "firm_code": "FIRM_A",
            "display_name": "Alice", "language": "fr"}
    html_out = dash.render_user_management(ctx, user, "", "", lang="en")
    assert "dave" not in html_out


def test_manager_cannot_access_users_page(dash):
    """Managers may manage their team's portfolios but NOT dashboard accounts."""
    ctx = ctx_for(dash, "manager", "FIRM_A")
    user = {"username": "m", "role": "manager", "firm_code": "FIRM_A",
            "display_name": "Mary", "language": "fr"}
    html_out = dash.render_user_management(ctx, user, "", "", lang="en")
    # The page_layout fallback renders err_access_denied heading; the user
    # table should NOT appear.
    assert "Add user" not in html_out
    assert "col_password_header" not in html_out


# ---------------------------------------------------------------------------
# 9. _forbid writes a valid 403 JSON response
# ---------------------------------------------------------------------------

class _FakeHandler:
    def __init__(self):
        self.status = None
        self.headers_set = []
        self.wfile = BytesIO()
        self.ended = False

    def send_response(self, code):
        self.status = code

    def send_header(self, k, v):
        self.headers_set.append((k, v))

    def end_headers(self):
        self.ended = True


def test_forbid_writes_403_json(dash):
    h = _FakeHandler()
    dash._forbid(h, "nope")
    assert h.status == 403
    assert h.ended is True
    assert ("Content-Type", "application/json") in h.headers_set
    body = json.loads(h.wfile.getvalue().decode("utf-8"))
    assert body == {"error": "nope"}

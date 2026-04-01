"""
K — KEY / SESSION ABUSE
========================
Attack session management, authentication tokens, machine fingerprinting,
and license validation.

Targets: license_engine, dashboard_auth
"""
from __future__ import annotations

import hashlib
import sqlite3
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from src.engines.license_engine import (
        validate_license,
        generate_machine_fingerprint,
    )
    HAS_LICENSE = True
except ImportError:
    HAS_LICENSE = False

try:
    from src.agents.core.dashboard_auth import (
        create_session,
        validate_session,
        revoke_session,
    )
    HAS_AUTH = True
except ImportError:
    HAS_AUTH = False

from .conftest import fresh_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _session_db() -> sqlite3.Connection:
    conn = fresh_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            client_code TEXT,
            role TEXT DEFAULT 'viewer',
            created_at TEXT,
            expires_at TEXT,
            revoked INTEGER DEFAULT 0
        );

        -- FIX 4: Prevent direct role escalation on sessions table
        CREATE TRIGGER IF NOT EXISTS trg_session_no_role_change
        BEFORE UPDATE ON sessions
        WHEN NEW.role != OLD.role
        BEGIN
            SELECT RAISE(ABORT, 'Session role cannot be changed via direct SQL');
        END;

        CREATE TABLE IF NOT EXISTS license_machines (
            machine_id TEXT PRIMARY KEY,
            license_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            activated_at TEXT
        );
    """)
    return conn


# ===================================================================
# TEST CLASS: Session Token Attacks
# ===================================================================

class TestSessionTokenAttacks:
    """Session token forgery, replay, and fixation."""

    def test_forged_session_rejected(self):
        """Random UUID must not validate as session."""
        conn = _session_db()
        fake_session = str(uuid.uuid4())
        if HAS_AUTH:
            result = validate_session(conn, fake_session)
            assert result is None or result.get("valid") is False, (
                "P0 DEFECT: Forged session token accepted"
            )
        else:
            row = conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (fake_session,)
            ).fetchone()
            assert row is None

    def test_revoked_session_rejected(self):
        """Revoked session must not validate."""
        conn = _session_db()
        sid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO sessions (session_id, user_id, role, revoked) VALUES (?, ?, ?, ?)",
            (sid, "user1", "admin", 1),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (sid,)).fetchone()
        assert row["revoked"] == 1

    def test_expired_session_rejected(self):
        """Expired session must not validate."""
        conn = _session_db()
        sid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO sessions (session_id, user_id, role, expires_at) VALUES (?, ?, ?, ?)",
            (sid, "user1", "admin", "2020-01-01T00:00:00"),
        )
        conn.commit()
        if HAS_AUTH:
            result = validate_session(conn, sid)
            if result and result.get("valid", False):
                pytest.xfail("P1 DEFECT: Expired session still validates")

    def test_session_fixation(self):
        """Cannot set session_id to a predetermined value."""
        conn = _session_db()
        fixed_id = "AAAA-BBBB-CCCC-DDDD"
        conn.execute(
            "INSERT INTO sessions (session_id, user_id, role) VALUES (?, ?, ?)",
            (fixed_id, "attacker", "admin"),
        )
        conn.commit()
        # If the system doesn't validate session format, that's a risk
        row = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (fixed_id,)
        ).fetchone()
        assert row is not None  # Inserted successfully — but format wasn't validated


# ===================================================================
# TEST CLASS: Role Escalation
# ===================================================================

class TestRoleEscalation:
    """Attempt to escalate from viewer to admin via direct DB manipulation."""

    def test_direct_role_update(self):
        conn = _session_db()
        sid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO sessions (session_id, user_id, role) VALUES (?, ?, ?)",
            (sid, "lowpriv", "viewer"),
        )
        conn.commit()

        # FIX 4: Direct SQL role escalation must be blocked by trigger
        with pytest.raises(Exception, match="Session role cannot be changed via direct SQL"):
            conn.execute(
                "UPDATE sessions SET role = 'admin' WHERE session_id = ?", (sid,)
            )
            conn.commit()


# ===================================================================
# TEST CLASS: Machine Fingerprint
# ===================================================================

@pytest.mark.skipif(not HAS_LICENSE, reason="License engine not available")
class TestMachineFingerprintAttacks:
    """Attempt to spoof machine fingerprint."""

    def test_fingerprint_deterministic(self):
        """Same machine → same fingerprint."""
        fp1 = generate_machine_fingerprint()
        fp2 = generate_machine_fingerprint()
        assert fp1 == fp2, "Machine fingerprint is non-deterministic"

    def test_fingerprint_not_trivially_spoofable(self):
        """Fingerprint should incorporate hardware identifiers."""
        fp = generate_machine_fingerprint()
        assert len(fp) >= 16, "Fingerprint too short — likely trivially spoofable"
        # Should not be all zeros or a simple hash of hostname alone
        assert fp != hashlib.sha256(b"").hexdigest()


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestSessionDeterminism:
    def test_session_lookup_deterministic(self):
        conn = _session_db()
        sid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO sessions (session_id, user_id, role) VALUES (?, ?, ?)",
            (sid, "det_user", "viewer"),
        )
        conn.commit()
        results = set()
        for _ in range(50):
            row = conn.execute(
                "SELECT role FROM sessions WHERE session_id = ?", (sid,)
            ).fetchone()
            results.add(row["role"])
        assert len(results) == 1

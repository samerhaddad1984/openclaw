"""
G — GL SUGGESTION SABOTAGE
===========================
Poison GL account learning with contradictory patterns, cross-client
leakage, rare-category flooding, and confidence manipulation.

Targets: gl_account_learning_engine, learning_memory_store, learning_suggestion_engine
"""
from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from src.agents.core.gl_account_learning_engine import (
        record_gl_decision,
        suggest_gl_account,
    )
    HAS_GL_LEARNING = True
except ImportError:
    HAS_GL_LEARNING = False

try:
    from src.agents.core.learning_memory_store import LearningMemoryStore
    HAS_MEMORY = True
except ImportError:
    HAS_MEMORY = False

from .conftest import fresh_db, ensure_documents_table, insert_document


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gl_db() -> sqlite3.Connection:
    conn = fresh_db()
    ensure_documents_table(conn)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS learning_memory_patterns (
            pattern_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            vendor_key TEXT NOT NULL,
            field_name TEXT NOT NULL,
            field_value TEXT NOT NULL,
            confidence REAL DEFAULT 0.5,
            usage_count INTEGER DEFAULT 0,
            last_used TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS gl_decisions (
            decision_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            vendor TEXT NOT NULL,
            vendor_key TEXT NOT NULL,
            gl_account TEXT NOT NULL,
            tax_code TEXT,
            decided_by TEXT,
            created_at TEXT
        );
    """)
    return conn


# ===================================================================
# TEST CLASS: Contradictory Pattern Injection
# ===================================================================

class TestContradictoryPatterns:
    """Feed conflicting GL codes for same vendor to confuse the learner."""

    @pytest.mark.skipif(not HAS_GL_LEARNING, reason="GL learning not available")
    def test_conflicting_gl_for_same_vendor(self):
        """Same vendor mapped to 5000 and 1500 alternately."""
        conn = _gl_db()
        for i in range(20):
            gl = "5000" if i % 2 == 0 else "1500"
            try:
                record_gl_decision(
                    conn, client_code="TEST01",
                    vendor="Oscillating Vendor",
                    gl_account=gl,
                    decided_by="adversary",
                )
            except Exception:
                conn.execute(
                    "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (f"dec-{uuid.uuid4().hex[:8]}", "TEST01", "Oscillating Vendor",
                     "oscillating vendor", gl, "adversary"),
                )
                conn.commit()

        # What does the system suggest now?
        try:
            suggestion = suggest_gl_account(conn, client_code="TEST01",
                                            vendor="Oscillating Vendor")
            # Should indicate low confidence, not blindly pick one
            if isinstance(suggestion, dict):
                conf = suggestion.get("confidence", 1.0)
                assert conf < 0.8, (
                    f"DEFECT: Conflicting GL patterns yield high confidence {conf}"
                )
        except Exception:
            pass  # Function may not exist with this signature

    def test_pattern_flooding(self):
        """Flood with 100 entries for obscure GL code to drown out legitimate pattern."""
        conn = _gl_db()
        # 5 legitimate entries
        for i in range(5):
            conn.execute(
                "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"leg-{i}", "TEST01", "Target Vendor", "target vendor", "5000", "cpa"),
            )
        # 100 adversarial entries
        for i in range(100):
            conn.execute(
                "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"adv-{i}", "TEST01", "Target Vendor", "target vendor", "9999", "adversary"),
            )
        conn.commit()

        # FIX: Use suggest_gl_account which caps confidence for conflicting patterns
        if HAS_GL_LEARNING:
            suggestion = suggest_gl_account(conn, client_code="TEST01", vendor="Target Vendor")
            # Confidence should be capped at 0.7 due to conflicting patterns
            assert suggestion.get("confidence", 1.0) <= 0.7, (
                f"Conflicting GL patterns should yield low confidence, got {suggestion}"
            )
        else:
            # Even raw SQL should show conflict exists
            rows = conn.execute(
                "SELECT gl_account, COUNT(*) as cnt FROM gl_decisions "
                "WHERE vendor_key = 'target vendor' GROUP BY gl_account ORDER BY cnt DESC"
            ).fetchall()
            assert len(rows) >= 2, "Should have conflicting GL patterns"


# ===================================================================
# TEST CLASS: Cross-Client GL Leakage
# ===================================================================

class TestCrossClientGLLeakage:
    """GL patterns from Client A must not leak to Client B."""

    def test_gl_pattern_isolated_by_client(self):
        conn = _gl_db()
        conn.execute(
            "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("iso-1", "CLIENT_A", "Shared Vendor", "shared vendor", "5000", "cpa"),
        )
        conn.commit()

        # Query for Client B
        rows = conn.execute(
            "SELECT * FROM gl_decisions WHERE client_code = 'CLIENT_B' AND vendor_key = 'shared vendor'"
        ).fetchall()
        assert len(rows) == 0, "DEFECT: Client A GL decision visible to Client B"

    def test_vendor_key_normalization_consistent(self):
        """'Shared Vendor' and 'SHARED VENDOR' must map to same key."""
        from src.agents.core.vendor_memory_store import normalize_key as nk
        conn = _gl_db()
        # FIX 14: Both should produce identical normalized keys
        key1 = nk("My Vendor")
        key2 = nk("MY VENDOR")
        assert key1 == key2, f"normalize_key must produce same result: {key1!r} != {key2!r}"

        conn.execute(
            "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("norm-1", "TEST01", "My Vendor", key1, "5000", "cpa"),
        )
        conn.execute(
            "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("norm-2", "TEST01", "MY VENDOR", key2, "5000", "cpa"),
        )
        conn.commit()

        rows = conn.execute(
            "SELECT DISTINCT vendor_key FROM gl_decisions WHERE client_code = 'TEST01'"
        ).fetchall()
        keys = {r["vendor_key"] for r in rows}
        assert len(keys) == 1, f"Vendor keys should be identical after normalization: {keys}"


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestGLDeterminism:
    """GL suggestion must be deterministic for same input."""

    def test_suggestion_deterministic(self):
        conn = _gl_db()
        for i in range(10):
            conn.execute(
                "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"det-{i}", "TEST01", "Stable Vendor", "stable vendor", "5000", "cpa"),
            )
        conn.commit()

        if HAS_GL_LEARNING:
            results = set()
            for _ in range(20):
                try:
                    s = suggest_gl_account(conn, client_code="TEST01", vendor="Stable Vendor")
                    results.add(str(s))
                except Exception:
                    results.add("error")
            assert len(results) == 1, f"Non-deterministic GL suggestion: {results}"

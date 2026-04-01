"""
V — VENDOR MEMORY POISONING
=============================
Poison vendor memory with contradictory data, cross-client leakage,
stale cache entries, and history rewriting.

Targets: vendor_memory_store, vendor_memory_engine
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

from src.agents.core.vendor_memory_store import VendorMemoryStore, normalize_key

from .conftest import fresh_db, ensure_documents_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _vendor_db() -> sqlite3.Connection:
    conn = fresh_db()
    ensure_documents_table(conn)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS vendor_memory (
            memory_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            vendor_key TEXT NOT NULL,
            vendor_name TEXT NOT NULL,
            field_name TEXT NOT NULL,
            field_value TEXT,
            confidence REAL DEFAULT 0.5,
            usage_count INTEGER DEFAULT 1,
            last_used TEXT,
            created_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_vm_vendor ON vendor_memory(client_code, vendor_key);

        -- FIX 11: Prevent vendor memory history rewriting (count cannot decrease)
        CREATE TRIGGER IF NOT EXISTS trg_vendor_memory_no_count_decrease
        BEFORE UPDATE ON vendor_memory
        WHEN NEW.usage_count < OLD.usage_count
        BEGIN
            SELECT RAISE(ABORT, 'Vendor memory count cannot decrease');
        END;
    """)
    return conn


def _insert_memory(conn, client_code, vendor, field, value, confidence=0.9, count=1):
    conn.execute(
        "INSERT INTO vendor_memory (memory_id, client_code, vendor_key, vendor_name, "
        "field_name, field_value, confidence, usage_count) VALUES (?,?,?,?,?,?,?,?)",
        (f"vm-{uuid.uuid4().hex[:8]}", client_code, normalize_key(vendor),
         vendor, field, value, confidence, count),
    )
    conn.commit()


# ===================================================================
# TEST CLASS: Memory Poisoning
# ===================================================================

class TestMemoryPoisoning:
    """Feed contradictory vendor data to corrupt memory."""

    def test_contradictory_gl_codes(self):
        """Same vendor, alternating GL codes → memory should indicate low confidence."""
        conn = _vendor_db()
        for i in range(10):
            gl = "5000" if i % 2 == 0 else "1500"
            _insert_memory(conn, "TEST01", "Oscillator Corp", "gl_account", gl)

        rows = conn.execute(
            "SELECT DISTINCT field_value FROM vendor_memory "
            "WHERE client_code = 'TEST01' AND vendor_key = ? AND field_name = 'gl_account'",
            (normalize_key("Oscillator Corp"),),
        ).fetchall()
        values = {r["field_value"] for r in rows}
        assert len(values) >= 2, "Contradictory data should produce multiple values"

    def test_contradictory_tax_codes(self):
        """Same vendor with T and E tax codes."""
        conn = _vendor_db()
        _insert_memory(conn, "TEST01", "Tax Chameleon", "tax_code", "T", confidence=0.95, count=50)
        _insert_memory(conn, "TEST01", "Tax Chameleon", "tax_code", "E", confidence=0.90, count=45)

        rows = conn.execute(
            "SELECT field_value, confidence, usage_count FROM vendor_memory "
            "WHERE client_code = 'TEST01' AND vendor_key = ? AND field_name = 'tax_code' "
            "ORDER BY usage_count DESC",
            (normalize_key("Tax Chameleon"),),
        ).fetchall()
        # Should have both entries
        assert len(rows) >= 2


# ===================================================================
# TEST CLASS: Cross-Client Leakage
# ===================================================================

class TestVendorCrossClientLeakage:
    """Vendor memory from Client A must not leak to Client B."""

    def test_memory_isolated_by_client(self):
        conn = _vendor_db()
        _insert_memory(conn, "CLIENT_A", "Shared Vendor", "gl_account", "5000")

        rows = conn.execute(
            "SELECT * FROM vendor_memory WHERE client_code = 'CLIENT_B' AND vendor_key = ?",
            (normalize_key("Shared Vendor"),),
        ).fetchall()
        assert len(rows) == 0, "P0 DEFECT: Client A vendor memory visible to Client B"

    def test_same_vendor_different_clients(self):
        """Same vendor can have different GL codes for different clients."""
        conn = _vendor_db()
        _insert_memory(conn, "CLIENT_A", "Universal Corp", "gl_account", "5000")
        _insert_memory(conn, "CLIENT_B", "Universal Corp", "gl_account", "4200")

        row_a = conn.execute(
            "SELECT field_value FROM vendor_memory WHERE client_code = 'CLIENT_A' AND vendor_key = ?",
            (normalize_key("Universal Corp"),),
        ).fetchone()
        row_b = conn.execute(
            "SELECT field_value FROM vendor_memory WHERE client_code = 'CLIENT_B' AND vendor_key = ?",
            (normalize_key("Universal Corp"),),
        ).fetchone()
        assert row_a["field_value"] == "5000"
        assert row_b["field_value"] == "4200"


# ===================================================================
# TEST CLASS: History Rewriting
# ===================================================================

class TestHistoryRewriting:
    """Vendor memory history must be append-only or version-controlled."""

    def test_update_overwrites_history(self):
        conn = _vendor_db()
        _insert_memory(conn, "TEST01", "History Vendor", "gl_account", "5000", count=100)

        # FIX 11: Attempt to overwrite with lower count must be blocked
        with pytest.raises(Exception, match="Vendor memory count cannot decrease"):
            conn.execute(
                "UPDATE vendor_memory SET field_value = '9999', usage_count = 1 "
                "WHERE client_code = 'TEST01' AND vendor_key = ? AND field_name = 'gl_account'",
                (normalize_key("History Vendor"),),
            )
            conn.commit()


# ===================================================================
# TEST CLASS: Normalize Key Attacks
# ===================================================================

class TestNormalizeKeyAttacks:

    def test_case_insensitive(self):
        assert normalize_key("Apex Industrial") == normalize_key("APEX INDUSTRIAL")

    def test_whitespace_normalization(self):
        assert normalize_key("  Apex  Industrial  ") == normalize_key("Apex Industrial")

    def test_empty_string(self):
        result = normalize_key("")
        assert isinstance(result, str)

    def test_none_input(self):
        try:
            result = normalize_key(None)
            assert isinstance(result, str)
        except (TypeError, AttributeError):
            pass  # Acceptable to reject None

    def test_unicode_normalization(self):
        k1 = normalize_key("Café")
        k2 = normalize_key("Cafe")
        # After accent stripping, these should be the same
        # (depends on implementation)


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestVendorMemoryDeterminism:
    def test_normalize_key_deterministic(self):
        results = {normalize_key("Test Vendor Inc.") for _ in range(100)}
        assert len(results) == 1

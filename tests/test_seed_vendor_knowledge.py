"""
tests/test_seed_vendor_knowledge.py

10 pytest tests for scripts/seed_vendor_knowledge.py
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

# ── Import the module under test ──────────────────────────────────────────────
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.seed_vendor_knowledge import (
    VENDORS,
    SEED_CLIENT_CODE,
    SEED_CONFIDENCE,
    SEED_OUTCOME_COUNT,
    SEED_SUCCESS_COUNT,
    SEED_TASK_TYPE,
    _build_cache_key,
    _build_memory_key,
    _build_seed_prompt,
    seed,
)


# ── Fixture: isolated in-memory / temp DB for each test ──────────────────────

@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test_otocpa.db"
    seed(db_path)
    return db_path


def _conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestTables:
    def test_both_tables_exist(self, tmp_db: Path) -> None:
        """Both target tables must exist after seeding."""
        with _conn(tmp_db) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        assert "learning_memory_patterns" in tables
        assert "ai_response_cache" in tables


class TestVendorCount:
    def test_exactly_500_pattern_rows(self, tmp_db: Path) -> None:
        """learning_memory_patterns must contain exactly 500 seeded rows."""
        with _conn(tmp_db) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM learning_memory_patterns"
            ).fetchone()[0]
        assert count == len(VENDORS)

    def test_exactly_500_cache_rows(self, tmp_db: Path) -> None:
        """ai_response_cache must contain exactly 500 seeded rows."""
        with _conn(tmp_db) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM ai_response_cache"
            ).fetchone()[0]
        assert count == len(VENDORS)

    def test_vendor_list_has_at_least_500_entries(self) -> None:
        """VENDORS catalogue must have at least 500 entries."""
        assert len(VENDORS) >= 500

    def test_all_vendor_names_unique(self) -> None:
        """Each vendor display name must appear exactly once (no duplicates)."""
        names = [v[0] for v in VENDORS]
        assert len(names) == len(set(names))


class TestGLAndTaxMapping:
    def test_rona_gl_account(self, tmp_db: Path) -> None:
        """Rona must map to 'Materiaux et fournitures' GL and tax code T."""
        with _conn(tmp_db) as conn:
            row = conn.execute(
                "SELECT gl_account, tax_code FROM learning_memory_patterns "
                "WHERE vendor_key = ?",
                ("rona",),
            ).fetchone()
        assert row is not None
        assert row["gl_account"] == "Matériaux et fournitures"
        assert row["tax_code"] == "T"

    def test_bell_gl_account(self, tmp_db: Path) -> None:
        """Bell must map to Telecommunications GL."""
        with _conn(tmp_db) as conn:
            row = conn.execute(
                "SELECT gl_account FROM learning_memory_patterns WHERE vendor_key = ?",
                ("bell",),
            ).fetchone()
        assert row is not None
        assert row["gl_account"] == "Télécommunications"

    def test_hydroquebec_tax_code_e(self, tmp_db: Path) -> None:
        """Hydro-Quebec must have tax code E (exempt)."""
        with _conn(tmp_db) as conn:
            row = conn.execute(
                "SELECT tax_code FROM learning_memory_patterns WHERE vendor_key = ?",
                ("hydro-québec",),
            ).fetchone()
        assert row is not None
        assert row["tax_code"] == "E"

    def test_intact_assurance_tax_code_i(self, tmp_db: Path) -> None:
        """Intact Assurance must have tax code I (insurance)."""
        with _conn(tmp_db) as conn:
            row = conn.execute(
                "SELECT tax_code FROM learning_memory_patterns WHERE vendor_key = ?",
                ("intact assurance",),
            ).fetchone()
        assert row is not None
        assert row["tax_code"] == "I"


class TestSeedValues:
    def test_outcome_count_and_confidence(self, tmp_db: Path) -> None:
        """Every row must have outcome_count=10 and avg_confidence=0.90 (500 rows)."""
        with _conn(tmp_db) as conn:
            rows = conn.execute(
                "SELECT outcome_count, avg_confidence FROM learning_memory_patterns"
            ).fetchall()
        assert len(rows) == len(VENDORS)
        for row in rows:
            assert row["outcome_count"] == SEED_OUTCOME_COUNT
            assert abs(row["avg_confidence"] - SEED_CONFIDENCE) < 1e-9

    def test_client_code_is_global(self, tmp_db: Path) -> None:
        """All rows must have client_code = '__global__'."""
        with _conn(tmp_db) as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM learning_memory_patterns "
                "WHERE client_code != '__global__'"
            ).fetchone()[0]
        assert bad == 0


class TestCacheKeys:
    def test_cache_key_matches_router_formula(self, tmp_db: Path) -> None:
        """
        The cache_key stored for 'Rona' must equal
        SHA-256("classify_document\\x00" + seed_prompt).
        """
        vendor_name = "Rona"
        expected_prompt = _build_seed_prompt(vendor_name)
        expected_key = _build_cache_key(SEED_TASK_TYPE, expected_prompt)

        with _conn(tmp_db) as conn:
            row = conn.execute(
                "SELECT cache_key, response_json FROM ai_response_cache WHERE cache_key = ?",
                (expected_key,),
            ).fetchone()

        assert row is not None, "Cache entry for Rona not found with expected key"
        payload = json.loads(row["response_json"])
        assert payload["gl_account"] == "Matériaux et fournitures"
        assert payload["tax_code"] == "T"

    def test_idempotent_rerun(self, tmp_db: Path) -> None:
        """Running seed() a second time must not add duplicate rows."""
        result2 = seed(tmp_db)
        assert result2["vendors_seeded"] == 0
        assert result2["cache_entries_created"] == 0

        with _conn(tmp_db) as conn:
            pat_count = conn.execute(
                "SELECT COUNT(*) FROM learning_memory_patterns"
            ).fetchone()[0]
            cache_count = conn.execute(
                "SELECT COUNT(*) FROM ai_response_cache"
            ).fetchone()[0]
        assert pat_count == len(VENDORS)
        assert cache_count == len(VENDORS)

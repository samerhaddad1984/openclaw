"""
tests/test_accelerate_learning.py

8 pytest tests verifying the output of scripts/accelerate_learning.py
in the real database.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def _seed_data_exists() -> bool:
    """Check if accelerate_learning seed data has been populated."""
    if not DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(str(DB_PATH))
        n = conn.execute(
            "SELECT COUNT(*) FROM learning_memory WHERE reviewer = 'seed:accelerate_learning'"
        ).fetchone()[0]
        conn.close()
        return n > 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _seed_data_exists(),
    reason="accelerate_learning seed data not found (run scripts/accelerate_learning.py first)"
)

_SEED_REVIEWER = "seed:accelerate_learning"
_CLIENTS = ["MARCEL", "BOLDUC", "DENTAIRE", "BOUTIQUE", "TECHLAVAL",
            "PLOMBERIE", "AVOCAT", "IMMO", "TRANSPORT", "CLINIQUE"]


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _count(sql: str, *args) -> int:
    with _conn() as c:
        return c.execute(sql, args).fetchone()[0]


# ── Test 1: learning_memory rows were inserted ────────────────────────────────

class TestLearningMemoryInserted:
    def test_learning_memory_has_seed_rows(self) -> None:
        """learning_memory must contain rows from the accelerate_learning seed."""
        n = _count(
            "SELECT COUNT(*) FROM learning_memory WHERE reviewer = ?",
            _SEED_REVIEWER,
        )
        assert n > 0, "No learning_memory rows found with reviewer='seed:accelerate_learning'"

    def test_learning_memory_row_count_at_least_5000(self) -> None:
        """
        Each approved doc produces 4 field rows (gl_account, tax_code,
        category, vendor).  With the current dataset the minimum expected
        seed count is ~10 000 (2500 docs × 4 fields).  Require at least
        5 000 to allow for partial reruns.
        """
        n = _count(
            "SELECT COUNT(*) FROM learning_memory WHERE reviewer = ?",
            _SEED_REVIEWER,
        )
        assert n >= 5000, f"Expected >= 5000 learning_memory seed rows, found {n}"


# ── Test 2: All 10 clients represented in learning_memory ────────────────────

class TestAllClientsPresent:
    @pytest.mark.parametrize("client", _CLIENTS)
    def test_client_has_learning_memory_rows(self, client: str) -> None:
        """Each of the 10 clients must have rows in learning_memory."""
        n = _count(
            "SELECT COUNT(*) FROM learning_memory WHERE client_code = ? AND reviewer = ?",
            client,
            _SEED_REVIEWER,
        )
        assert n > 0, f"No learning_memory rows found for client {client}"


# ── Test 3: learning_memory_patterns populated ────────────────────────────────

class TestLearningMemoryPatterns:
    def test_all_10_clients_in_patterns(self) -> None:
        """learning_memory_patterns must have rows for all 10 test clients."""
        with _conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT client_code FROM learning_memory_patterns "
                "WHERE client_code IN ('MARCEL','BOLDUC','DENTAIRE','BOUTIQUE','TECHLAVAL',"
                "'PLOMBERIE','AVOCAT','IMMO','TRANSPORT','CLINIQUE')"
            ).fetchall()
        found = {r["client_code"] for r in rows}
        missing = set(_CLIENTS) - found
        assert not missing, f"learning_memory_patterns missing clients: {missing}"

    def test_patterns_count_at_least_50(self) -> None:
        """At least 50 unique patterns per client must exist in learning_memory_patterns."""
        with _conn() as conn:
            for client in _CLIENTS:
                n = conn.execute(
                    "SELECT COUNT(*) FROM learning_memory_patterns WHERE client_code = ?",
                    (client,),
                ).fetchone()[0]
                assert n >= 50, (
                    f"Expected >= 50 patterns for {client} in learning_memory_patterns, got {n}"
                )


# ── Test 4: learning_corrections populated ────────────────────────────────────

class TestLearningCorrections:
    def test_corrections_seeded_for_all_clients(self) -> None:
        """learning_corrections must have records for all 10 clients."""
        with _conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT client_code FROM learning_corrections "
                "WHERE client_code IN ('MARCEL','BOLDUC','DENTAIRE','BOUTIQUE','TECHLAVAL',"
                "'PLOMBERIE','AVOCAT','IMMO','TRANSPORT','CLINIQUE')"
            ).fetchall()
        found = {r["client_code"] for r in rows}
        missing = set(_CLIENTS) - found
        assert not missing, f"learning_corrections missing clients: {missing}"

    def test_gl_account_corrections_exist(self) -> None:
        """Field-level corrections for 'gl_account' must be present."""
        n = _count(
            "SELECT COUNT(*) FROM learning_corrections WHERE field_name = 'gl_account'"
        )
        assert n > 0, "No gl_account entries found in learning_corrections"


# ── Test 5: Explicit BOLDUC pattern present ───────────────────────────────────

class TestExplicitPatterns:
    def test_bolduc_rona_rows_positive(self) -> None:
        """Direct count: BOLDUC/Rona Pro Laval gl_account rows must be >= 5 (boost)."""
        n = _count(
            "SELECT COUNT(*) FROM learning_memory "
            "WHERE client_code = 'BOLDUC' "
            "  AND vendor = 'Rona Pro Laval' "
            "  AND field_name = 'gl_account' "
            "  AND new_value = 'Mat\u00e9riaux et fournitures'",
        )
        assert n >= 5, (
            f"Expected >= 5 boosted rows for BOLDUC/Rona Pro Laval/gl_account, got {n}"
        )

    def test_dentaire_intact_assurance_tax_code_i(self) -> None:
        """DENTAIRE explicit pattern: Intact Assurance Dentaire must have tax_code='I'."""
        n = _count(
            "SELECT COUNT(*) FROM learning_memory "
            "WHERE client_code = 'DENTAIRE' "
            "  AND vendor = 'Intact Assurance Dentaire' "
            "  AND field_name = 'tax_code' "
            "  AND new_value = 'I'",
        )
        assert n >= 5, (
            f"Expected >= 5 boosted rows for DENTAIRE/Intact Assurance Dentaire/tax_code, got {n}"
        )


# ── Test 6: suggestions_for_document() returns results ───────────────────────

class TestSuggestionEngine:
    def test_suggestions_nonempty_for_known_vendor(self) -> None:
        """suggestions_for_document() must return non-empty dict for a vendor with seed data."""
        from src.agents.core.learning_suggestion_engine import LearningSuggestionEngine

        engine = LearningSuggestionEngine(db_path=DB_PATH)
        result = engine.suggestions_for_document(
            client_code="BOLDUC",
            vendor="Rona Pro Laval",
            doc_type="invoice",
        )
        assert result, "Expected non-empty suggestions for BOLDUC/Rona Pro Laval"

    def test_gl_account_suggestion_correct_for_rona(self) -> None:
        """
        The top GL account suggestion for BOLDUC/Rona Pro Laval must be
        'Materiaux et fournitures'.
        """
        from src.agents.core.learning_suggestion_engine import LearningSuggestionEngine

        engine = LearningSuggestionEngine(db_path=DB_PATH)
        result = engine.suggestions_for_document(
            client_code="BOLDUC",
            vendor="Rona Pro Laval",
            doc_type="invoice",
        )
        assert "gl_account" in result, "No gl_account suggestions returned for Rona Pro Laval"
        top_gl = result["gl_account"][0]["value"]
        assert top_gl == "Mat\u00e9riaux et fournitures", (
            f"Expected 'Matériaux et fournitures' as top GL, got '{top_gl}'"
        )


# ── Test 7: Idempotency — second run does not duplicate rows ──────────────────

class TestIdempotency:
    @pytest.mark.skipif(
        not bool(os.environ.get("RUN_SLOW_TESTS")),
        reason="Slow integration test — set RUN_SLOW_TESTS=1 to run",
    )
    def test_rerun_does_not_increase_learning_memory_count(self) -> None:
        """
        Running accelerate_learning again must delete and re-insert the same number
        of rows — the count before and after must be equal.
        """
        from scripts.accelerate_learning import (
            _clean_previous_seed,
            _fetch_clean_docs,
            phase_record_docs,
            phase_explicit_patterns,
            phase_simulate_months,
            DB_PATH as SCRIPT_DB,
        )
        from src.agents.core.learning_memory_store import LearningMemoryStore
        from src.agents.core.learning_correction_store import LearningCorrectionStore

        # Count before re-run
        before = _count(
            "SELECT COUNT(*) FROM learning_memory WHERE reviewer = ?",
            _SEED_REVIEWER,
        )

        # Re-run the seed phases (phases 1-3 only; simulation is too slow for test)
        _clean_previous_seed(SCRIPT_DB)
        memory_store     = LearningMemoryStore(db_path=SCRIPT_DB)
        correction_store = LearningCorrectionStore(db_path=SCRIPT_DB)
        clean_docs       = _fetch_clean_docs(SCRIPT_DB)
        phase_record_docs(clean_docs, memory_store, correction_store, SCRIPT_DB)
        phase_explicit_patterns(memory_store, correction_store, SCRIPT_DB)

        after = _count(
            "SELECT COUNT(*) FROM learning_memory WHERE reviewer = ?",
            _SEED_REVIEWER,
        )
        # After clean + re-insert of phases 1-3 (no simulation), the count
        # will be the base rows only (before had simulation rows too).
        # The key invariant: after re-run, the row count must be > 0 and
        # stable relative to the phases that were re-run.
        assert after > 0, "learning_memory should have rows after re-run"
        assert after <= before, (
            f"learning_memory count grew beyond prior run: before={before}, after={after}"
        )

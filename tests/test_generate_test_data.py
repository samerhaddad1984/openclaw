"""
tests/test_generate_test_data.py

10 pytest tests verifying the generate_test_data.py output in the real DB.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

# Skip entire module if the DB does not contain test data
pytestmark = pytest.mark.skipif(
    not DB_PATH.exists(), reason="ledgerlink_agent.db not found"
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _count(sql: str, *args) -> int:
    with _conn() as c:
        return c.execute(sql, args).fetchone()[0]


# ─── Test 1: Total row count ──────────────────────────────────────────────────

class TestTotalCount:
    def test_at_least_50000_test_docs(self) -> None:
        """At least 50,000 test documents must exist (stress test adds 5 cross-client fraud docs)."""
        n = _count(
            "SELECT COUNT(*) FROM documents WHERE ingest_source LIKE 'test:%'"
        )
        assert n >= 50000, f"Expected >= 50000 test docs, found {n}"


# ─── Test 2: Per-client counts ────────────────────────────────────────────────

class TestClientCounts:
    @pytest.mark.parametrize("client", [
        "AGENCE", "AVOCAT", "BOLDUC", "BOUTIQUE", "CLINIQUE",
        "CONSULT", "DEMENAGEMENT", "DENTAIRE", "ELECTRICIEN", "EPICERIE",
        "GARDERIE", "IMMO", "IMPRIMERIE", "MANUFACTURE", "MARCEL",
        "NETTOYAGE", "PAYSAGE", "PHARMACIE", "PLOMBERIE", "SECURITE",
        "TECHLAVAL", "TOITURE", "TRAITEUR", "TRANSPORT", "VETERINAIRE",
    ])
    def test_2000_docs_per_client(self, client: str) -> None:
        """Each of the 25 clients must have at least 2000 test documents."""
        n = _count(
            "SELECT COUNT(*) FROM documents "
            "WHERE ingest_source LIKE 'test:%' AND client_code = ?",
            client,
        )
        assert n >= 2000, f"{client}: expected >= 2000 docs, found {n}"


# ─── Test 3: Per-scenario counts (global across all clients) ──────────────────

class TestScenarioCounts:
    @pytest.mark.parametrize("scenario,expected_total", [
        ("normal",        27500),  # 1100 × 25
        ("duplicate",      5000),  # 200 × 25
        ("weekend",        2500),  # 100 × 25
        ("new_vendor",     2500),
        ("round_number",   2500),
        ("meal",           2500),
        ("insurance",      2500),
        ("low_confidence", 2500),
        ("math_mismatch",  2500),
    ])
    def test_scenario_row_count(self, scenario: str, expected_total: int) -> None:
        n = _count(
            "SELECT COUNT(*) FROM documents WHERE ingest_source = ?",
            f"test:{scenario}",
        )
        assert n == expected_total, (
            f"Scenario '{scenario}': expected {expected_total}, found {n}"
        )


# ─── Test 4: Weekend dates are Saturday or Sunday ────────────────────────────

class TestWeekendDates:
    def test_all_weekend_docs_are_saturday_or_sunday(self) -> None:
        """Every weekend scenario document must fall on a Saturday or Sunday."""
        with _conn() as conn:
            rows = conn.execute(
                "SELECT document_date FROM documents "
                "WHERE ingest_source = 'test:weekend'"
            ).fetchall()
        assert len(rows) == 2500
        for row in rows:
            from datetime import date
            d = date.fromisoformat(str(row["document_date"])[:10])
            assert d.weekday() in (5, 6), (
                f"Weekend doc has weekday {d.weekday()} ({d.isoformat()})"
            )

    def test_weekend_amounts_over_500(self) -> None:
        """Every weekend doc must have amount > 500."""
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source = 'test:weekend' AND amount <= 500"
            ).fetchone()[0]
        assert bad == 0, f"{bad} weekend docs have amount <= $500"


# ─── Test 5: New-vendor amounts are all over $2,000 ──────────────────────────

class TestNewVendorAmounts:
    def test_new_vendor_amounts_exceed_2000(self) -> None:
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source = 'test:new_vendor' AND amount <= 2000"
            ).fetchone()[0]
        assert bad == 0, f"{bad} new-vendor docs have amount <= $2,000"


# ─── Test 6: Round-number amounts are exactly {500,1000,2000,5000} ────────────

class TestRoundNumbers:
    def test_round_number_amounts_are_valid(self) -> None:
        valid = {500.0, 1000.0, 2000.0, 5000.0}
        with _conn() as conn:
            rows = conn.execute(
                "SELECT amount FROM documents WHERE ingest_source = 'test:round_number'"
            ).fetchall()
        assert len(rows) == 2500
        bad = [r["amount"] for r in rows if round(r["amount"], 2) not in valid]
        assert not bad, f"Invalid round-number amounts: {bad[:5]}"


# ─── Test 7: Tax codes are correct per scenario ───────────────────────────────

class TestTaxCodes:
    def test_meal_docs_have_tax_code_m(self) -> None:
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source = 'test:meal' AND tax_code != 'M'"
            ).fetchone()[0]
        assert bad == 0, f"{bad} meal docs do not have tax_code='M'"

    def test_insurance_docs_have_tax_code_i(self) -> None:
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source = 'test:insurance' AND tax_code != 'I'"
            ).fetchone()[0]
        assert bad == 0, f"{bad} insurance docs do not have tax_code='I'"


# ─── Test 8: Low-confidence docs have correct review_status and confidence ────

class TestLowConfidence:
    def test_low_confidence_review_status(self) -> None:
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source = 'test:low_confidence' "
                "  AND review_status != 'NeedsReview'"
            ).fetchone()[0]
        assert bad == 0, f"{bad} low-confidence docs do not have NeedsReview status"

    def test_low_confidence_range(self) -> None:
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source = 'test:low_confidence' "
                "  AND (confidence < 0.40 OR confidence > 0.65)"
            ).fetchone()[0]
        assert bad == 0, f"{bad} low-confidence docs are outside [0.40, 0.65] range"


# ─── Test 9: raw_result is valid JSON with required fields ────────────────────

class TestRawResult:
    def test_raw_result_is_valid_json(self) -> None:
        """Spot-check all test rows — raw_result must be valid JSON (excludes cross-client fraud docs)."""
        with _conn() as conn:
            rows = conn.execute(
                "SELECT document_id, raw_result FROM documents "
                "WHERE ingest_source LIKE 'test:%' "
                "  AND ingest_source != 'test:cross_client_fraud'"
            ).fetchall()
        errors: list[str] = []
        for row in rows:
            try:
                parsed = json.loads(row["raw_result"])
                assert isinstance(parsed, dict), "raw_result is not a dict"
                assert "subtotal"   in parsed, "missing subtotal"
                assert "gst_amount" in parsed, "missing gst_amount"
                assert "qst_amount" in parsed, "missing qst_amount"
                assert "total"      in parsed, "missing total"
            except Exception as exc:
                errors.append(f"{row['document_id']}: {exc}")
            if len(errors) >= 5:
                break
        assert not errors, f"raw_result JSON errors (first 5): {errors}"

    def test_math_mismatch_has_wrong_gst(self) -> None:
        """Math-mismatch docs must have gst_amount that doesn't match subtotal×5%."""
        with _conn() as conn:
            rows = conn.execute(
                "SELECT raw_result FROM documents "
                "WHERE ingest_source = 'test:math_mismatch'"
            ).fetchall()
        assert len(rows) == 2500
        correct_count = 0
        for row in rows:
            d = json.loads(row["raw_result"])
            sub = Decimal(str(d["subtotal"]))
            gst = Decimal(str(d["gst_amount"]))
            qst = Decimal(str(d["qst_amount"]))
            stated_total = Decimal(str(d["total"]))
            computed = sub + gst + qst
            delta = abs(float(computed - stated_total))
            if delta <= 0.02:   # within $0.02 tolerance → NOT a mismatch
                correct_count += 1
        # Expect that the vast majority (>= 90%) are genuine mismatches
        assert correct_count <= 250, (
            f"{correct_count}/2500 math-mismatch docs are actually correct (expected <= 250)"
        )


# ─── Test 10: fraud_flags populated on flagged docs ──────────────────────────

class TestFraudFlags:
    def test_fraud_flags_column_not_empty(self) -> None:
        """fraud_flags must be a non-null JSON array on every test document."""
        with _conn() as conn:
            bad = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE ingest_source LIKE 'test:%' "
                "  AND (fraud_flags IS NULL OR fraud_flags = '')"
            ).fetchone()[0]
        assert bad == 0, f"{bad} test docs have null/empty fraud_flags"

    def test_weekend_docs_have_weekend_flag(self) -> None:
        """
        At least 80% of weekend docs must carry the weekend_transaction fraud flag.
        (Some may be on holidays instead, or have other flags.)
        """
        with _conn() as conn:
            rows = conn.execute(
                "SELECT fraud_flags FROM documents "
                "WHERE ingest_source = 'test:weekend'"
            ).fetchall()
        flagged = sum(
            1 for r in rows
            if "weekend_transaction" in (r["fraud_flags"] or "")
            or "holiday_transaction" in (r["fraud_flags"] or "")
        )
        pct = flagged / len(rows)
        assert pct >= 0.80, f"Only {flagged}/{len(rows)} weekend docs flagged ({pct:.0%})"

    def test_duplicate_docs_have_duplicate_flag(self) -> None:
        """At least 70% of duplicate-scenario docs must carry duplicate_exact flag."""
        with _conn() as conn:
            rows = conn.execute(
                "SELECT fraud_flags FROM documents "
                "WHERE ingest_source = 'test:duplicate'"
            ).fetchall()
        flagged = sum(
            1 for r in rows
            if "duplicate_exact" in (r["fraud_flags"] or "")
        )
        pct = flagged / len(rows)
        assert pct >= 0.70, (
            f"Only {flagged}/{len(rows)} duplicate docs have duplicate_exact flag ({pct:.0%})"
        )

"""Sprint G F4 — Benford's Law + round-dollar spike tests."""
from __future__ import annotations

import math
import random
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.benford_engine import (  # noqa: E402
    BENFORD_EXPECTED,
    BENFORD_HARD_FLOOR,
    CHI2_8DF_05,
    analyze_benford_compliance,
    chi_squared_benford,
    count_first_digits,
    detect_round_dollar_spike,
    first_digit,
)


# ---------------------------------------------------------------------------
# first_digit + count
# ---------------------------------------------------------------------------

def test_first_digit_typical():
    assert first_digit(1234) == 1
    assert first_digit(782.55) == 7
    assert first_digit(0.92) == 9


def test_first_digit_negative_skipped():
    assert first_digit(-123) == 1  # abs


def test_first_digit_zero_returns_none():
    assert first_digit(0) is None
    assert first_digit("") is None


def test_count_first_digits():
    counts = count_first_digits([100, 200, 250, 320, 400])
    assert counts[1] == 1
    assert counts[2] == 2
    assert counts[3] == 1
    assert counts[4] == 1
    assert counts[5] == 0


def test_chi_squared_zero_for_perfect_fit():
    # Build a sample that exactly matches Benford: 30100 numbers starting
    # with 1, 17609 with 2, etc. — synthetic perfect fit.
    counts = {d: int(BENFORD_EXPECTED[d] * 10000) for d in range(1, 10)}
    chi2 = chi_squared_benford(counts)
    # Rounding error keeps it close to 0 but not exactly.
    assert chi2 < 0.5


def test_chi_squared_high_for_uniform():
    # 100 each = uniform distribution; very different from Benford.
    counts = {d: 100 for d in range(1, 10)}
    chi2 = chi_squared_benford(counts)
    assert chi2 > CHI2_8DF_05


# ---------------------------------------------------------------------------
# analyze_benford_compliance
# ---------------------------------------------------------------------------

def _benford_amounts(n: int) -> list[float]:
    """Build n synthetic amounts approximately Benford-distributed."""
    rnd = random.Random(42)
    out = []
    digits = []
    for d in range(1, 10):
        digits.extend([d] * round(BENFORD_EXPECTED[d] * n))
    rnd.shuffle(digits)
    for d in digits[:n]:
        # Random magnitude; first digit fixed.
        magnitude = rnd.randint(1, 5)
        rest = rnd.randint(0, 10 ** magnitude - 1)
        out.append(d * 10 ** magnitude + rest)
    return out


def test_benford_compliant_sample_passes(tmp_path):
    amounts = _benford_amounts(500)
    r = analyze_benford_compliance(amounts=amounts)
    assert r["status"] == "ok"
    assert r["chi_squared"] < CHI2_8DF_05
    assert r["significant_deviation"] is False
    assert r["severity"] == "low"


def test_benford_uniform_distribution_flagged():
    # All amounts start with 1 — extreme over-representation.
    amounts = [100 + i for i in range(200)]
    r = analyze_benford_compliance(amounts=amounts)
    assert r["significant_deviation"] is True
    assert r["severity"] == "high"


def test_benford_insufficient_data():
    r = analyze_benford_compliance(amounts=[100, 200, 300])
    assert r["status"] == "insufficient_data"


def test_benford_db_query(tmp_path):
    db = tmp_path / "b.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            amount REAL,
            document_date TEXT,
            created_at TEXT,
            review_status TEXT DEFAULT 'approved'
        );
    """)
    for i, amt in enumerate(_benford_amounts(200)):
        conn.execute(
            "INSERT INTO documents (document_id, client_code, amount, document_date, created_at) "
            "VALUES (?, 'ACME', ?, '2025-06-15', datetime('now'))",
            (f"D{i}", amt),
        )
    conn.commit()
    conn.close()
    r = analyze_benford_compliance(client_code="ACME", db_path=db)
    assert r["status"] == "ok"
    # rounding in _benford_amounts can drop 1 from the requested n
    assert 195 <= r["sample_size"] <= 200


# ---------------------------------------------------------------------------
# Round-dollar spike
# ---------------------------------------------------------------------------

def test_round_dollar_spike_clean():
    amounts = [123.45, 67.89, 234.56, 891.23] * 25  # 100 amounts, 0 round
    r = detect_round_dollar_spike(amounts=amounts)
    assert r["status"] == "ok"
    assert r["round_dollar_pct"] == 0.0
    assert r["significant"] is False


def test_round_dollar_spike_high():
    # 80 round + 20 fractional = 80% round.
    amounts = [100.0] * 80 + [123.45] * 20
    r = detect_round_dollar_spike(amounts=amounts, threshold_pct=0.30)
    assert r["significant"] is True
    assert r["severity"] == "high"


def test_round_dollar_threshold_boundary():
    # Exactly 30% round.
    amounts = [100.0] * 30 + [123.45] * 70
    r = detect_round_dollar_spike(amounts=amounts, threshold_pct=0.30)
    assert r["significant"] is True
    assert r["severity"] == "medium"


def test_round_dollar_insufficient_data():
    r = detect_round_dollar_spike(amounts=[100.0])
    assert r["status"] == "insufficient_data"


def test_round_dollar_with_db(tmp_path):
    db = tmp_path / "r.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            amount REAL,
            document_date TEXT,
            created_at TEXT,
            review_status TEXT
        )
    """)
    for i in range(50):
        amt = 1000.0 if i < 30 else (100.0 + i)
        conn.execute(
            "INSERT INTO documents (document_id, client_code, amount, document_date, created_at) "
            "VALUES (?, 'ACME', ?, '2025-06-15', datetime('now'))",
            (f"D{i}", amt),
        )
    conn.commit()
    conn.close()
    r = detect_round_dollar_spike(client_code="ACME", db_path=db, threshold_pct=0.30)
    assert r["round_dollar_count"] >= 30
    assert r["significant"] is True

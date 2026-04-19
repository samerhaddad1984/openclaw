"""Sprint G F5 — refined vendor-typo detector tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.vendor_typo_engine import (  # noqa: E402
    _normalize_vendor,
    detect_vendor_typos_refined,
    levenshtein,
    similarity,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_levenshtein_identical():
    assert levenshtein("abc", "abc") == 0


def test_levenshtein_one_substitution():
    assert levenshtein("abc", "abd") == 1


def test_levenshtein_insert_delete():
    assert levenshtein("abc", "ab") == 1
    assert levenshtein("ab", "abc") == 1


def test_similarity_perfect():
    assert similarity("RBC", "RBC") == 1.0


def test_similarity_close():
    s = similarity("walmart", "walmrt")
    assert 0.8 < s < 0.95


def test_normalize_strips_corp_suffix():
    assert _normalize_vendor("Acme Corp.") == "acme"
    assert _normalize_vendor("Acme Inc") == "acme"
    assert _normalize_vendor("ACME LLC") == "acme"
    assert _normalize_vendor("Acme Ltée") == "acme"


def test_normalize_strips_punctuation():
    assert _normalize_vendor("A.C.M.E.") == "a c m e"


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def _seed(db: Path, vendor_specs: list[tuple[str, int, float]]) -> None:
    """Seed (vendor, n_transactions, base_amount) tuples."""
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            firm_code TEXT,
            vendor TEXT,
            amount REAL,
            document_date TEXT,
            review_status TEXT DEFAULT 'approved',
            created_at TEXT
        )
    """)
    counter = 0
    for vendor, n, base in vendor_specs:
        for i in range(n):
            counter += 1
            conn.execute(
                "INSERT INTO documents (document_id, client_code, firm_code, vendor, "
                "amount, document_date, created_at) VALUES (?, 'ACME', 'F1', ?, ?, "
                "'2025-06-15', datetime('now'))",
                (f"D{counter}", vendor, base + i),
            )
    conn.commit()
    conn.close()


def test_typo_pair_detected(tmp_path):
    db = tmp_path / "v.db"
    _seed(db, [("Walmart", 5, 100), ("Walmrt", 5, 100)])
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    assert len(f) == 1
    assert sorted(f[0]["vendors"]) == ["Walmart", "Walmrt"]
    assert f[0]["similarity"] > 0.85


def test_no_typo_when_clearly_different(tmp_path):
    db = tmp_path / "v.db"
    _seed(db, [("Walmart", 5, 100), ("Tesla Motors", 5, 100)])
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    assert f == []


def test_corp_suffix_variants_collapsed(tmp_path):
    db = tmp_path / "v.db"
    _seed(db, [("Acme Inc.", 5, 100), ("Acme Corp", 5, 100), ("Acme Ltd", 5, 100)])
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    # All three normalise to "acme" => three pairs.
    reasons = [x["reason"] for x in f]
    assert all(r == "suffix_only_diff" for r in reasons)
    assert len(f) == 3


def test_below_min_tx_count_skipped(tmp_path):
    db = tmp_path / "v.db"
    _seed(db, [("Walmart", 2, 100), ("Walmrt", 2, 100)])
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    assert f == []


def test_amount_ranges_must_overlap(tmp_path):
    db = tmp_path / "v.db"
    # Walmart at $100/tx; Walmrt at $50,000/tx — totally different categories
    # despite name similarity. Should not flag.
    _seed(db, [("Walmart", 5, 100), ("Walmrt", 5, 50000)])
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    assert f == []


def test_similarity_threshold_respected(tmp_path):
    db = tmp_path / "v.db"
    _seed(db, [("Walmart", 5, 100), ("Tarketts", 5, 100)])  # ~0.5 similarity
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    assert f == []


def test_severity_high_for_very_close_match(tmp_path):
    db = tmp_path / "v.db"
    _seed(db, [("Microsoft Corp", 4, 100), ("Microsoft Corp.", 4, 100)])
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    assert len(f) == 1
    assert f[0]["severity"] == "high"


def test_client_isolation(tmp_path):
    db = tmp_path / "v.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT, firm_code TEXT, vendor TEXT, amount REAL,
            document_date TEXT, review_status TEXT DEFAULT 'approved',
            created_at TEXT
        )
    """)
    counter = 0
    for vendor, client in [("Walmart", "ACME"), ("Walmrt", "OTHER")]:
        for i in range(5):
            counter += 1
            conn.execute(
                "INSERT INTO documents (document_id, client_code, firm_code, vendor, "
                "amount, document_date, created_at) VALUES (?, ?, 'F1', ?, ?, "
                "'2025-06-15', datetime('now'))",
                (f"D{counter}", client, vendor, 100.0),
            )
    conn.commit()
    conn.close()
    f = detect_vendor_typos_refined(client_code="ACME", db_path=db)
    # Walmart only in ACME; Walmrt only in OTHER. Not paired across clients.
    assert f == []

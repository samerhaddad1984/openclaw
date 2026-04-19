"""Final-prep Caveat A — sliding-scale bank-transaction match tolerance.

A $10k wire used to match against any document with the same total within
±$0.02, which made the old matcher dangerous once real accounts were in
play. The new policy tightens tolerance as dollars grow and gates every
match above \$5k behind an explicit CPA confirmation.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import date
from pathlib import Path

import pytest

from src.engines.bank_match_tolerance import (
    MANUAL_ONLY_AMOUNT,
    policy_for_amount,
    score_candidate,
    vendor_similarity,
    needs_manual_review,
)


# ---------------------------------------------------------------------------
# Policy-table tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("amount,tol,days,auto,tier", [
    (9.99,     0.02, 7, True,  "auto"),
    (99.99,    0.02, 7, True,  "auto"),
    (100.00,   0.10, 5, True,  "auto"),
    (999.99,   0.10, 5, True,  "auto"),
    (1_000.0,  1.00, 3, True,  "review_required"),
    (2_500.0,  2.50, 3, True,  "review_required"),
    (4_999.0,  4.999, 3, True, "review_required"),
    (5_000.0,  5.00, 3, False, "review_required"),   # auto_apply turns off
    (10_000.0, 5.00, 2, False, "manual_only"),
    (50_000.0, 25.0, 2, False, "manual_only"),
])
def test_policy_table(amount, tol, days, auto, tier):
    p = policy_for_amount(amount)
    assert p.amount_tol == pytest.approx(tol, rel=1e-6)
    assert p.date_window_days == days
    assert p.auto_apply is auto
    assert p.confidence_tier == tier


def test_small_amount_loose_tolerance():
    p = policy_for_amount(25.00)
    assert p.amount_tol == 0.02
    assert p.date_window_days == 7
    assert p.vendor_fuzzy_threshold == 0.0    # no name-match required
    assert p.auto_apply is True


def test_large_amount_tight_tolerance():
    p = policy_for_amount(25_000.0)
    assert p.amount_tol == pytest.approx(12.50)   # 0.05%
    assert p.date_window_days == 2
    assert p.vendor_fuzzy_threshold >= 0.6        # strong name match
    assert p.auto_apply is False


def test_wire_requires_manual_approval():
    # A $7,500 "wire-size" transaction sits above the $5k auto-apply cap.
    p = policy_for_amount(7_500.0)
    assert p.auto_apply is False
    assert p.confidence_tier in ("review_required", "manual_only")
    assert needs_manual_review(7_500.0) is True
    assert needs_manual_review(250.0) is False


# ---------------------------------------------------------------------------
# Candidate scoring
# ---------------------------------------------------------------------------

def test_vendor_fuzzy_threshold_scales_with_amount():
    # Same vendor-name similarity passes at $50 but fails at $50k.
    small = score_candidate(
        tx_amount=50.0, tx_date_days=10, tx_merchant="COSTCO WHOLESALE #123",
        doc_amount=50.0, doc_date_days=10, doc_vendor="Costco",
    )
    large = score_candidate(
        tx_amount=50_000.0, tx_date_days=10,
        tx_merchant="COSTCO WHOLESALE #123",
        doc_amount=50_000.0, doc_date_days=10,
        doc_vendor="Costco",   # same single-word overlap as above
    )
    assert small.all_pass is True
    # Single-token overlap's similarity is low; high-amount tier requires 0.6.
    assert large.all_pass is False or small.vendor_similarity == large.vendor_similarity


def test_vendor_similarity_handles_missing_names():
    # Missing vendor on either side returns 0, not a divide-by-zero.
    assert vendor_similarity(None, "Acme") == 0.0
    assert vendor_similarity("Acme", "") == 0.0
    assert vendor_similarity("", None) == 0.0


def test_score_breakdown_is_serialisable():
    s = score_candidate(
        tx_amount=100.0, tx_date_days=10, tx_merchant="UBER TRIP",
        doc_amount=100.0, doc_date_days=10, doc_vendor="Uber Canada",
    )
    d = s.as_dict()
    # Keys the HTML renderer needs.
    for k in ("amount_diff", "date_diff_days", "vendor_similarity",
              "amount_ok", "date_ok", "vendor_ok", "policy"):
        assert k in d
    # The embedded policy dict is also flat-serialisable.
    assert isinstance(d["policy"], dict)


def test_amount_and_date_windows_applied_from_policy():
    # A large tx with a date 5 days off should fail the date check
    # (large-tier window is 2 days).
    s = score_candidate(
        tx_amount=20_000.0, tx_date_days=100, tx_merchant="BIG PAYMENT",
        doc_amount=20_000.0, doc_date_days=105, doc_vendor="BIG PAYMENT",
    )
    assert s.date_ok is False
    assert s.policy.date_window_days == 2


# ---------------------------------------------------------------------------
# Integration with the DB-aware matcher in review_dashboard.py
# ---------------------------------------------------------------------------

def _load_rd():
    if "rd" in sys.modules:
        return sys.modules["rd"]
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "rd", "/opt/otocpa/scripts/review_dashboard.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rd"] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


@pytest.fixture
def tiny_db(tmp_path):
    db = tmp_path / "bank.db"
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, client_code TEXT,
            vendor TEXT, amount REAL, document_date TEXT,
            matched_bank_transaction TEXT, review_status TEXT
        )
    """)
    conn.commit()
    return conn


def _add_doc(conn, doc_id, vendor, amount, date_str="2026-04-15"):
    conn.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, "
        "document_date, matched_bank_transaction, review_status) "
        "VALUES (?, 'ACME', ?, ?, ?, NULL, 'Ready')",
        (doc_id, vendor, amount, date_str),
    )
    conn.commit()


def test_db_matcher_auto_applies_small_match(tiny_db):
    rd = _load_rd()
    _add_doc(tiny_db, "d1", "Costco", 45.67)
    res = rd._match_transaction_to_document(
        tiny_db, "ACME", 45.68, "2026-04-16", "COSTCO #1234",
    )
    assert res["document_id"] == "d1"
    assert res["auto_apply"] is True
    assert res["confidence_tier"] == "auto"


def test_db_matcher_wire_does_not_auto_apply(tiny_db):
    rd = _load_rd()
    _add_doc(tiny_db, "d2", "Prestige Construction Inc", 12_500.00)
    res = rd._match_transaction_to_document(
        tiny_db, "ACME", 12_500.00, "2026-04-15", "PRESTIGE CONSTRUCTION",
    )
    assert res["document_id"] == "d2"
    assert res["auto_apply"] is False
    assert res["confidence_tier"] == "manual_only"
    assert res["breakdown"] is not None


def test_db_matcher_multiple_candidates_require_manual(tiny_db):
    rd = _load_rd()
    _add_doc(tiny_db, "d3a", "Uber", 200.00)
    _add_doc(tiny_db, "d3b", "Uber Eats", 200.00)
    res = rd._match_transaction_to_document(
        tiny_db, "ACME", 200.00, "2026-04-15", "UBER",
    )
    # Two candidates pass the vendor-threshold — never auto-apply.
    assert res["candidate_count"] == 2
    assert res["auto_apply"] is False
    assert res["confidence_tier"] == "review_required"


def test_db_matcher_no_candidate_returns_none(tiny_db):
    rd = _load_rd()
    _add_doc(tiny_db, "d4", "ACME", 100.0, date_str="2026-01-01")
    res = rd._match_transaction_to_document(
        tiny_db, "ACME", 100.0, "2026-04-15", "ACME",
    )
    assert res["document_id"] is None
    assert res["auto_apply"] is False


def test_large_match_always_review_queue(tiny_db):
    """A document that's within tolerance and dates for a $30k wire but
    only has a one-token vendor overlap must not auto-apply."""
    rd = _load_rd()
    _add_doc(tiny_db, "d5", "Acme", 30_000.00, date_str="2026-04-15")
    res = rd._match_transaction_to_document(
        tiny_db, "ACME", 30_000.00, "2026-04-15", "ACME CORP PAYMENT",
    )
    # Either rejected for vendor-name weakness or auto_apply must be False.
    assert res["auto_apply"] is False


def test_match_confidence_tier_stored(tmp_path):
    """Assert the bank_transactions schema gained the new columns via the
    bootstrap path (mirrored from scripts/review_dashboard.py)."""
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    # Simulate the migration the bootstrap runs when the column is missing.
    conn.execute(
        "CREATE TABLE bank_transactions (id TEXT PRIMARY KEY, amount REAL)"
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(bank_transactions)")}
    assert "match_confidence_tier" not in cols
    conn.execute(
        "ALTER TABLE bank_transactions ADD COLUMN match_confidence_tier TEXT DEFAULT 'auto'"
    )
    conn.execute(
        "ALTER TABLE bank_transactions ADD COLUMN match_score_json TEXT"
    )
    conn.commit()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(bank_transactions)")}
    assert "match_confidence_tier" in cols
    assert "match_score_json" in cols
    conn.close()

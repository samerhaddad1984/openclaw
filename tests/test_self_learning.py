"""Sprint A — self-learning tests.

Covers:
- Correction logging for vendor / GL / tax_code fields.
- Vendor alias application at extraction time.
- Confidence / support-count thresholds.
- Firm scope isolation (Firm A corrections don't apply for Firm B).
- GL suggestion flow.
- Persistence across processes (survives "restart").
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.engines import self_learning as sl


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "learning.db"
    # Minimal documents table so the vendor_hint lookup path works.
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE documents ("
        "document_id TEXT PRIMARY KEY, vendor TEXT)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(sl, "DB_PATH", path)
    sl.ensure_schema(path)
    return path


# ---------------------------------------------------------------------------
# Correction logging
# ---------------------------------------------------------------------------

def test_correction_logged_on_vendor_change(db):
    res = sl.record_correction(
        document_id="doc_a", field="vendor",
        old_value="unprix", new_value="Uniprix",
        corrected_by="sam", firm_code="FIRM_A",
    )
    assert res["correction_logged"] is True
    assert res["vendor_learning"] is True
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT extracted_vendor, canonical_vendor, correction_count "
                         "FROM vendor_learning").fetchall()
    logs = conn.execute("SELECT field, old_value, new_value FROM correction_log").fetchall()
    conn.close()
    assert rows == [("unprix", "Uniprix", 1)]
    assert logs == [("vendor", "unprix", "Uniprix")]


def test_correction_logged_on_gl_change(db):
    # Need a vendor to attribute the GL correction to.
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO documents VALUES ('doc_b', 'Starbucks')")
    conn.commit()
    conn.close()

    res = sl.record_correction(
        document_id="doc_b", field="gl_account",
        old_value="5440", new_value="5410",
        corrected_by="sam", firm_code="FIRM_A",
    )
    assert res["gl_learning"] is True

    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT canonical_vendor, gl_account, correction_count FROM vendor_gl_learning"
    ).fetchall()
    conn.close()
    assert rows == [("Starbucks", "5410", 1)]


def test_correction_logged_on_tax_code_change(db):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO documents VALUES ('doc_t', 'Costco')")
    conn.commit()
    conn.close()
    sl.record_correction(
        document_id="doc_t", field="gl_account", old_value=None, new_value="5440",
        firm_code="FIRM_A",
    )
    sl.record_correction(
        document_id="doc_t", field="tax_code", old_value="", new_value="T",
        firm_code="FIRM_A",
    )
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT gl_account, tax_code, correction_count FROM vendor_gl_learning "
        "WHERE canonical_vendor='Costco' ORDER BY id"
    ).fetchall()
    conn.close()
    # Two rows: one with just the GL, one with the same GL + tax_code pair.
    glaccounts = {r[0] for r in rows}
    taxcodes = {r[1] for r in rows}
    assert "5440" in glaccounts
    assert "T" in taxcodes


def test_no_correction_recorded_when_values_equal(db):
    res = sl.record_correction(
        document_id="doc_x", field="vendor",
        old_value="Aux Vivres", new_value="Aux Vivres",
    )
    assert res["correction_logged"] is False
    conn = sqlite3.connect(db)
    n = conn.execute("SELECT COUNT(*) FROM correction_log").fetchone()[0]
    conn.close()
    assert n == 0


def test_correction_log_timestamps(db):
    sl.record_correction(
        document_id="doc_t1", field="vendor",
        old_value="oldco", new_value="NewCo", firm_code="FIRM_A",
    )
    conn = sqlite3.connect(db)
    ts = conn.execute("SELECT created_at FROM correction_log").fetchone()[0]
    conn.close()
    assert ts and len(ts) >= 10  # 'YYYY-MM-DD HH:MM:SS'


# ---------------------------------------------------------------------------
# Applying learned aliases
# ---------------------------------------------------------------------------

def test_vendor_alias_applied_on_next_extraction(db):
    # One correction is below the floor (MIN=2) — pass-through.
    sl.record_correction(
        document_id="d1", field="vendor",
        old_value="unprix", new_value="Uniprix", firm_code="FIRM_A",
    )
    canonical, original = sl.apply_vendor_learning("unprix", "FIRM_A", db_path=db)
    assert canonical == "unprix"
    assert original is None

    # Second correction promotes the alias — now it gets applied.
    sl.record_correction(
        document_id="d2", field="vendor",
        old_value="unprix", new_value="Uniprix", firm_code="FIRM_A",
    )
    canonical, original = sl.apply_vendor_learning("unprix", "FIRM_A", db_path=db)
    assert canonical == "Uniprix"
    assert original == "unprix"


def test_multiple_corrections_increase_confidence(db):
    for _ in range(5):
        sl.record_correction(
            document_id="d", field="vendor",
            old_value="starbux", new_value="Starbucks", firm_code="FIRM_A",
        )
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT correction_count, confidence FROM vendor_learning "
        "WHERE extracted_vendor='starbux'"
    ).fetchone()
    conn.close()
    count, conf = row
    assert count == 5
    assert conf >= 1.0  # Clamped at 1.0.


def test_firm_scope_isolation(db):
    sl.record_correction(
        document_id="d1", field="vendor",
        old_value="unprix", new_value="Uniprix", firm_code="FIRM_A",
    )
    sl.record_correction(
        document_id="d2", field="vendor",
        old_value="unprix", new_value="Uniprix", firm_code="FIRM_A",
    )
    # FIRM_A has enough support → alias applies.
    a, _ = sl.apply_vendor_learning("unprix", "FIRM_A", db_path=db)
    assert a == "Uniprix"
    # FIRM_B has never corrected this string → pass-through.
    b, _ = sl.apply_vendor_learning("unprix", "FIRM_B", db_path=db)
    assert b == "unprix"


def test_confidence_threshold_prevents_low_quality_suggestions(db):
    # Single correction — below the default MIN=2 floor.
    sl.record_correction(
        document_id="d", field="vendor",
        old_value="xyz", new_value="XYZ Corp", firm_code="FIRM_A",
    )
    v, _ = sl.apply_vendor_learning("xyz", "FIRM_A", db_path=db)
    assert v == "xyz"
    # Explicitly requesting min_corrections=1 does apply it.
    v2, _ = sl.apply_vendor_learning("xyz", "FIRM_A", db_path=db, min_corrections=1)
    assert v2 == "XYZ Corp"


# ---------------------------------------------------------------------------
# GL suggestion
# ---------------------------------------------------------------------------

def test_gl_suggestion_when_vendor_known(db):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO documents VALUES ('d1', 'Starbucks')")
    conn.execute("INSERT INTO documents VALUES ('d2', 'Starbucks')")
    conn.commit()
    conn.close()
    for did in ("d1", "d2"):
        sl.record_correction(
            document_id=did, field="gl_account",
            old_value="5440", new_value="5410", firm_code="FIRM_A",
        )
    suggest = sl.suggest_gl_for_vendor("Starbucks", "FIRM_A", db_path=db)
    assert suggest == {"gl_account": "5410", "tax_code": None}


def test_no_suggestion_when_vendor_unknown(db):
    # Nothing learned — no suggestion.
    assert sl.suggest_gl_for_vendor("Unknown", "FIRM_A", db_path=db) is None


def test_line_item_suggestions_only_fill_empty(db):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO documents VALUES ('d1', 'Costco')")
    conn.execute("INSERT INTO documents VALUES ('d2', 'Costco')")
    conn.commit()
    conn.close()
    for did in ("d1", "d2"):
        sl.record_correction(
            document_id=did, field="gl_account",
            old_value="5440", new_value="5640", firm_code="FIRM_A",
        )
        sl.record_correction(
            document_id=did, field="tax_code",
            old_value="", new_value="T", firm_code="FIRM_A",
        )
    lines = [
        {"description": "Coffee beans", "gl_account": "5400"},  # already assigned
        {"description": "Snacks"},                                # empty
    ]
    sl.suggest_line_item_gl(lines, "Costco", "FIRM_A", db_path=db)
    assert "gl_account_suggested" not in lines[0]  # untouched
    assert lines[1]["gl_account_suggested"] == "5640"
    assert lines[1]["gl_source"] == "learned"


# ---------------------------------------------------------------------------
# Persistence across processes ("restart")
# ---------------------------------------------------------------------------

def test_learning_survives_service_restart(db, tmp_path, monkeypatch):
    sl.record_correction(
        document_id="d1", field="vendor",
        old_value="oldco", new_value="NewCo", firm_code="FIRM_A",
    )
    sl.record_correction(
        document_id="d2", field="vendor",
        old_value="oldco", new_value="NewCo", firm_code="FIRM_A",
    )
    # Simulate a restart by reimporting the module under a fresh
    # module namespace — the SQLite file is the durable surface.
    import importlib
    importlib.reload(sl)
    monkeypatch.setattr(sl, "DB_PATH", db)
    v, _ = sl.apply_vendor_learning("oldco", "FIRM_A", db_path=db)
    assert v == "NewCo"


# ---------------------------------------------------------------------------
# Summary / dashboard helpers
# ---------------------------------------------------------------------------

def test_learning_summary_counts(db):
    sl.record_correction(
        document_id="d1", field="vendor",
        old_value="a", new_value="A Corp", firm_code="FIRM_A",
    )
    sl.record_correction(
        document_id="d1", field="vendor",
        old_value="b", new_value="B Corp", firm_code="FIRM_A",
    )
    s = sl.learning_summary(db_path=db)
    assert s["vendor_aliases"]["distinct"] == 2
    assert s["corrections_all"] == 2
    assert s["recent_by_field"].get("vendor") == 2


def test_top_vendor_corrections_firm_filter(db):
    for firm in ("FIRM_A", "FIRM_B"):
        sl.record_correction(
            document_id=f"d_{firm}", field="vendor",
            old_value="x", new_value=f"X-{firm}", firm_code=firm,
        )
    a = sl.top_vendor_corrections(firm_code="FIRM_A", db_path=db)
    b = sl.top_vendor_corrections(firm_code="FIRM_B", db_path=db)
    assert len(a) == 1 and a[0]["canonical_vendor"] == "X-FIRM_A"
    assert len(b) == 1 and b[0]["canonical_vendor"] == "X-FIRM_B"


def test_apply_vendor_learning_with_null_input(db):
    v, o = sl.apply_vendor_learning(None, "FIRM_A", db_path=db)
    assert v is None and o is None
    v, o = sl.apply_vendor_learning("", "FIRM_A", db_path=db)
    assert v == "" and o is None

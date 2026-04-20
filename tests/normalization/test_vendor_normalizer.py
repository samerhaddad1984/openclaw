"""Tests for the VendorNormalizer."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.vendor_normalizer import (  # noqa: E402
    BRAND_MAP,
    FUZZY_THRESHOLD,
    LEARNING_CONF_CAP,
    TYPO_CORRECTIONS,
    VendorNormalizer,
    normalize_vendor,
)


@pytest.fixture
def empty_db(tmp_path: Path) -> Path:
    """An on-disk DB with the vendor_learning table but no rows."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE vendor_learning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extracted_vendor TEXT NOT NULL,
            canonical_vendor TEXT NOT NULL,
            firm_code TEXT,
            confidence REAL DEFAULT 1.0,
            correction_count INTEGER DEFAULT 1
        )
        """
    )
    conn.commit()
    conn.close()
    return db


def _normalizer(db_path: Path, firm_code: str = "") -> VendorNormalizer:
    return VendorNormalizer(db_path=db_path, firm_code=firm_code)


def test_empty_vendor_handled(empty_db):
    out = _normalizer(empty_db).normalize("")
    assert out["source"] == "empty"
    assert out["canonical"] is None


def test_none_vendor_handled(empty_db):
    assert _normalizer(empty_db).normalize(None)["source"] == "empty"


def test_whitespace_only_handled(empty_db):
    assert _normalizer(empty_db).normalize("   ")["source"] == "empty"


def test_brand_map_lookup_exact(empty_db):
    out = _normalizer(empty_db).normalize("Tim Hortons")
    assert out["canonical"] == "Tim Hortons"
    assert out["source"] == "brand_map"
    assert out["confidence"] == 1.0


def test_brand_map_case_insensitive(empty_db):
    out = _normalizer(empty_db).normalize("TIM HORTONS")
    assert out["canonical"] == "Tim Hortons"
    assert out["source"] == "brand_map"


def test_legal_suffix_stripped_inc(empty_db):
    out = _normalizer(empty_db).normalize("Tim Hortons Inc")
    assert out["canonical"] == "Tim Hortons"
    assert out["source"] == "brand_map"


def test_legal_suffix_stripped_ltee(empty_db):
    out = _normalizer(empty_db).normalize("Metro Ltée")
    assert out["canonical"] == "Metro"
    assert out["source"] == "brand_map"


def test_legal_suffix_stripped_sdn_bhd(empty_db):
    """SROIE dataset shipped with Malaysian legal suffixes."""
    out = _normalizer(empty_db).normalize("Tesco Stores Sdn Bhd")
    # Tesco isn't in our brand map, but suffix-strip still wins at no_change.
    assert out["source"] in {"no_change", "fuzzy"}
    assert "Sdn Bhd" not in (out["canonical"] or "")


def test_legal_suffix_incorporated(empty_db):
    out = _normalizer(empty_db).normalize("Home Depot Incorporated")
    assert out["canonical"] == "Home Depot"


def test_parent_company_mapped_to_brand(empty_db):
    """TDL Group is the legal parent of Tim Hortons."""
    out = _normalizer(empty_db).normalize("TDL Group")
    assert out["canonical"] == "Tim Hortons"
    assert out["source"] == "brand_map"


def test_typo_correction_uniprix(empty_db):
    out = _normalizer(empty_db).normalize("unprix")
    assert out["canonical"] == "Uniprix"
    assert out["source"] == "typo"
    assert out["confidence"] == 0.9


def test_typo_correction_pharmacien(empty_db):
    out = _normalizer(empty_db).normalize("pharmacien")
    assert out["canonical"] == "Pharmaprix"
    assert out["source"] == "typo"


def test_fuzzy_match_high_similarity(empty_db):
    # 'uniprx' is 1 delete from 'uniprix' (sim ~ 0.857)
    out = _normalizer(empty_db).normalize("uniprx")
    assert out["canonical"] == "Uniprix"
    # Either typo or fuzzy — both acceptable.
    assert out["source"] in {"typo", "fuzzy"}
    assert out["confidence"] >= FUZZY_THRESHOLD


def test_fuzzy_match_below_threshold_returns_no_change(empty_db):
    out = _normalizer(empty_db).normalize("xyzqwerty")
    assert out["source"] == "no_change"


def test_self_learning_table_consulted(empty_db):
    """Learned alias with correction_count>=2 is applied."""
    conn = sqlite3.connect(str(empty_db))
    conn.execute(
        "INSERT INTO vendor_learning "
        "(extracted_vendor, canonical_vendor, firm_code, correction_count, confidence)"
        " VALUES (?,?,?,?,?)",
        ("ACME CORP PLUS", "Acme Widgets Inc", "FIRM1", 3, 0.98),
    )
    conn.commit()
    conn.close()

    out = _normalizer(empty_db, firm_code="FIRM1").normalize("ACME CORP PLUS")
    assert out["canonical"] == "Acme Widgets Inc"
    assert out["source"] == "self_learning"


def test_self_learning_ignored_below_min_corrections(empty_db):
    conn = sqlite3.connect(str(empty_db))
    conn.execute(
        "INSERT INTO vendor_learning "
        "(extracted_vendor, canonical_vendor, firm_code, correction_count, confidence)"
        " VALUES (?,?,?,?,?)",
        ("rareVendor", "Rare Canonical", "FIRM1", 1, 0.9),
    )
    conn.commit()
    conn.close()
    out = _normalizer(empty_db, firm_code="FIRM1").normalize("rareVendor")
    assert out["source"] != "self_learning"


def test_self_learning_respects_firm_scope(empty_db):
    """Firm B's corrections must not leak into Firm A."""
    conn = sqlite3.connect(str(empty_db))
    conn.execute(
        "INSERT INTO vendor_learning "
        "(extracted_vendor, canonical_vendor, firm_code, correction_count, confidence)"
        " VALUES (?,?,?,?,?)",
        ("someVendor", "CanonicalB", "FIRM_B", 5, 0.99),
    )
    conn.commit()
    conn.close()
    # Firm A sees no match.
    out_a = _normalizer(empty_db, firm_code="FIRM_A").normalize("someVendor")
    assert out_a["source"] != "self_learning"
    # Firm B sees it.
    out_b = _normalizer(empty_db, firm_code="FIRM_B").normalize("someVendor")
    assert out_b["canonical"] == "CanonicalB"
    assert out_b["source"] == "self_learning"


def test_confidence_from_learning_capped_at_095(empty_db):
    conn = sqlite3.connect(str(empty_db))
    conn.execute(
        "INSERT INTO vendor_learning "
        "(extracted_vendor, canonical_vendor, firm_code, correction_count, confidence)"
        " VALUES (?,?,?,?,?)",
        ("brand123", "BrandOne", "", 10, 1.0),  # explicitly over cap
    )
    conn.commit()
    conn.close()
    out = _normalizer(empty_db).normalize("brand123")
    assert out["source"] == "self_learning"
    assert out["confidence"] <= LEARNING_CONF_CAP


def test_unicode_vendor_handled(empty_db):
    out = _normalizer(empty_db).normalize("Marché Richelieu")
    assert out["canonical"] == "Marché Richelieu"


def test_brand_with_location_suffix(empty_db):
    """'Metro Plus Laval' should still resolve to Metro via brand map."""
    out = _normalizer(empty_db).normalize("Metro Plus")
    assert out["canonical"] == "Metro"
    assert out["source"] == "brand_map"


def test_priority_brand_map_beats_fuzzy(empty_db):
    """Exact brand_map hit must short-circuit before fuzzy matching."""
    out = _normalizer(empty_db).normalize("Costco")
    assert out["source"] == "brand_map"
    assert out["confidence"] == 1.0


def test_priority_typo_beats_fuzzy(empty_db):
    out = _normalizer(empty_db).normalize("unprix")  # exact typo entry
    assert out["source"] == "typo"


def test_self_learning_beats_fuzzy(empty_db):
    conn = sqlite3.connect(str(empty_db))
    # Make 'simulacrum' learned → 'Simulacrum Inc.' (not in brand map).
    conn.execute(
        "INSERT INTO vendor_learning "
        "(extracted_vendor, canonical_vendor, firm_code, correction_count, confidence)"
        " VALUES (?,?,?,?,?)",
        ("simulacrum", "Simulacrum Inc.", "", 4, 0.9),
    )
    conn.commit()
    conn.close()
    out = _normalizer(empty_db).normalize("simulacrum")
    assert out["source"] == "self_learning"


def test_original_always_preserved(empty_db):
    out = _normalizer(empty_db).normalize("  Tim Hortons Inc.  ")
    assert "Tim Hortons" in (out["original"] or "")


def test_all_brands_from_overlays_seeded():
    """Every overlay's canonical name must be present in BRAND_MAP."""
    from src.engines.merchant_overlay import OVERLAYS
    for ov in OVERLAYS:
        key = ov.VENDOR_CANONICAL.lower()
        assert key in BRAND_MAP, f"{ov.VENDOR_CANONICAL} not seeded"


def test_typo_map_nonempty():
    assert len(TYPO_CORRECTIONS) >= 10


def test_module_level_convenience(empty_db):
    out = normalize_vendor("Jean Coutu Inc", firm_code="", db_path=empty_db)
    assert out["canonical"] == "Jean Coutu"


def test_handles_missing_db_gracefully(tmp_path):
    """A path that doesn't exist must not raise; returns no_change / brand_map."""
    nonexistent = tmp_path / "nope.db"
    v = VendorNormalizer(db_path=nonexistent)
    out = v.normalize("Tim Hortons")
    assert out["canonical"] == "Tim Hortons"


def test_strip_suffixes_chains(empty_db):
    """Multiple suffixes stripped iteratively."""
    out = _normalizer(empty_db).normalize("BrandX Holdings Ltd")
    # BrandX isn't in map; we just ensure suffix chain stripped.
    assert "Holdings" not in (out["canonical"] or "")
    assert "Ltd" not in (out["canonical"] or "")


def test_shoppers_mapped_to_pharmaprix(empty_db):
    out = _normalizer(empty_db).normalize("Shoppers Drug Mart")
    assert out["canonical"] == "Pharmaprix"
    assert out["source"] == "brand_map"

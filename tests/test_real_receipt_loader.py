"""Unit tests for chaos.generators.real_receipt_loader."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest


def test_parse_cord_amount_thousands_separator():
    from chaos.generators.real_receipt_loader import _parse_cord_amount

    assert _parse_cord_amount("60.000") == Decimal("60000")
    assert _parse_cord_amount("174.600") == Decimal("174600")
    assert _parse_cord_amount("5.455") == Decimal("5455")


def test_parse_cord_amount_comma_thousands():
    from chaos.generators.real_receipt_loader import _parse_cord_amount

    assert _parse_cord_amount("28,000") == Decimal("28000")
    assert _parse_cord_amount("165,000.00") == Decimal("165000.00")


def test_parse_cord_amount_handles_noise():
    from chaos.generators.real_receipt_loader import _parse_cord_amount

    assert _parse_cord_amount("Rp 60.000") == Decimal("60000")
    assert _parse_cord_amount("") is None
    assert _parse_cord_amount(None) is None


def test_cord_loader_has_receipts():
    from chaos.generators.real_receipt_loader import (
        DATASETS_ROOT,
        RealReceiptLoader,
    )

    base = DATASETS_ROOT / "cord"
    if not base.exists() or not any(base.glob("ground_truth/*.json")):
        pytest.skip("CORD fixtures not present in this checkout")
    loader = RealReceiptLoader("cord")
    assert len(loader) > 0
    stats = loader.stats()
    assert stats["total"] == len(loader)
    # Difficulty bucket should contain at least one non-empty bucket
    assert any(v > 0 for v in stats["by_difficulty"].values())


def test_cord_loader_scenario_has_skip_fields():
    from chaos.generators.real_receipt_loader import (
        DATASETS_ROOT,
        RealReceiptLoader,
    )

    base = DATASETS_ROOT / "cord"
    if not base.exists() or not any(base.glob("ground_truth/*.json")):
        pytest.skip("CORD fixtures not present")
    loader = RealReceiptLoader("cord")
    sc = loader.receipts[0].to_scenario()
    skip = set(sc["input_spec"]["skip_fields"])
    assert "gst" in skip and "qst" in skip
    assert "vendor" in skip and "document_date" in skip
    # CORD images have a local image_path the runner can read
    assert Path(sc["input_spec"]["image_path"]).exists()


def test_receipt_oracle_locale_scaled_amount():
    """CORD's ``60.000`` must match an AI-extracted ``60.0``."""
    from chaos.oracles.receipt_oracle import _amount_close_scaled

    assert _amount_close_scaled("60.0", "60000") is True
    assert _amount_close_scaled("46.0", "46000") is True
    assert _amount_close_scaled("46.00", "46000") is True
    # Actual mismatch should still fail
    assert _amount_close_scaled("31000", "91000") is False


def test_receipt_oracle_skip_fields_redistribute_weight():
    """When skip_fields drop half the weight, remaining fields still sum to 100."""
    from chaos.oracles.receipt_oracle import ReceiptOracle

    oracle = ReceiptOracle()
    gt = {"total": "100.00", "line_count": 1}
    extracted = {"total": "100.00", "line_count": 1}
    r = oracle.validate(
        extracted, gt,
        skip_fields=["vendor", "document_date", "gst", "qst",
                     "subtotal", "currency", "tax_code"],
    )
    assert r.passed
    assert r.total_score == pytest.approx(100.0, rel=0.01)

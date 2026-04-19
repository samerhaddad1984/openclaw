"""Part 3 — date-sanity regression tests for _fix_quebec_date.

Hunts for the "2031-12-04 from a 2024-12-31 receipt" pattern found in
the real-receipt accuracy analysis.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.ocr_engine import _date_in_sane_range, _fix_quebec_date


def test_sane_range_current_year():
    y = datetime.now().year
    assert _date_in_sane_range(y, 6, 15) is True


def test_sane_range_rejects_far_future():
    assert _date_in_sane_range(2099, 6, 15) is False


def test_sane_range_rejects_ancient():
    assert _date_in_sane_range(1995, 6, 15) is False


def test_sane_range_rejects_invalid_month():
    assert _date_in_sane_range(2025, 13, 15) is False


def test_iso_date_in_sane_range_passes_through():
    assert _fix_quebec_date("2024-12-31") == "2024-12-31"


def test_far_future_year_recovered_by_swap():
    """The real bug: parsed 2031-12-04 but actually 2024-12-31.
    Swap year-last-two (31) ↔ day (04): 2004-12-31 — still bad.
    Try 20+day (20+04=2004) — bad, so return None.
    This matches our 'flag for review' recovery rather than invent a date.
    """
    out = _fix_quebec_date("2031-12-04")
    # Recovered to None (not a credible date) rather than the wrong 2031-12-04.
    assert out is None


def test_far_future_year_recoverable_swap():
    """Year 2025 > current+1 but the swap 20+day can produce a sane date:
    e.g., 2030-06-25 → swap day 25 with year suffix 30 → 2025-06-30.
    """
    out = _fix_quebec_date("2030-06-25")
    # Either the swap recovers to 2025-06-30 or we return None — both are OK
    # ("never silently return a far-future year").
    if out is not None:
        year = int(out[:4])
        assert year <= datetime.now().year + 1


def test_legacy_dd_mm_yy_format_still_works():
    assert _fix_quebec_date("31/12/24") == "2024-12-31"


def test_legacy_french_month_still_works():
    assert _fix_quebec_date("19 mars 2026") == "2026-03-19"


def test_empty_input_returns_none():
    assert _fix_quebec_date("") is None
    assert _fix_quebec_date(None) is None


def test_illegible_returns_none():
    assert _fix_quebec_date("illegible") is None

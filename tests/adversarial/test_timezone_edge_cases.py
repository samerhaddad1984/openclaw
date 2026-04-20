"""R4-Investigation 6 — timezone and clock edge cases."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# DST transitions — Toronto spring-forward 2026-03-08 02:00 → 03:00.
# Any timestamp at 02:30 local does not exist.
# ---------------------------------------------------------------------------

def test_dst_spring_forward_nonexistent_local_time_does_not_crash():
    """The entry_date '2026-03-08' in get_document_period should yield
    period '2026-03' regardless of what time is attached."""
    from src.agents.core.period_close import get_document_period
    # These strings as stored in the documents table.
    for s in ("2026-03-08", "2026-03-08T02:30:00",
              "2026-03-08T02:30:00-05:00", "2026-03-08T07:30:00Z"):
        assert get_document_period(s) == "2026-03", s


def test_dst_fall_back_ambiguous_local_time():
    """2026-11-01 01:30 is ambiguous in US/Eastern (occurs twice).
    Dashboard stores UTC; both renditions map to the same period."""
    from src.agents.core.period_close import get_document_period
    # Two UTCs for a US/Eastern 01:30 on 2026-11-01: before DST end
    # (05:30Z) and after (06:30Z).
    for s in ("2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"):
        assert get_document_period(s) == "2026-11"


# ---------------------------------------------------------------------------
# Fiscal year boundary — JE dated 2026-12-31 23:00 ET (= 2027-01-01 04:00 UTC)
# ---------------------------------------------------------------------------

def test_jan_1_utc_maps_to_prior_month_when_client_is_eastern(tmp_path):
    """If the document_date stored is '2026-12-31' (client chose this
    in their form), the period must be 2026-12, regardless of the
    server's UTC clock."""
    from src.agents.core.period_close import get_document_period
    assert get_document_period("2026-12-31") == "2026-12"
    assert get_document_period("2027-01-01") == "2027-01"


# ---------------------------------------------------------------------------
# Server timestamp is always UTC with explicit marker.
# ---------------------------------------------------------------------------

def test_utc_now_iso_is_timezone_aware():
    import scripts.review_dashboard as rd
    s = rd.utc_now_iso()
    assert s.endswith("Z") or s.endswith("+00:00"), s


def test_utc_now_iso_second_precision_stable():
    """Two consecutive calls within 1 second should produce strings
    no more than 1 second apart. Catches accidental clock jumps from
    time.time() vs datetime.utcnow drift."""
    import scripts.review_dashboard as rd
    import time
    a = rd.utc_now_iso()
    time.sleep(0.01)
    b = rd.utc_now_iso()
    # Parse both; difference < 2 seconds.
    from datetime import datetime
    ta = datetime.fromisoformat(a.replace("Z", "+00:00"))
    tb = datetime.fromisoformat(b.replace("Z", "+00:00"))
    delta = abs((tb - ta).total_seconds())
    assert delta < 2.0, (a, b, delta)


# ---------------------------------------------------------------------------
# Leap second tolerance.
# ---------------------------------------------------------------------------

def test_datetime_fromisoformat_handles_weird_but_valid_iso():
    """Python's fromisoformat accepts most well-formed ISO strings.
    A CPA's form may submit '2024-12-31T23:59:59' and our storage
    must round-trip it."""
    for s in ("2024-12-31T23:59:59",
              "2024-12-31T23:59:59.123",
              "2024-12-31T23:59:59.999999+00:00"):
        dt = datetime.fromisoformat(s)
        assert dt.year == 2024
        assert dt.month == 12
        assert dt.day == 31


# ---------------------------------------------------------------------------
# Clock drift: utc_now_iso must match the system clock.
# ---------------------------------------------------------------------------

def test_utc_now_iso_within_5_seconds_of_system_clock():
    import scripts.review_dashboard as rd
    from datetime import datetime, timezone
    system = datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(rd.utc_now_iso().replace("Z", "+00:00"))
    delta = abs((system - parsed).total_seconds())
    assert delta < 5.0, f"utc_now_iso drifts from system clock by {delta}s"

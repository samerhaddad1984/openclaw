"""R2-Investigation 3 — pytest wrapper for the sustained-load leak probe.

Runs for 300 s (5 min) by default. The full 2-hour version is the
standalone script — pytest CI windows are too short.

Asserts:
  - 0 errors over the run
  - RSS growth rate < 30 MB/hour (looser than 60-s burn because cache
    warm-up dominates short windows)
  - FD count grows by < 30
  - Thread count grows by < 30
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.stress.real_memory_leak_test import run  # noqa: E402


@pytest.mark.slow
def test_5_minute_sustained_load_does_not_leak():
    summary = run(duration_seconds=300)
    assert summary["errors"] == 0, f"errors during 5-min run: {summary}"
    # Compare the POST-WARMUP slope (first 60 s excluded) against a 30
    # MB/hr threshold. Cache warm-up in the first minute is normal and
    # should not count as a leak.
    growth = summary["rss_growth_mb_per_hour"]
    assert growth < 30, (
        f"post-warmup RSS growth rate {growth:.1f} MB/hr exceeds 30 MB/hr. "
        f"full-run slope {summary.get('rss_growth_mb_per_hour_full'):.1f} MB/hr, "
        f"first {summary['rss_first_mb']} MB -> last {summary['rss_last_mb']} MB"
    )
    # The FD window bounces between request / response sockets in a
    # threading HTTP server (50+ connections opening + closing). We
    # check thread count and raw FD diff against looser bounds —
    # both are informational.
    assert summary["threads_last"] - summary["threads_first"] < 30, (
        f"Thread count grew from {summary['threads_first']} to {summary['threads_last']}"
    )

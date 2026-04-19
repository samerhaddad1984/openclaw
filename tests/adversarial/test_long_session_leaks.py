"""Investigation 6 — short-burn leak probe (60 s).

Proxy for the 8-hour simulated session: if a minute of rapid /login +
/health traffic leaks memory, FDs, or bloats the DB in detectable
amounts, something's wrong at the per-request layer.

Thresholds are intentionally loose to avoid flaky failures from noisy
CI boxes. The strict numeric leak analysis is in
``scripts/stress/long_session_test.py`` — run with ``LEAK_SECONDS=1800``
for the full-length run.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.stress.long_session_test import run  # noqa: E402


@pytest.mark.slow
def test_60_second_burn_does_not_leak():
    summary = run(duration_seconds=60)
    assert summary["errors"] == 0, f"request errors during burn: {summary}"
    # RSS growth cap: 100 MB. Below that we consider it normal cache warmup.
    assert summary["rss_growth_mb"] < 100, (
        f"RSS grew by {summary['rss_growth_mb']} MB over 60 s "
        f"— potential memory leak. Summary: {summary}"
    )
    # FD growth cap: 30 handles. Above that = likely unclosed file / socket.
    assert summary["fd_growth"] < 30, (
        f"Open file descriptors grew by {summary['fd_growth']} over 60 s "
        f"— potential FD leak. Summary: {summary}"
    )
    # DB growth cap: 10 MB (each burst is a couple of trivial SELECTs;
    # anything more is uncontrolled write traffic).
    assert summary["final_db_mb"] - summary["baseline_db_mb"] < 10, (
        f"DB grew by {summary['final_db_mb'] - summary['baseline_db_mb']} MB "
        f"over 60 s — runaway inserts? {summary}"
    )

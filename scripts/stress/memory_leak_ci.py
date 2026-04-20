"""CI-friendly 10-minute memory leak probe.

Mirrors the 2-hour ``leak_test_2hr.py`` pipeline in-process but runs
for a much shorter window so it can sit in a pre-merge gate. Exits
non-zero if the post-warmup RSS slope exceeds the threshold, or if
file-descriptor or thread counts grow over the run.

Thresholds are the same as the 2-hour verdict tiers:

- ``RSS`` slope > 80 MB/hr → fail (more forgiving than the 2-hour
  50 MB/hr bound because a 10-minute window has less smoothing).
- ``open_fds`` slope > 10 /hr → fail.
- ``threads`` slope > 2 /hr → fail.

Run::

    python3 scripts/stress/memory_leak_ci.py          # default 600s
    LEAK_CI_SECONDS=180 python3 scripts/stress/memory_leak_ci.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.stress.leak_test_2hr import run  # noqa: E402

RSS_GROWTH_MAX_MB_PER_HOUR = float(os.environ.get("LEAK_CI_RSS_MAX", "80"))
FD_GROWTH_MAX_PER_HOUR = float(os.environ.get("LEAK_CI_FD_MAX", "10"))
THREAD_GROWTH_MAX_PER_HOUR = float(os.environ.get("LEAK_CI_THREAD_MAX", "2"))


def main() -> int:
    duration = int(os.environ.get("LEAK_CI_SECONDS", "600"))
    samples_path = Path(os.environ.get("LEAK_CI_SAMPLES",
                                        "/tmp/memory_leak_ci_samples.jsonl"))
    print(f"[leak-ci] duration={duration}s samples→{samples_path}")

    result = run(
        duration_seconds=duration,
        samples_path=samples_path,
        sample_every_s=10.0,
    )

    rss = result["rss_slope_mb_per_hour"]
    fd = result["fd_slope_per_hour"]
    thr = result["thread_slope_per_hour"]

    print(json.dumps(result, indent=2))

    failed = []
    if rss > RSS_GROWTH_MAX_MB_PER_HOUR:
        failed.append(f"RSS slope {rss} MB/hr > {RSS_GROWTH_MAX_MB_PER_HOUR}")
    if fd > FD_GROWTH_MAX_PER_HOUR:
        failed.append(f"FD slope {fd}/hr > {FD_GROWTH_MAX_PER_HOUR}")
    if thr > THREAD_GROWTH_MAX_PER_HOUR:
        failed.append(f"thread slope {thr}/hr > {THREAD_GROWTH_MAX_PER_HOUR}")

    if failed:
        print("\n=== FAIL ===")
        for f in failed:
            print(f"- {f}")
        return 1

    print("\n=== PASS ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

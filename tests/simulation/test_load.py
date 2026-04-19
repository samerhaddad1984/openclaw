"""Part 6 — simple load test using requests + threading.

Locust failed to install cleanly in this sandbox (broken gevent/zope
dependency chain). This is a self-contained replacement that still
measures p50/p95/p99 latency, error rate, and memory over time.

Scenarios:
  * 50 concurrent "virtual users"
  * each issues a rotating mix of GET requests at ~4 req/sec
  * runs for 30 seconds
  * collects per-request latency + status code + any exceptions

Output: /tmp/load_test_results.md
"""
from __future__ import annotations

import collections
import json
import os
import random
import statistics
import threading
import time
from pathlib import Path

import requests

BASE = "http://127.0.0.1:8787"

ROUTES = [
    "/", "/login",
    "/audit/anomalies", "/partnerships", "/sred", "/tax/planning",
    "/reconciliation/adjustments", "/t2", "/cashflow", "/ar",
    "/aging", "/fixed_assets", "/financial_statements", "/engagements",
    "/audit/sample", "/audit/evidence",
]


def _proc_rss_bytes(pid: int) -> int:
    """Read /proc/<pid>/status VmRSS in bytes (Linux only)."""
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        return 0
    return 0


def _otocpa_pid() -> int | None:
    import subprocess
    r = subprocess.run(
        ["systemctl", "show", "-p", "MainPID", "otocpa"],
        capture_output=True, text=True,
    )
    line = r.stdout.strip()
    if "=" in line:
        v = line.split("=", 1)[1]
        if v.isdigit() and v != "0":
            return int(v)
    return None


def run(duration_s: int = 30, workers: int = 50) -> dict:
    latencies: list[float] = []
    statuses: list[int] = []
    errors: list[str] = []
    lock = threading.Lock()
    stop_at = time.time() + duration_s

    def _worker(i):
        sess = requests.Session()
        rng = random.Random(i * 7919)
        while time.time() < stop_at:
            path = rng.choice(ROUTES)
            t0 = time.perf_counter()
            try:
                r = sess.get(f"{BASE}{path}", timeout=10, allow_redirects=True)
                with lock:
                    latencies.append((time.perf_counter() - t0) * 1000.0)
                    statuses.append(r.status_code)
            except Exception as e:
                with lock:
                    errors.append(str(e))
            time.sleep(rng.uniform(0.1, 0.4))  # ~4 req/sec/user

    # Memory sampling thread.
    pid = _otocpa_pid()
    memory_samples = []
    memory_stop = threading.Event()

    def _mem_watcher():
        while not memory_stop.is_set():
            if pid:
                memory_samples.append((time.time(), _proc_rss_bytes(pid)))
            time.sleep(1)

    memory_thread = threading.Thread(target=_mem_watcher)
    memory_thread.start()

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(workers)]
    start_ts = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    memory_stop.set()
    memory_thread.join(timeout=2)
    elapsed = time.time() - start_ts

    def _pct(p):
        if not latencies:
            return 0
        return statistics.quantiles(sorted(latencies), n=100)[p - 1] if len(latencies) >= 2 else latencies[0]

    status_counts = collections.Counter(statuses)
    summary = {
        "duration_s": round(elapsed, 1),
        "workers": workers,
        "total_requests": len(latencies) + len(errors),
        "successful": len(latencies),
        "errors": len(errors),
        "error_rate_pct": round(100 * len(errors) / max(len(latencies) + len(errors), 1), 2),
        "p50_ms": round(_pct(50), 1),
        "p95_ms": round(_pct(95), 1),
        "p99_ms": round(_pct(99), 1),
        "max_ms": round(max(latencies), 1) if latencies else 0,
        "throughput_rps": round(len(latencies) / max(elapsed, 0.001), 1),
        "status_codes": dict(status_counts),
        "memory_start_mb": round(memory_samples[0][1] / 1_000_000, 1) if memory_samples else 0,
        "memory_end_mb": round(memory_samples[-1][1] / 1_000_000, 1) if memory_samples else 0,
        "memory_peak_mb": round(max(m[1] for m in memory_samples) / 1_000_000, 1) if memory_samples else 0,
        "memory_growth_mb": (
            round((memory_samples[-1][1] - memory_samples[0][1]) / 1_000_000, 1)
            if memory_samples else 0
        ),
        "sample_errors": errors[:5],
    }
    return summary


def main():
    print(f"Load test: 50 workers × 30s against {BASE}")
    summary = run(duration_s=30, workers=50)
    Path("/tmp/load_test_results.json").write_text(
        json.dumps(summary, default=str, indent=2),
    )
    md = [
        "# Part 6 — Load test results",
        "",
        f"**Target:** {BASE}",
        f"**Workers:** {summary['workers']}",
        f"**Duration:** {summary['duration_s']}s",
        "",
        "## Throughput + latency",
        f"- Total requests: **{summary['total_requests']}**",
        f"- Successful: **{summary['successful']}**",
        f"- Errors: **{summary['errors']}** ({summary['error_rate_pct']}%)",
        f"- Throughput: **{summary['throughput_rps']} req/s**",
        f"- Latency p50 / p95 / p99 / max: "
        f"**{summary['p50_ms']} / {summary['p95_ms']} / "
        f"{summary['p99_ms']} / {summary['max_ms']} ms**",
        "",
        "## Status code distribution",
    ]
    for code, n in sorted(summary.get("status_codes", {}).items()):
        md.append(f"- {code}: {n}")
    md.extend([
        "",
        "## Memory (otocpa process)",
        f"- Start: **{summary['memory_start_mb']} MB**",
        f"- End: **{summary['memory_end_mb']} MB**",
        f"- Peak: **{summary['memory_peak_mb']} MB**",
        f"- Growth: **{summary['memory_growth_mb']} MB** "
        f"({'leak suspected' if summary['memory_growth_mb'] > 50 else 'within noise'})",
        "",
    ])
    if summary.get("sample_errors"):
        md.append("## Sample errors")
        for e in summary["sample_errors"]:
            md.append(f"- `{e}`")
    Path("/tmp/load_test_results.md").write_text("\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()

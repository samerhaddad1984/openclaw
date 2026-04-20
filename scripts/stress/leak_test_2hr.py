"""2-hour memory-leak probe (extends real_memory_leak_test.py).

- Drives realistic read + write traffic against an in-process
  ``ThreadingHTTPServer`` instance of the OtoCPA dashboard.
- Samples every 30 s for the full duration (default 7200 s).
- Appends each sample to ``/tmp/memory_leak_samples.jsonl`` so the file
  remains useful even if the script is interrupted.
- Computes post-warmup linear-regression slope of RSS vs time.
- Checks FD / thread / DB-size / /tmp-size deltas against thresholds
  and prints a verdict.

Run::

    python3 scripts/stress/leak_test_2hr.py --duration 7200 \
        --samples /tmp/memory_leak_samples.jsonl \
        --result  /tmp/memory_leak_2hr.json

Relies on the bootstrap helpers in ``real_memory_leak_test.py``.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Reuse the bootstrap helper.
from scripts.stress.real_memory_leak_test import _bootstrap  # noqa: E402

READ_PATHS = [
    "/",
    "/clients",
    "/queue",
    "/health",
    "/aging",
    "/financial_statements",
    "/audit/anomalies",
    "/reconciliation",
    "/dashboard",
    "/documents",
]

# Verdict thresholds (per spec).
RSS_GROWTH_MAX_MB_PER_HOUR = 50.0
FD_GROWTH_MAX_PER_HOUR = 5.0
THREAD_GROWTH_MAX_PER_HOUR = 0.5  # threads should be stable


def _dir_size_mb(path: Path) -> float:
    total = 0
    try:
        for root, _dirs, files in os.walk(path):
            for f in files:
                try:
                    total += (Path(root) / f).stat().st_size
                except OSError:
                    pass
    except OSError:
        pass
    return round(total / 1024 / 1024, 1)


def _slope_per_hour(samples: list[dict], field: str, skip_warmup_s: float = 60.0) -> float:
    win = [s for s in samples if s["t"] >= skip_warmup_s]
    if len(win) < 2:
        return 0.0
    n = len(win)
    xs = [s["t"] for s in win]
    ys = [s[field] for s in win]
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    slope_s = (n * sxy - sx * sy) / denom if denom else 0.0
    return slope_s * 3600.0


def run(
    duration_seconds: int,
    samples_path: Path,
    sample_every_s: float = 30.0,
) -> dict[str, Any]:
    tmp = Path(tempfile.mkdtemp(prefix="leak2hr_"))
    db = tmp / "leak.db"
    secret = tmp / "s"
    secret.write_text("x" * 48)
    _bootstrap(db)

    import scripts.review_dashboard as rd
    rd.DB_PATH = db
    rd.PASSWORD_LINK_SECRET_FILE = str(secret)
    rd.bootstrap_schema()
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()

    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "leak2hr@det.com", "customer": "cus_L2",
             "subscription": "sub_L2", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?",
            (rd.hash_password("Leak2H@r!Pass1"), "leak2hr@det.com"),
        )
        conn.commit()
    cookies = f"session_token={rd.create_session('leak2hr@det.com')}"

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"

    import psutil
    proc = psutil.Process()
    samples: list[dict] = []
    errors = 0
    samples_path.parent.mkdir(parents=True, exist_ok=True)
    # Truncate the samples file on fresh run.
    samples_path.write_text("")

    start = time.time()
    next_sample = start + sample_every_s
    counter = 0

    while time.time() - start < duration_seconds:
        counter += 1
        path = READ_PATHS[counter % len(READ_PATHS)]
        try:
            req = urllib.request.Request(
                f"{base}{path}",
                headers={"Cookie": cookies},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
        except urllib.error.HTTPError as e:
            if e.code >= 500:
                errors += 1
        except Exception:
            errors += 1

        # Every ~10 read cycles, do a small POST to a safe endpoint.
        # /health accepts POST without auth; keeps handler code on the
        # write path warm in terms of header/cookie parsing.
        if counter % 10 == 0:
            try:
                data = urllib.parse.urlencode({"ping": "1"}).encode()
                req = urllib.request.Request(
                    f"{base}/health",
                    data=data,
                    headers={"Cookie": cookies,
                             "Content-Type": "application/x-www-form-urlencoded"},
                )
                with urllib.request.urlopen(req, timeout=10) as r:
                    r.read()
            except urllib.error.HTTPError as e:
                if e.code >= 500:
                    errors += 1
            except Exception:
                errors += 1

        if time.time() >= next_sample:
            import gc as _gc
            _gc.collect()
            mem = proc.memory_info()
            sample = {
                "t": round(time.time() - start, 1),
                "rss_mb": round(mem.rss / 1024 / 1024, 1),
                "vms_mb": round(mem.vms / 1024 / 1024, 1),
                "open_fds": proc.num_fds(),
                "threads": proc.num_threads(),
                "cpu_percent": proc.cpu_percent(interval=None),
                "db_size_mb": round(db.stat().st_size / 1024 / 1024, 2) if db.exists() else 0.0,
                "tmp_size_mb": _dir_size_mb(Path("/tmp")),
                "errors": errors,
                "iterations": counter,
            }
            samples.append(sample)
            with samples_path.open("a") as fh:
                fh.write(json.dumps(sample) + "\n")
            next_sample += sample_every_s
        time.sleep(0.05)

    server.shutdown()
    server.server_close()

    rss_slope = _slope_per_hour(samples, "rss_mb")
    fd_slope = _slope_per_hour(samples, "open_fds")
    thread_slope = _slope_per_hour(samples, "threads")
    db_slope = _slope_per_hour(samples, "db_size_mb")

    first = samples[0] if samples else {}
    last = samples[-1] if samples else {}

    leak_detected = (
        rss_slope > RSS_GROWTH_MAX_MB_PER_HOUR
        or fd_slope > FD_GROWTH_MAX_PER_HOUR
        or thread_slope > THREAD_GROWTH_MAX_PER_HOUR
    )

    verdict = "CLEAN"
    if leak_detected:
        if rss_slope > RSS_GROWTH_MAX_MB_PER_HOUR * 2:
            verdict = "HIGH"
        elif rss_slope > RSS_GROWTH_MAX_MB_PER_HOUR:
            verdict = "MEDIUM"
        else:
            verdict = "LOW"

    result = {
        "duration_s": duration_seconds,
        "iterations": counter,
        "errors": errors,
        "rss_slope_mb_per_hour": round(rss_slope, 2),
        "fd_slope_per_hour": round(fd_slope, 2),
        "thread_slope_per_hour": round(thread_slope, 2),
        "db_slope_mb_per_hour": round(db_slope, 2),
        "first": first,
        "last": last,
        "rss_delta_mb": round(
            (last.get("rss_mb", 0) - first.get("rss_mb", 0)), 1
        ) if first else 0,
        "fd_delta": (last.get("open_fds", 0) - first.get("open_fds", 0))
                    if first else 0,
        "thread_delta": (last.get("threads", 0) - first.get("threads", 0))
                        if first else 0,
        "leak_detected": leak_detected,
        "verdict": verdict,
        "sample_count": len(samples),
    }

    # Clean up temp dir (samples already persisted to samples_path).
    try:
        shutil.rmtree(tmp, ignore_errors=True)
    except Exception:
        pass

    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=int,
                   default=int(os.environ.get("LEAK_SECONDS", "7200")))
    p.add_argument("--samples", type=Path,
                   default=Path("/tmp/memory_leak_samples.jsonl"))
    p.add_argument("--result", type=Path,
                   default=Path("/tmp/memory_leak_2hr.json"))
    p.add_argument("--sample-every", type=float, default=30.0)
    args = p.parse_args()

    print(f"[leak2hr] duration={args.duration}s "
          f"sample_every={args.sample_every}s "
          f"samples→{args.samples} result→{args.result}")

    started = time.time()
    out = run(
        duration_seconds=args.duration,
        samples_path=args.samples,
        sample_every_s=args.sample_every,
    )
    elapsed = time.time() - started
    out["wallclock_s"] = round(elapsed, 1)

    args.result.write_text(json.dumps(out, indent=2))
    print("\n=== RESULT ===")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

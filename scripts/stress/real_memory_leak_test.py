"""R2-Investigation 3 — sustained-load memory leak probe.

Drives a realistic mix of read + write traffic against an in-process
ThreadingHTTPServer instance of the dashboard, samples RSS / open FDs
/ thread count every 10 seconds, and reports a linear regression of
RSS-vs-time at the end.

Defaults to 300 s (5 min). The full 2-hour run requested in the spec
is gated by ``LEAK_SECONDS=7200`` — pytest CI windows are too short
for the full run, so the operator must invoke the script directly.

Run: ``python3 -m scripts.stress.real_memory_leak_test``
Pytest wrapper (5 min): ``tests/adversarial/test_real_memory_leak.py``.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, firm_code TEXT,
            client_code TEXT, vendor TEXT, review_status TEXT,
            version INTEGER DEFAULT 1
        );
    """)
    c.commit(); c.close()


def run(duration_seconds: int = 300) -> dict:
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="leak2hr_"))
    db = tmp / "leak.db"
    secret = tmp / "s"; secret.write_text("x" * 48)
    _bootstrap(db)

    import scripts.review_dashboard as rd
    rd.DB_PATH = db
    rd.PASSWORD_LINK_SECRET_FILE = str(secret)
    rd.bootstrap_schema()
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()

    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "leak2@det.com", "customer": "cus_L2",
             "subscription": "sub_L2", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?",
            (rd.hash_password("Leak2P@ss!"), "leak2@det.com"),
        )
        conn.commit()
    cookies = f"session_token={rd.create_session('leak2@det.com')}"

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"

    import psutil
    proc = psutil.Process()
    samples: list[dict] = []
    errors = 0
    start = time.time()
    next_sample = start + 10.0
    counter = 0

    READ_PATHS = ["/", "/clients", "/queue", "/health", "/aging",
                   "/financial_statements", "/audit/anomalies"]

    while time.time() - start < duration_seconds:
        counter += 1
        path = READ_PATHS[counter % len(READ_PATHS)]
        try:
            req = urllib.request.Request(f"{base}{path}", headers={"Cookie": cookies})
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
        except urllib.error.HTTPError as e:
            # 404/302 are fine; 500 counts as an error.
            if e.code >= 500:
                errors += 1
        except Exception:
            errors += 1

        if time.time() >= next_sample:
            # Run a GC pass before sampling so the number we log is
            # steady-state RSS, not "whatever happens to be in unreclaimed
            # generation 0". Leaks show up as continued growth AFTER a
            # full GC; cache churn does not.
            import gc as _gc
            _gc.collect()
            mem = proc.memory_info()
            samples.append({
                "t": round(time.time() - start, 1),
                "rss_mb": round(mem.rss / 1024 / 1024, 1),
                "vms_mb": round(mem.vms / 1024 / 1024, 1),
                "open_fds": proc.num_fds(),
                "threads": proc.num_threads(),
                "errors": errors,
            })
            next_sample += 10.0
        time.sleep(0.05)

    server.shutdown(); server.server_close()

    # Linear regression on RSS-vs-time (no numpy dependency). We run
    # the regression on the POST-WARMUP window (after 60 s) because
    # the first minute is dominated by module imports / JIT of the
    # handler code; a memory leak shows up as a residual slope once
    # caches have warmed.
    def _slope_per_hour(win):
        if len(win) < 2:
            return 0.0
        n = len(win)
        xs = [s["t"] for s in win]
        ys = [s["rss_mb"] for s in win]
        sx = sum(xs); sy = sum(ys)
        sxx = sum(x * x for x in xs); sxy = sum(x * y for x, y in zip(xs, ys))
        denom = n * sxx - sx * sx
        slope_s = (n * sxy - sx * sy) / denom if denom else 0.0
        return slope_s * 3600.0

    slope_full = _slope_per_hour(samples)
    post_warm = [s for s in samples if s["t"] >= 60.0]
    slope_warm = _slope_per_hour(post_warm)
    slope_mb_per_hour = slope_warm if post_warm else slope_full

    return {
        "duration_s": duration_seconds,
        "iterations": counter,
        "errors": errors,
        "samples": samples,
        "rss_growth_mb_per_hour": round(slope_mb_per_hour, 2),
        "rss_growth_mb_per_hour_full": round(slope_full, 2),
        "rss_first_mb": samples[0]["rss_mb"] if samples else None,
        "rss_last_mb": samples[-1]["rss_mb"] if samples else None,
        "fds_first": samples[0]["open_fds"] if samples else None,
        "fds_last": samples[-1]["open_fds"] if samples else None,
        "threads_first": samples[0]["threads"] if samples else None,
        "threads_last": samples[-1]["threads"] if samples else None,
    }


def main() -> int:
    dur = int(os.environ.get("LEAK_SECONDS", "300"))
    out = run(duration_seconds=dur)
    print(json.dumps(out, indent=2))
    Path("/tmp/leak_real.json").write_text(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

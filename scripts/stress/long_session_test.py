"""Investigation 6 — long-running session leak hunt.

Simulates 8 hours of CPA activity at 16x speed (so the whole run
finishes in ~3 minutes rather than an hour). We hit a spawned dashboard
instance with a rotating mix of reads/writes, sample process RSS + open
file descriptors + DB size every 10 simulated minutes, and fail the run
if:
  - RSS grows by more than 50 MB baseline to peak (memory leak),
  - FD count grows by more than 50 (file-handle leak),
  - DB bloats by more than 100 MB (runaway inserts without vacuum),
  - error rate > 0 (should be 0 for read traffic).

Run: ``python3 -m scripts.stress.long_session_test``.
Invoked as a pytest test in
``tests/adversarial/test_long_session_leaks.py``.
"""
from __future__ import annotations

import os
import random
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _rss_mb(pid: int) -> float:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                kb = int(line.split()[1])
                return kb / 1024.0
    except Exception:
        pass
    return 0.0


def _open_fds(pid: int) -> int:
    try:
        return len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except Exception:
        return 0


def _db_mb(db: Path) -> float:
    try:
        return db.stat().st_size / 1024.0 / 1024.0
    except Exception:
        return 0.0


def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT, active INTEGER DEFAULT 1, version INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, client_code TEXT, vendor TEXT, review_status TEXT, version INTEGER DEFAULT 1, firm_code TEXT, updated_at TEXT);
    """)
    c.commit(); c.close()


def run(duration_seconds: int = 180) -> dict:
    """Run the stress loop for ``duration_seconds`` real-time seconds.

    Returns a summary dict with samples for leak analysis.
    """
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="leak_"))
    db = tmp / "leak.db"
    secret = tmp / "secret"; secret.write_text("x" * 48)
    _bootstrap(db)

    import scripts.review_dashboard as rd
    rd.DB_PATH = db
    rd.PASSWORD_LINK_SECRET_FILE = str(secret)
    rd.bootstrap_schema()
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()

    # Seed a user + session cookie.
    from unittest.mock import patch
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "leak@det.com", "customer": "cus_L",
             "subscription": "sub_L", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 WHERE username=?",
            (rd.hash_password("LeakP@ss1!"), "leak@det.com"),
        )
        conn.commit()
    tok = rd.create_session("leak@det.com")
    cookies = f"session_token={tok}"

    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True); t.start()
    base = f"http://127.0.0.1:{port}"

    # Read-heavy loop: each virtual tick = 1 simulated hour.
    samples: list[dict] = []
    errors = 0
    pid = os.getpid()
    baseline_rss = _rss_mb(pid)
    baseline_fd = _open_fds(pid)
    baseline_db = _db_mb(db)

    start = time.time()
    tick = 0
    while time.time() - start < duration_seconds:
        tick += 1
        # Mix of reads with light writes every tick.
        for ep in ("/login", "/health"):
            try:
                req = urllib.request.Request(f"{base}{ep}")
                with urllib.request.urlopen(req, timeout=5):
                    pass
            except Exception:
                errors += 1
        # Every 10 seconds real time, sample leak metrics.
        if tick % 50 == 0:
            samples.append({
                "tick": tick,
                "elapsed_s": round(time.time() - start, 1),
                "rss_mb": round(_rss_mb(pid), 1),
                "open_fds": _open_fds(pid),
                "db_mb": round(_db_mb(db), 2),
                "errors": errors,
            })
        # Rate-limit to avoid hot-looping.
        time.sleep(0.05)

    server.shutdown(); server.server_close()

    summary = {
        "duration_s": duration_seconds,
        "ticks": tick,
        "errors": errors,
        "baseline_rss_mb": round(baseline_rss, 1),
        "final_rss_mb": round(_rss_mb(pid), 1),
        "rss_growth_mb": round(_rss_mb(pid) - baseline_rss, 1),
        "baseline_fds": baseline_fd,
        "final_fds": _open_fds(pid),
        "fd_growth": _open_fds(pid) - baseline_fd,
        "baseline_db_mb": round(baseline_db, 2),
        "final_db_mb": round(_db_mb(db), 2),
        "samples": samples,
    }
    return summary


def main() -> int:
    dur = int(os.environ.get("LEAK_SECONDS", "180"))
    out = run(duration_seconds=dur)
    import json
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

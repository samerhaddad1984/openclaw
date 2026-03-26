#!/usr/bin/env python3
"""
setup_cloudflare.py — LedgerLink AI: Cloudflare Tunnel setup wizard
====================================================================
Interactive script that:

  1. Downloads cloudflared.exe from the official Cloudflare release URL.
  2. Guides the user through: cloudflared login → tunnel create → config.
  3. Configures the tunnel to expose port 8788 (client portal) as HTTPS.
  4. Saves tunnel credentials and config to the cloudflare/ folder.
  5. Registers cloudflared as a Windows Service (auto-start).
  6. Writes the public HTTPS URL to ledgerlink.config.json as
     "public_portal_url".

Usage:
    python scripts/setup_cloudflare.py [--install-dir PATH]
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT        = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "ledgerlink.config.json"

# Official Cloudflare release URL for Windows amd64
CLOUDFLARED_DOWNLOAD_URL = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/"
    "cloudflared-windows-amd64.exe"
)

TUNNEL_NAME       = "ledgerlink"
PORTAL_PORT       = 8788
SERVICE_NAME      = "cloudflared"
LOG_FILENAME      = "cloudflared.log"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _print_step(n: int, text: str) -> None:
    print(f"\n[Step {n}] {text}")
    print("─" * 60)


def _run(cmd: list[str], *, check: bool = True, capture: bool = False,
         cwd: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        cwd=cwd,
    )


def _run_cf(cloudflared: Path, args: list[str], **kwargs) -> subprocess.CompletedProcess:
    return _run([str(cloudflared)] + args, **kwargs)


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        json.dumps(cfg, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — Download cloudflared.exe
# ──────────────────────────────────────────────────────────────────────────────

def step_download(install_dir: Path) -> Path:
    _print_step(1, "Download cloudflared.exe")
    dest = install_dir / "cloudflared.exe"

    if dest.exists():
        print(f"  cloudflared.exe already present at {dest}")
        print("  Skipping download.")
        return dest

    print(f"  Downloading from:\n  {CLOUDFLARED_DOWNLOAD_URL}")
    print("  This may take a moment…")

    def _progress(block_num: int, block_size: int, total_size: int) -> None:
        if total_size > 0:
            pct = min(100, block_num * block_size * 100 // total_size)
            print(f"\r  {pct}%", end="", flush=True)

    urllib.request.urlretrieve(CLOUDFLARED_DOWNLOAD_URL, str(dest), _progress)
    print(f"\n  Downloaded → {dest}")
    return dest


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — cloudflared login (opens browser)
# ──────────────────────────────────────────────────────────────────────────────

def step_login(cloudflared: Path) -> None:
    _print_step(2, "Authenticate with Cloudflare (opens your browser)")
    print("  A browser window will open.  Sign in to your Cloudflare account")
    print("  and authorise the certificate.  Return here when done.")
    input("  Press ENTER to open the browser… ")
    _run_cf(cloudflared, ["login"])
    print("  ✔  Login complete.")


# ──────────────────────────────────────────────────────────────────────────────
# Step 3 — Create tunnel
# ──────────────────────────────────────────────────────────────────────────────

def step_create_tunnel(cloudflared: Path, cf_dir: Path) -> str:
    """Create (or reuse) the 'ledgerlink' tunnel.  Returns the tunnel UUID."""
    _print_step(3, f"Create tunnel '{TUNNEL_NAME}'")

    # Check if a tunnel with this name already exists
    result = _run_cf(cloudflared, ["tunnel", "list", "--output", "json"],
                     capture=True, check=False)
    tunnel_id = ""
    if result.returncode == 0:
        try:
            tunnels = json.loads(result.stdout)
            for t in tunnels:
                if t.get("name") == TUNNEL_NAME:
                    tunnel_id = t["id"]
                    print(f"  Tunnel '{TUNNEL_NAME}' already exists: {tunnel_id}")
                    break
        except Exception:
            pass

    if not tunnel_id:
        result = _run_cf(
            cloudflared,
            ["tunnel", "--origincert", str(cf_dir / "cert.pem"), "create", TUNNEL_NAME],
            capture=True,
        )
        print(result.stdout)
        # Extract UUID from output like "Created tunnel ledgerlink with id <uuid>"
        for line in result.stdout.splitlines():
            if "with id" in line:
                tunnel_id = line.split("with id")[-1].strip().split()[0]
                break

    if not tunnel_id:
        print("  WARNING: Could not extract tunnel ID automatically.")
        tunnel_id = input("  Paste the tunnel ID shown above: ").strip()

    print(f"  ✔  Tunnel ID: {tunnel_id}")
    return tunnel_id


# ──────────────────────────────────────────────────────────────────────────────
# Step 4 — Write tunnel config file
# ──────────────────────────────────────────────────────────────────────────────

def step_write_config(cf_dir: Path, tunnel_id: str, install_dir: Path) -> Path:
    _print_step(4, "Write tunnel configuration")

    # Credentials file is written by cloudflared to ~/.cloudflared/<uuid>.json
    # Copy it into our cloudflare/ folder for portability
    home_cf = Path.home() / ".cloudflared"
    cred_src = home_cf / f"{tunnel_id}.json"
    cred_dst = cf_dir / f"{tunnel_id}.json"
    if cred_src.exists() and not cred_dst.exists():
        shutil.copy2(str(cred_src), str(cred_dst))
        print(f"  Copied credentials → {cred_dst}")

    log_path = cf_dir / LOG_FILENAME

    config_content = f"""\
# cloudflared tunnel config — generated by LedgerLink setup_cloudflare.py
tunnel: {tunnel_id}
credentials-file: {cred_dst}

ingress:
  - hostname: ""   # filled by Cloudflare DNS routing
    service: http://localhost:{PORTAL_PORT}
  - service: http_status:404

loglevel: info
logfile: {log_path}
"""
    config_path = cf_dir / "config.yml"
    config_path.write_text(config_content, encoding="utf-8")
    print(f"  Config written → {config_path}")
    return config_path


# ──────────────────────────────────────────────────────────────────────────────
# Step 5 — Create DNS route (public hostname)
# ──────────────────────────────────────────────────────────────────────────────

def step_dns_route(cloudflared: Path, tunnel_id: str) -> str:
    _print_step(5, "Set up public DNS hostname")
    print("  You need a domain or subdomain in your Cloudflare account.")
    print("  Example: portal.yourfirm.com")
    hostname = input("  Enter the public hostname for the portal: ").strip()

    if hostname:
        result = _run_cf(
            cloudflared,
            ["tunnel", "route", "dns", tunnel_id, hostname],
            capture=True, check=False,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"  WARNING: DNS route creation returned error:\n  {result.stderr}")
            print("  You can set this up manually in the Cloudflare dashboard.")

    public_url = f"https://{hostname}" if hostname else ""
    return public_url


# ──────────────────────────────────────────────────────────────────────────────
# Step 6 — Register Windows Service
# ──────────────────────────────────────────────────────────────────────────────

def step_windows_service(cloudflared: Path, config_path: Path) -> bool:
    _print_step(6, "Register cloudflared as a Windows Service (auto-start)")

    if platform.system() != "Windows":
        print("  Skipped — not running on Windows.")
        return False

    # Stop and delete existing service if present
    _run(["sc", "stop", SERVICE_NAME], check=False, capture=True)
    time.sleep(1)
    _run(["sc", "delete", SERVICE_NAME], check=False, capture=True)
    time.sleep(1)

    result = _run_cf(
        cloudflared,
        ["service", "install", "--config", str(config_path)],
        capture=True, check=False,
    )
    print(result.stdout)

    if result.returncode != 0:
        print(f"  WARNING: Service install failed:\n  {result.stderr}")
        print("  You can start cloudflared manually:")
        print(f"    {cloudflared} tunnel --config {config_path} run")
        return False

    # Set auto-start
    _run(["sc", "config", SERVICE_NAME, "start=", "auto"], check=False, capture=True)
    # Start now
    _run(["sc", "start", SERVICE_NAME], check=False, capture=True)
    time.sleep(2)

    # Verify
    status = _run(["sc", "query", SERVICE_NAME], capture=True, check=False)
    if "RUNNING" in status.stdout:
        print("  ✔  cloudflared service is running.")
        return True
    else:
        print("  WARNING: Service did not start. Check the Event Viewer for details.")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Step 7 — Save public URL to ledgerlink.config.json
# ──────────────────────────────────────────────────────────────────────────────

def step_save_url(public_url: str) -> None:
    _print_step(7, "Save public URL to ledgerlink.config.json")
    cfg = _load_config()
    cfg["public_portal_url"] = public_url
    _save_config(cfg)
    print(f"  public_portal_url = {public_url!r}")
    print(f"  Saved → {CONFIG_PATH}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="LedgerLink AI — Cloudflare Tunnel setup wizard",
    )
    parser.add_argument(
        "--install-dir",
        default=str(ROOT),
        help="Installation directory (default: repo root)",
    )
    parser.add_argument(
        "--skip-login",
        action="store_true",
        help="Skip cloudflared login (use if already authenticated)",
    )
    args = parser.parse_args()

    install_dir = Path(args.install_dir).resolve()
    cf_dir      = install_dir / "cloudflare"
    cf_dir.mkdir(parents=True, exist_ok=True)

    print()
    print("═" * 60)
    print("  LedgerLink AI — Cloudflare Tunnel Setup")
    print("═" * 60)
    print(f"  Install dir : {install_dir}")
    print(f"  Config dir  : {cf_dir}")
    print(f"  Portal port : {PORTAL_PORT}")
    print()

    try:
        # 1. Download
        cloudflared = step_download(cf_dir)

        # 2. Login
        if not args.skip_login:
            step_login(cloudflared)
        else:
            print("\n[Step 2] Login — SKIPPED (--skip-login)")

        # 3. Create tunnel
        tunnel_id = step_create_tunnel(cloudflared, cf_dir)

        # 4. Write config
        config_path = step_write_config(cf_dir, tunnel_id, install_dir)

        # 5. DNS route
        public_url = step_dns_route(cloudflared, tunnel_id)

        # 6. Windows service
        step_windows_service(cloudflared, config_path)

        # 7. Save URL
        step_save_url(public_url)

        print()
        print("═" * 60)
        print("  ✔  Cloudflare Tunnel setup complete!")
        if public_url:
            print(f"  Public URL : {public_url}")
        print()
        print("  The LedgerLink client portal is now accessible at:")
        print(f"  {public_url or '(configure DNS to see the URL)'}")
        print()
        print("  To check tunnel status:")
        print(f"    sc query {SERVICE_NAME}   (Windows)")
        print("  Or visit /troubleshoot in the review dashboard.")
        print("═" * 60)
        print()
        return 0

    except KeyboardInterrupt:
        print("\n\nSetup cancelled.")
        return 1
    except Exception as exc:
        print(f"\nERROR: {exc}")
        import traceback as _tb
        _tb.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

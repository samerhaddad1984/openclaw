#!/usr/bin/env python3
"""
scripts/build_installer.py -- Package LedgerLink into a distributable ZIP.

Usage:
    python scripts/build_installer.py

Creates dist/LedgerLink_v{version}_Windows.zip ready for client delivery.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT / "dist"


def _read_version() -> str:
    vf = ROOT / "version.json"
    if vf.exists():
        return json.loads(vf.read_text(encoding="utf-8")).get("version", "1.0.0")
    return "1.0.0"


# Directories / files to EXCLUDE from the ZIP
EXCLUDE_DIRS = {
    ".git", "node_modules", "__pycache__", "tests", "dist",
    ".venv", "venv", ".mypy_cache", ".pytest_cache", ".tox",
}
EXCLUDE_FILES = {".env", "ledgerlink_agent.db"}

# Directories that must appear in the ZIP (even if empty on disk)
REQUIRED_EMPTY_DIRS = [
    "data/backups",
    "data/client_uploads",
    "data/incoming_documents",
    "data/training",
]


def _should_skip_dir(name: str) -> bool:
    return name in EXCLUDE_DIRS or name.startswith(".")


def _should_skip_file(name: str) -> bool:
    if name in EXCLUDE_FILES:
        return True
    if name.endswith(".pyc") or name.endswith(".pyo"):
        return True
    return False


def _make_template_config() -> str:
    """Return ledgerlink.config.json with API keys blanked out."""
    cfg_path = ROOT / "ledgerlink.config.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    # Blank out secrets
    if "ai_router" in cfg:
        for provider in ("routine_provider", "premium_provider"):
            if provider in cfg["ai_router"]:
                cfg["ai_router"][provider]["api_key"] = ""
    if "digest" in cfg:
        cfg["digest"]["smtp_password"] = "your-app-password"
        cfg["digest"]["smtp_user"] = "yourfirm@gmail.com"
        cfg["digest"]["from_address"] = "ledgerlink@yourfirm.com"
    if "ingest" in cfg:
        cfg["ingest"]["api_key"] = ""
    # Remove license if present
    cfg.pop("license", None)
    return json.dumps(cfg, indent=2, ensure_ascii=False)


def build() -> Path:
    version = _read_version()
    zip_name = f"LedgerLink_v{version}_Windows.zip"
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DIST_DIR / zip_name

    # Remove old ZIP if present
    if zip_path.exists():
        zip_path.unlink()

    prefix = "LedgerLink"
    included_top_dirs = {"src", "scripts", "installer"}

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # 1. Source directories: src/, scripts/, installer/
        for top_dir_name in included_top_dirs:
            top_dir = ROOT / top_dir_name
            if not top_dir.exists():
                continue
            for dirpath_str, dirnames, filenames in os.walk(top_dir):
                dirpath = Path(dirpath_str)
                # Prune excluded sub-directories in-place
                dirnames[:] = [
                    d for d in dirnames if not _should_skip_dir(d)
                ]
                for fname in filenames:
                    if _should_skip_file(fname):
                        continue
                    full = dirpath / fname
                    arcname = f"{prefix}/{full.relative_to(ROOT).as_posix()}"
                    zf.write(full, arcname)

        # 2. Top-level files
        for fname in ("requirements.txt", "version.json"):
            fpath = ROOT / fname
            if fpath.exists():
                zf.write(fpath, f"{prefix}/{fname}")

        # 3. Template config (secrets blanked)
        zf.writestr(
            f"{prefix}/ledgerlink.config.json",
            _make_template_config(),
        )

        # 4. README_INSTALL.txt
        readme = ROOT / "docs" / "README_INSTALL.txt"
        if readme.exists():
            zf.write(readme, f"{prefix}/docs/README_INSTALL.txt")

        # 5. Empty data/ directory structure (placeholder files)
        for empty_dir in REQUIRED_EMPTY_DIRS:
            zf.writestr(f"{prefix}/{empty_dir}/.gitkeep", "")

    return zip_path


def main() -> None:
    print("Building LedgerLink installer ZIP ...")
    zip_path = build()
    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"\nZIP created: {zip_path}")
    print(f"Size: {size_mb:.1f} MB")

    # List contents
    print(f"\nContents:")
    with zipfile.ZipFile(zip_path, "r") as zf:
        entries = sorted(zf.namelist())
        dirs_shown = set()
        for entry in entries:
            # Show directories at 2 levels deep for readability
            parts = entry.split("/")
            if len(parts) >= 3:
                dir_key = "/".join(parts[:3])
            else:
                dir_key = entry
            if dir_key not in dirs_shown:
                dirs_shown.add(dir_key)
                if entry.endswith("/") or entry.endswith(".gitkeep"):
                    print(f"  {'/'.join(parts[:3])}/")
                else:
                    print(f"  {entry}")


if __name__ == "__main__":
    main()

"""Playwright fixtures. Sandbox-specific: Chromium requires
``LD_LIBRARY_PATH`` to pick up sideloaded libs at /tmp/libs/extracted.
If those libs are absent, all browser tests skip cleanly.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

_SIDELOAD_LIBS = Path("/tmp/libs/extracted/usr/lib/x86_64-linux-gnu")
_CHROMIUM_OK = False


def _ensure_ld_library_path() -> bool:
    """Return True if Chromium can run. Mutates LD_LIBRARY_PATH in-place
    so subprocesses inherit it."""
    global _CHROMIUM_OK
    if _CHROMIUM_OK:
        return True
    # Check the sideload dir exists AND contains libatk.
    if not (_SIDELOAD_LIBS / "libatk-1.0.so.0").exists():
        return False
    current = os.environ.get("LD_LIBRARY_PATH", "")
    if str(_SIDELOAD_LIBS) not in current:
        os.environ["LD_LIBRARY_PATH"] = (
            f"{_SIDELOAD_LIBS}:{current}" if current else str(_SIDELOAD_LIBS)
        )
    _CHROMIUM_OK = True
    return True


@pytest.fixture(scope="session")
def chromium_browser():
    """Launch a headless Chromium once per session."""
    if not _ensure_ld_library_path():
        pytest.skip("Chromium libs not available; skipping browser tests")
    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    )
    yield browser
    browser.close()
    pw.stop()


@pytest.fixture
def browser_context(chromium_browser):
    """Fresh context per test (isolated cookies/storage)."""
    ctx = chromium_browser.new_context(
        viewport={"width": 375, "height": 812},  # iPhone-ish default
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                   "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
                   "Mobile/15E148 Safari/604.1",
    )
    yield ctx
    ctx.close()

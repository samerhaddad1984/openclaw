"""Chaos framework configuration — API keys, budgets, paths, difficulty mix."""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CHAOS_ROOT = Path(__file__).resolve().parent
REPO_ROOT = CHAOS_ROOT.parent

CHAOS_RESULTS_DIR = CHAOS_ROOT / "results"
CHAOS_RUNS_DIR = CHAOS_RESULTS_DIR / "runs"
CHAOS_FAILURES_DIR = CHAOS_RESULTS_DIR / "failures"
CHAOS_REPORTS_DIR = CHAOS_RESULTS_DIR / "reports"
CHAOS_CACHE_DIR = CHAOS_RESULTS_DIR / "cache"

CHAOS_FIXTURES_DIR = CHAOS_ROOT / "fixtures"
CHAOS_PROMPTS_DIR = CHAOS_FIXTURES_DIR / "prompts"

for _d in (
    CHAOS_RESULTS_DIR,
    CHAOS_RUNS_DIR,
    CHAOS_FAILURES_DIR,
    CHAOS_REPORTS_DIR,
    CHAOS_CACHE_DIR,
):
    _d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Google / Imagen
# ---------------------------------------------------------------------------
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY", "")
IMAGEN_MODEL = os.environ.get("CHAOS_IMAGEN_MODEL", "imagen-3.0-generate-002")

# Approximate cost per generated image (USD). Imagen 3 is ~$0.03/image as of 2026.
IMAGEN_COST_PER_IMAGE_USD = 0.03


# ---------------------------------------------------------------------------
# Budget controls
# ---------------------------------------------------------------------------
DEFAULT_BUDGET_USD = 30.0
HARD_CAP_BUDGET_USD = 200.0


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------
DEFAULT_WORKERS = 4
MAX_WORKERS = 16


# ---------------------------------------------------------------------------
# Difficulty distribution when running "all" / "mixed" mode
# ---------------------------------------------------------------------------
DIFFICULTY_MIX = {
    "easy":       0.10,
    "normal":     0.30,
    "hard":       0.35,
    "nightmare":  0.20,
    "impossible": 0.05,
}

DIFFICULTIES = ("easy", "normal", "hard", "nightmare", "impossible")


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------
PRESETS = {
    "smoke":   {"count": 50,   "budget": 2.0,   "workers": 2},
    "full":    {"count": 1000, "budget": 30.0,  "workers": 4},
    "weekend": {"count": 5000, "budget": 150.0, "workers": 8},
}


# ---------------------------------------------------------------------------
# Tracks
# ---------------------------------------------------------------------------
TRACKS = (
    "receipts",
    "audit",
    "financial",
    "recon",
    "workflow",
    "fraud",
    "tax",
)


# ---------------------------------------------------------------------------
# Test database — isolated from production
# ---------------------------------------------------------------------------
CHAOS_TEST_DB_PATH = CHAOS_RESULTS_DIR / "chaos_test.db"


def budget_for_preset(name: str) -> float:
    return float(PRESETS.get(name, {}).get("budget", DEFAULT_BUDGET_USD))

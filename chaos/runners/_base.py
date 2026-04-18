"""Shared runner scaffolding — sys.path, isolated DB, timing."""
from __future__ import annotations

import logging
import sqlite3
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent  # /opt/otocpa
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

log = logging.getLogger(__name__)


@dataclass
class RunnerResult:
    scenario_id: str
    category: str
    subtype: str
    difficulty: str
    passed: bool = False
    score: float = 0.0
    elapsed_ms: float = 0.0
    output: dict[str, Any] = field(default_factory=dict)
    oracle_result: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    traceback: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id":   self.scenario_id,
            "category":      self.category,
            "subtype":       self.subtype,
            "difficulty":    self.difficulty,
            "passed":        self.passed,
            "score":         round(self.score, 4),
            "elapsed_ms":    round(self.elapsed_ms, 2),
            "output":        self.output,
            "oracle_result": self.oracle_result,
            "error":         self.error,
            "traceback":     self.traceback,
        }


@contextmanager
def timed():
    t0 = time.perf_counter()
    holder: dict[str, float] = {}
    try:
        yield holder
    finally:
        holder["elapsed_ms"] = (time.perf_counter() - t0) * 1000.0


def build_result(scenario: dict[str, Any]) -> RunnerResult:
    return RunnerResult(
        scenario_id=scenario.get("id", "?"),
        category=scenario.get("category", "?"),
        subtype=scenario.get("subtype", "?"),
        difficulty=scenario.get("difficulty", "?"),
    )


def safe_exec(scenario: dict[str, Any], fn) -> RunnerResult:
    """Wrap a runner function so exceptions become structured errors."""
    result = build_result(scenario)
    try:
        with timed() as t:
            fn(scenario, result)
        result.elapsed_ms = t.get("elapsed_ms", 0.0)
    except Exception as e:
        result.error = f"{type(e).__name__}: {e}"
        result.traceback = traceback.format_exc()
        result.passed = False
    return result


def open_isolated_db(path: Path) -> sqlite3.Connection:
    """Create a fresh empty sqlite DB for a single scenario."""
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

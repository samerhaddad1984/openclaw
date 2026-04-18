"""Master scenario catalog.

Aggregates every track's scenarios into one reproducible list. Each scenario
is a dict with at minimum:

    id            stable hash for reproducibility
    category      track name (receipts, audit, financial, recon, workflow, fraud, tax)
    subtype       short label for the kind of scenario
    difficulty    easy | normal | hard | nightmare | impossible
    input_spec    how the runner should construct input
    ground_truth  what the oracle should see when the system works correctly
    oracle        which oracle class validates this
    severity_on_failure  critical | high | medium | low
    affects_engines      list of engine modules exercised
    description   one-line human summary
"""
from __future__ import annotations

import hashlib
import json
import random
from typing import Any, Iterable

from . import (
    audit_scenarios,
    financial_scenarios,
    fraud_scenarios,
    receipt_scenarios,
    recon_scenarios,
    tax_scenarios,
    workflow_scenarios,
)


TRACK_GENERATORS = {
    "receipts":  receipt_scenarios.generate,
    "audit":     audit_scenarios.generate,
    "financial": financial_scenarios.generate,
    "recon":     recon_scenarios.generate,
    "workflow":  workflow_scenarios.generate,
    "fraud":     fraud_scenarios.generate,
    "tax":       tax_scenarios.generate,
}


def _stable_id(scenario: dict[str, Any]) -> str:
    """Reproducible id from category + subtype + index + inputs."""
    payload = {
        "category": scenario.get("category"),
        "subtype":  scenario.get("subtype"),
        "index":    scenario.get("_index"),
        "seed":     scenario.get("_seed"),
    }
    h = hashlib.sha1(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]
    return f"{scenario.get('category','?')}_{scenario.get('subtype','?')}_{h}"


def build_all_scenarios(
    *,
    tracks: Iterable[str] | None = None,
    difficulty: str | None = None,
    seed: int = 1337,
) -> list[dict[str, Any]]:
    """Expand every registered generator into the full scenario list.

    If *difficulty* is given, only scenarios matching that difficulty are kept.
    """
    rnd = random.Random(seed)
    selected = list(tracks) if tracks else list(TRACK_GENERATORS.keys())
    out: list[dict[str, Any]] = []
    for track in selected:
        gen = TRACK_GENERATORS.get(track)
        if not gen:
            continue
        for i, sc in enumerate(gen(rnd)):
            sc["_index"] = i
            sc["_seed"] = seed
            sc["category"] = sc.get("category") or track
            sc["id"] = sc.get("id") or _stable_id(sc)
            if difficulty and sc.get("difficulty") != difficulty:
                continue
            out.append(sc)
    return out


def sample_scenarios(
    *,
    tracks: Iterable[str] | None = None,
    count: int = 50,
    difficulty: str | None = None,
    difficulty_mix: dict[str, float] | None = None,
    seed: int = 1337,
) -> list[dict[str, Any]]:
    """Sample *count* scenarios across tracks, optionally weighted by difficulty."""
    rnd = random.Random(seed)
    pool = build_all_scenarios(tracks=tracks, difficulty=difficulty, seed=seed)
    if not pool:
        return []

    if difficulty_mix and not difficulty:
        buckets: dict[str, list[dict[str, Any]]] = {}
        for sc in pool:
            buckets.setdefault(sc.get("difficulty", "normal"), []).append(sc)
        out: list[dict[str, Any]] = []
        for level, weight in difficulty_mix.items():
            bucket = buckets.get(level, [])
            if not bucket:
                continue
            n = max(1, int(round(count * weight)))
            out.extend(rnd.choices(bucket, k=min(n, len(bucket) * 4)))
        rnd.shuffle(out)
        return out[:count]

    if count >= len(pool):
        rnd.shuffle(pool)
        return pool
    return rnd.sample(pool, count)


def count_by_track(seed: int = 1337) -> dict[str, int]:
    """Report scenarios per track for documentation."""
    pool = build_all_scenarios(seed=seed)
    counts: dict[str, int] = {}
    for sc in pool:
        counts[sc["category"]] = counts.get(sc["category"], 0) + 1
    return counts

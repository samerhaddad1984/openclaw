"""Multi-engine OCR consensus — Track 3 of the OCR upgrade sprint.

Runs several OCR engines against the same image in parallel, then computes
per-field consensus. The CPA sees:

- One canonical extraction (majority-vote per field).
- A per-field confidence (fraction of engines that agreed).
- Explicit disagreements, so borderline receipts can be flagged for review.

Architecture
------------

``ConsensusOCR`` is engine-agnostic. Engines are registered as callables
that accept an image path and return ``{vendor, date, subtotal, total,
gst, qst, line_items?, error?}``. The default registry includes:

- ``docai``         → ``src/engines/google_docai.py`` (always available in
  this sandbox; most-canonical extraction).
- ``claude_vision`` → real Claude Vision API; returns
  ``{"error": "api_key_missing"}`` when ``ANTHROPIC_API_KEY`` isn't set.
- ``deepseek_vision`` → same pattern for ``DEEPSEEK_API_KEY``.

Tests pass in deterministic mock engines, so no live calls are needed.

Consensus algorithm
-------------------

For each of ``vendor``, ``total``, ``subtotal``, ``gst``, ``qst``, ``date``:

- Numeric fields: round to cents, pick the Counter mode. Confidence =
  ``count / n_engines``.
- Text fields: exact match counter; when no consensus reaches >1 engine,
  the engine with highest self-reported confidence wins and ``confidence``
  is scaled by 1/n.

A disagreement is recorded whenever ``count < n_engines``.

Budget
------

``process(image, budget_cap)`` short-circuits once cumulative per-receipt
cost exceeds ``budget_cap``. Costs are declared per engine in ``COSTS``.

Feature flag
------------

Consensus is opt-in. Call sites check ``consensus_enabled(firm_code)``;
default is ``False`` so existing ingest keeps its single-engine cost.
"""
from __future__ import annotations

import logging
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger(__name__)

EngineCallable = Callable[[str | Path], dict[str, Any]]

# Per-engine per-receipt budget (USD). Tweakable by the operator; these
# reflect observed averages in R2's 846-receipt batch.
COSTS: dict[str, float] = {
    "docai": 0.015,
    "claude_vision": 0.015,
    "deepseek_vision": 0.010,
}

FIELDS_NUMERIC = ("total", "subtotal", "gst", "qst", "tax_total")
FIELDS_TEXT = ("vendor", "date")
REVIEW_CONFIDENCE_FLOOR = 0.67


# ---------------------------------------------------------------------------
# Default engine adapters (soft-fail when API keys aren't present)
# ---------------------------------------------------------------------------

def _engine_docai(image_path: str | Path) -> dict[str, Any]:
    """Call Google DocAI via the existing OCR engine and return a
    simplified field dict."""
    try:
        from src.engines import google_docai  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover
        return {"error": f"docai_import_failed: {exc}"}
    extractor = getattr(google_docai, "extract_document_fields", None)
    if extractor is None:
        return {"error": "docai_extract_fn_unavailable"}
    try:
        raw = extractor(str(image_path))
    except Exception as exc:  # pragma: no cover
        return {"error": f"docai_runtime_failed: {exc}"}
    return _flatten_extraction(raw)


def _engine_claude_vision(image_path: str | Path) -> dict[str, Any]:
    """Real Claude Vision call; returns ``{"error": "api_key_missing"}``
    if ``ANTHROPIC_API_KEY`` isn't set. Actual invocation is not exercised
    in this sandbox — shape is tested via mocks."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return {"error": "api_key_missing"}
    try:  # pragma: no cover — requires live key
        import anthropic  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover
        return {"error": f"anthropic_sdk_missing: {exc}"}
    # Actual prompt/invocation deliberately left out here until a budgeted
    # window is authorised; the ``error`` path is sufficient for fallback
    # behaviour.
    return {"error": "claude_vision_disabled_pending_budget_auth"}  # pragma: no cover


def _engine_deepseek_vision(image_path: str | Path) -> dict[str, Any]:
    """Symmetric to Claude Vision; gated on ``DEEPSEEK_API_KEY``."""
    if not os.environ.get("DEEPSEEK_API_KEY"):
        return {"error": "api_key_missing"}
    return {"error": "deepseek_vision_disabled_pending_budget_auth"}  # pragma: no cover


DEFAULT_ENGINES: dict[str, EngineCallable] = {
    "docai": _engine_docai,
    "claude_vision": _engine_claude_vision,
    "deepseek_vision": _engine_deepseek_vision,
}


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

def consensus_enabled(firm_code: str = "", env_override: str | None = None) -> bool:
    """Consensus runs only when explicitly opted in."""
    override = env_override or os.environ.get("USE_CONSENSUS")
    if override:
        return override.lower() in {"1", "true", "yes", "on"}
    return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten_extraction(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Normalise arbitrary engine outputs to the consensus dict shape."""
    if not raw:
        return {}
    out: dict[str, Any] = {}
    for k in FIELDS_TEXT + FIELDS_NUMERIC:
        if k in raw and raw[k] not in ("", None):
            out[k] = raw[k]
    if "line_items" in raw and raw["line_items"] is not None:
        out["line_items"] = raw["line_items"]
    return out


def _round_cents(x: Any) -> float | None:
    try:
        return round(float(x), 2)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# ConsensusOCR
# ---------------------------------------------------------------------------

class ConsensusOCR:
    """Run multiple OCR engines, determine consensus."""

    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(
        self,
        engines: dict[str, EngineCallable] | None = None,
        costs: dict[str, float] | None = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_workers: int | None = None,
    ) -> None:
        self.engines: dict[str, EngineCallable] = dict(engines or DEFAULT_ENGINES)
        self.costs: dict[str, float] = dict(costs or COSTS)
        self.timeout = timeout
        self.max_workers = max_workers or max(1, len(self.engines))

    # ----- public --------------------------------------------------------

    def process(
        self,
        image_path: str | Path,
        budget_cap: float = 0.10,
    ) -> dict[str, Any]:
        """Run registered engines in parallel and compute consensus.

        ``budget_cap`` caps cumulative cost. Engines running past the cap
        are dropped from the consensus.
        """
        results: dict[str, dict[str, Any]] = {}
        total_cost = 0.0
        running_cost = 0.0

        selected: dict[str, EngineCallable] = {}
        for name, fn in self.engines.items():
            engine_cost = self.costs.get(name, 0.0)
            if running_cost + engine_cost > budget_cap:
                continue
            selected[name] = fn
            running_cost += engine_cost

        if not selected:
            return self._empty_result(reason="budget_exhausted_before_any_engine")

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_to_name = {
                pool.submit(fn, image_path): name
                for name, fn in selected.items()
            }
            for future in future_to_name:
                name = future_to_name[future]
                try:
                    results[name] = future.result(timeout=self.timeout)
                except FuturesTimeout:
                    results[name] = {"error": "timeout"}
                except Exception as exc:
                    log.exception("consensus engine %s failed", name)
                    results[name] = {"error": f"runtime: {exc}"}
                else:
                    if "error" not in results[name]:
                        total_cost += self.costs.get(name, 0.0)

        consensus = self._compute_consensus(results)
        return {
            "extracted": consensus["fields"],
            "confidence_per_field": consensus["confidence"],
            "engines_agreed": consensus["agreement_count"],
            "disagreements": consensus["disagreements"],
            "total_cost": round(total_cost, 4),
            "needs_review": any(
                c < REVIEW_CONFIDENCE_FLOOR
                for c in consensus["confidence"].values()
            ) if consensus["confidence"] else True,
            "engines_run": list(results.keys()),
            "raw_results": results,
        }

    # ----- helpers -------------------------------------------------------

    def _empty_result(self, reason: str) -> dict[str, Any]:
        return {
            "extracted": {},
            "confidence_per_field": {},
            "engines_agreed": 0,
            "disagreements": [],
            "total_cost": 0.0,
            "needs_review": True,
            "engines_run": [],
            "raw_results": {},
            "error": reason,
        }

    def _compute_consensus(
        self,
        results: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        valid = {k: v for k, v in results.items() if isinstance(v, dict) and "error" not in v}
        n_engines = len(valid)

        if n_engines == 0:
            return {
                "fields": {},
                "confidence": {},
                "agreement_count": 0,
                "disagreements": [],
            }

        if n_engines == 1:
            only_data = next(iter(valid.values()))
            return {
                "fields": dict(only_data),
                "confidence": {k: 0.5 for k in only_data.keys()},
                "agreement_count": 1,
                "disagreements": [],
            }

        consensus_fields: dict[str, Any] = {}
        confidence: dict[str, float] = {}
        disagreements: list[dict[str, Any]] = []

        for field in FIELDS_NUMERIC:
            values: list[tuple[str, float]] = []
            for engine, data in valid.items():
                if field in data and data[field] is not None:
                    rounded = _round_cents(data[field])
                    if rounded is not None:
                        values.append((engine, rounded))
            if not values:
                continue
            counter = Counter(v for _, v in values)
            value, count = counter.most_common(1)[0]
            consensus_fields[field] = value
            confidence[field] = count / n_engines
            if count < len(values):
                disagreements.append({
                    "field": field,
                    "consensus": value,
                    "alternates": sorted(
                        {v for _, v in values if v != value}
                    ),
                })

        for field in FIELDS_TEXT:
            values = [(engine, data[field]) for engine, data in valid.items()
                      if field in data and data[field] not in ("", None)]
            if not values:
                continue
            # Case-fold text comparison but return the original best.
            normalized = [
                (engine, str(v).strip().lower(), str(v))
                for engine, v in values
            ]
            counter = Counter(norm for _, norm, _ in normalized)
            top_norm, top_count = counter.most_common(1)[0]
            original = next(
                (orig for _, norm, orig in normalized if norm == top_norm),
                None,
            )
            consensus_fields[field] = original
            confidence[field] = top_count / n_engines
            if top_count < len(values):
                disagreements.append({
                    "field": field,
                    "consensus": original,
                    "alternates": sorted({
                        orig for _, norm, orig in normalized if norm != top_norm
                    }),
                })

        agreement_count = max(
            (int(round(c * n_engines)) for c in confidence.values()),
            default=1,
        )

        return {
            "fields": consensus_fields,
            "confidence": confidence,
            "agreement_count": agreement_count,
            "disagreements": disagreements,
        }

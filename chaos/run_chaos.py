#!/usr/bin/env python3
"""
OtoCPA Chaos Testing Framework — CLI entry point.

Usage examples
--------------
    # Cheap smoke test (no AI images, ~1 min, no $)
    python3 chaos/run_chaos.py --preset smoke --no-ai

    # Full stress (~$30, ~2 hours)
    python3 chaos/run_chaos.py --preset full

    # Continuous until budget exhausted
    python3 chaos/run_chaos.py --continuous --budget 50

    # Specific track + difficulty
    python3 chaos/run_chaos.py --track receipts --difficulty nightmare --count 100

    # Reproduce a specific failure
    python3 chaos/run_chaos.py --reproduce receipts_coffee_stained_grocery_30_items_abc123

    # Generate images only (for human inspection)
    python3 chaos/run_chaos.py --generate-only --count 50

    # Use already-cached images, don't call Imagen
    python3 chaos/run_chaos.py --use-cached --track receipts
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Make chaos importable as a top-level package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chaos import config  # noqa: E402
from chaos.generators.ai_image_generator import AIImageGenerator, BudgetExceededError  # noqa: E402
from chaos.generators.scenario_catalog import build_all_scenarios, sample_scenarios, count_by_track  # noqa: E402
from chaos.reports.html_report import write_html_report  # noqa: E402
from chaos.reports.markdown_report import write_markdown_report  # noqa: E402
from chaos.runners import (  # noqa: E402
    AuditRunner,
    FinancialRunner,
    FraudRunner,
    ReceiptRunner,
    ReconRunner,
    TaxRunner,
    WorkflowRunner,
)

log = logging.getLogger("chaos")


RUNNER_FACTORIES = {
    "receipts":  lambda ctx: ReceiptRunner(
        image_generator=ctx["image_generator"],
        chaos_db_path=ctx["chaos_db_path"],
        real_ocr=ctx.get("real_ocr", False),
    ),
    "audit":     lambda ctx: AuditRunner(chaos_db_path=ctx["chaos_db_path"]),
    "fraud":     lambda ctx: FraudRunner(chaos_db_path=ctx["chaos_db_path"]),
    "financial": lambda ctx: FinancialRunner(chaos_db_path=ctx["chaos_db_path"]),
    "recon":     lambda ctx: ReconRunner(chaos_db_path=ctx["chaos_db_path"]),
    "workflow":  lambda ctx: WorkflowRunner(chaos_db_path=ctx["chaos_db_path"]),
    "tax":       lambda ctx: TaxRunner(),
}


class Checkpoint:
    """Persist partial run state so Ctrl-C is not a full loss."""

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.results_file = run_dir / "results.jsonl"
        self.budget_file  = run_dir / "budget.json"
        self.meta_file    = run_dir / "meta.json"

    def append(self, record: dict[str, Any]) -> None:
        with self.results_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def write_budget(self, snapshot: dict[str, Any]) -> None:
        self.budget_file.write_text(json.dumps(snapshot, indent=2))

    def write_meta(self, meta: dict[str, Any]) -> None:
        self.meta_file.write_text(json.dumps(meta, indent=2))

    def load_results(self) -> list[dict[str, Any]]:
        if not self.results_file.exists():
            return []
        out = []
        for line in self.results_file.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except Exception:
                pass
        return out


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="OtoCPA Chaos Testing Framework")
    p.add_argument("--preset", choices=sorted(config.PRESETS.keys()),
                   help="smoke | full | weekend")
    p.add_argument("--count", type=int, default=None,
                   help="Override scenario count")
    p.add_argument("--track", action="append", default=None,
                   help="Limit to one or more tracks (repeatable)")
    p.add_argument("--difficulty", default=None,
                   choices=config.DIFFICULTIES,
                   help="Filter to a specific difficulty")
    p.add_argument("--budget", type=float, default=None,
                   help="Hard USD budget for AI image generation")
    p.add_argument("--workers", type=int, default=config.DEFAULT_WORKERS,
                   help="Parallel workers (1 = serial)")
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--no-ai", action="store_true",
                   help="Skip image generation — use placeholder PNGs")
    p.add_argument("--use-cached", action="store_true",
                   help="Only use cached images; do not call Imagen")
    p.add_argument("--real-ocr", action="store_true",
                   help="Run receipt scenarios through the real OCR pipeline "
                        "(costs money per call). Default is a deterministic mock.")
    p.add_argument("--generate-only", action="store_true",
                   help="Generate images but don't run scenarios")
    p.add_argument("--continuous", action="store_true",
                   help="Loop until budget is exhausted or Ctrl-C")
    p.add_argument("--reproduce", default=None,
                   help="Run a single scenario by id")
    p.add_argument("--confirm-budget", action="store_true",
                   help="Required for budgets > $50")
    p.add_argument("--run-id", default=None,
                   help="Resume an existing run id")
    p.add_argument("--verbose", "-v", action="store_true")
    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Resolve count / budget from preset
    count = args.count
    budget = args.budget
    workers = args.workers
    if args.preset:
        preset = config.PRESETS[args.preset]
        count = count or preset["count"]
        budget = budget if budget is not None else preset["budget"]
        workers = workers or preset["workers"]
    count = count or 50
    if budget is None:
        budget = config.DEFAULT_BUDGET_USD

    if budget > 50.0 and not args.confirm_budget:
        log.error("Budget > $50 requires --confirm-budget; got $%.2f", budget)
        return 2
    if budget > config.HARD_CAP_BUDGET_USD:
        log.error("Budget exceeds hard cap $%.2f", config.HARD_CAP_BUDGET_USD)
        return 2

    # Reproduce path
    if args.reproduce:
        return _run_reproduce(args)

    # Resolve scenario list
    if args.difficulty:
        scenarios = sample_scenarios(
            tracks=args.track,
            count=count,
            difficulty=args.difficulty,
            seed=args.seed,
        )
    else:
        scenarios = sample_scenarios(
            tracks=args.track,
            count=count,
            difficulty_mix=config.DIFFICULTY_MIX,
            seed=args.seed,
        )

    if not scenarios:
        log.error("No scenarios matched filters.")
        return 2

    # Run dir
    run_id = args.run_id or datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    run_dir = config.CHAOS_RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    cp = Checkpoint(run_dir)
    cp.write_meta({
        "run_id":     run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "count":      count,
        "budget_usd": budget,
        "tracks":     args.track,
        "difficulty": args.difficulty,
        "preset":     args.preset,
        "no_ai":      args.no_ai,
        "seed":       args.seed,
    })

    # Image generator
    image_generator = AIImageGenerator(
        api_key=config.GOOGLE_API_KEY,
        cache_dir=config.CHAOS_CACHE_DIR,
        budget_usd=budget,
        cost_per_image=config.IMAGEN_COST_PER_IMAGE_USD,
        model=config.IMAGEN_MODEL,
        no_ai=args.no_ai or args.use_cached,
    )

    ctx = {
        "image_generator": image_generator,
        "chaos_db_path":   config.CHAOS_TEST_DB_PATH,
        "real_ocr":        bool(args.real_ocr),
    }
    runners = {track: factory(ctx) for track, factory in RUNNER_FACTORIES.items()}

    # Graceful shutdown
    stop_flag = {"stop": False}

    def _sigint(signum, frame):  # noqa: ARG001
        log.warning("Ctrl-C — finishing current scenario then stopping…")
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _sigint)

    # Generate-only path
    if args.generate_only:
        log.info("Generate-only: producing %d images into %s", count, config.CHAOS_CACHE_DIR)
        for sc in scenarios[:count]:
            if sc.get("oracle") != "receipt":
                continue
            try:
                path, _gt = image_generator.generate(sc)
                log.info("  %s → %s", sc["id"], path.name)
            except BudgetExceededError:
                log.warning("Budget exhausted at %d images", image_generator.state.generated_count)
                break
        cp.write_budget(image_generator.budget_snapshot())
        return 0

    # Main run
    passed = failed = errored = 0
    started = time.perf_counter()

    def _run_one(scenario: dict[str, Any]) -> dict[str, Any]:
        runner = runners.get(scenario["category"])
        if not runner:
            return {
                "scenario_id": scenario["id"],
                "category": scenario["category"],
                "subtype": scenario.get("subtype"),
                "difficulty": scenario.get("difficulty"),
                "passed": False,
                "score": 0.0,
                "error": f"no_runner_for_track:{scenario['category']}",
            }
        return runner.run(scenario).to_dict()

    loop_scenarios = scenarios if not args.continuous else (scenarios * 1_000_000)
    # Print progress without requiring tqdm
    total = len(scenarios) if not args.continuous else None
    for idx, sc in enumerate(loop_scenarios, 1):
        if stop_flag["stop"]:
            break
        if not args.no_ai and not image_generator.can_afford_one() and sc.get("oracle") == "receipt":
            log.warning("Budget exhausted — stopping")
            break

        record = _run_one(sc)
        cp.append(record)
        if record.get("error"):
            errored += 1
        elif record.get("passed"):
            passed += 1
        else:
            failed += 1

        if idx % 10 == 0 or total and idx == total:
            eta = ""
            if total:
                elapsed = time.perf_counter() - started
                rate = idx / elapsed if elapsed > 0 else 0
                remaining = (total - idx) / rate if rate > 0 else 0
                eta = f"  ETA: {remaining:.0f}s"
            log.info("  %d / %s  (pass=%d fail=%d err=%d)%s",
                     idx, total or "∞", passed, failed, errored, eta)
            cp.write_budget(image_generator.budget_snapshot())

        if args.continuous and idx >= count * 50:
            break

    cp.write_budget(image_generator.budget_snapshot())

    results = cp.load_results()
    elapsed = time.perf_counter() - started

    # Reports
    summary = {
        "run_id":      run_id,
        "total":       len(results),
        "passed":      passed,
        "failed":      failed,
        "errored":     errored,
        "pass_rate":   (passed / max(1, len(results))),
        "duration_s":  round(elapsed, 2),
        "budget":      image_generator.budget_snapshot(),
        "by_track":    _by_track(results),
        "by_difficulty": _by_difficulty(results),
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    html_path = config.CHAOS_REPORTS_DIR / f"{run_id}.html"
    md_path   = config.CHAOS_REPORTS_DIR / f"{run_id}.md"
    write_html_report(results=results, summary=summary, out_path=html_path)
    write_markdown_report(results=results, summary=summary, out_path=md_path)

    # Copy failures to failures dir for easy triage
    failures = [r for r in results if not r.get("passed")]
    if failures:
        (config.CHAOS_FAILURES_DIR / f"{run_id}.json").write_text(
            json.dumps(failures, indent=2)
        )

    # Console summary
    print()
    print("=" * 60)
    print(f"Run:      {run_id}")
    print(f"Total:    {len(results)}")
    print(f"Pass:     {passed}  ({summary['pass_rate']*100:.1f}%)")
    print(f"Fail:     {failed}")
    print(f"Error:    {errored}")
    print(f"Duration: {elapsed:.1f}s")
    print(f"Budget:   ${image_generator.state.used_usd:.2f} / ${budget:.2f}")
    print(f"Report:   {html_path}")
    print(f"Failures: {config.CHAOS_FAILURES_DIR}/{run_id}.json" if failures else "No failures 🎉")
    print("=" * 60)

    return 0 if errored == 0 and failed == 0 else 1


def _run_reproduce(args) -> int:
    scenarios = build_all_scenarios(seed=args.seed)
    sc = next((s for s in scenarios if s["id"] == args.reproduce), None)
    if not sc:
        print(f"No scenario with id {args.reproduce}")
        return 2
    print(f"Reproducing: {sc['id']} — {sc.get('description')}")
    run_id = datetime.now(timezone.utc).strftime("reproduce_%Y%m%d_%H%M%S")
    run_dir = config.CHAOS_RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    image_generator = AIImageGenerator(
        api_key=config.GOOGLE_API_KEY,
        cache_dir=config.CHAOS_CACHE_DIR,
        budget_usd=args.budget or config.DEFAULT_BUDGET_USD,
        cost_per_image=config.IMAGEN_COST_PER_IMAGE_USD,
        model=config.IMAGEN_MODEL,
        no_ai=args.no_ai or args.use_cached,
    )
    ctx = {"image_generator": image_generator,
           "chaos_db_path": config.CHAOS_TEST_DB_PATH,
           "real_ocr": bool(args.real_ocr)}
    runner = RUNNER_FACTORIES[sc["category"]](ctx)
    record = runner.run(sc).to_dict()
    print(json.dumps(record, indent=2))
    return 0 if record.get("passed") else 1


def _by_track(results: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for r in results:
        cat = r.get("category", "?")
        d = out.setdefault(cat, {"total": 0, "passed": 0, "failed": 0, "errored": 0})
        d["total"] += 1
        if r.get("error"):
            d["errored"] += 1
        elif r.get("passed"):
            d["passed"] += 1
        else:
            d["failed"] += 1
    return out


def _by_difficulty(results: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for r in results:
        diff = r.get("difficulty", "?")
        d = out.setdefault(diff, {"total": 0, "passed": 0, "failed": 0, "errored": 0})
        d["total"] += 1
        if r.get("error"):
            d["errored"] += 1
        elif r.get("passed"):
            d["passed"] += 1
        else:
            d["failed"] += 1
    return out


if __name__ == "__main__":
    sys.exit(main())

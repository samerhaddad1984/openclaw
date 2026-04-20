#!/usr/bin/env python3
"""Live production-pipeline stress test. Hard $10 budget cap.

Runs the FULL ingest pipeline (src.engines.ocr_engine.process_file)
against a corpus of real receipts, tracks cumulative DocAI / Claude
Vision spend, and scores extractions against available ground truth.

Corpus:
- 21 Canadian receipts with Claude-Vision truth labels
  (``scripts/analysis/canadian_receipts_analysis_after_fixes.json``).
- Up to N SROIE receipts with ground-truth JSON
  (``chaos/fixtures/real_receipts/sroie``).

Outputs:
- ``/tmp/live_stress_results.json`` — per-receipt records.
- ``/tmp/live_stress_r5.log``       — run log on stdout (tee by caller).

Budget cap is enforced hard: when the running cost crosses
``BUDGET_CAP`` the loop stops, prints a summary, and returns normally.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# ---------------------------------------------------------------------------
# Budget tracking
# ---------------------------------------------------------------------------

BUDGET_CAP_USD = float(os.environ.get("BUDGET_CAP_USD", "10.00"))
DOCAI_COST_PER_PAGE = 0.015
CLAUDE_VISION_COST_PER_RECEIPT = 0.015


class SpendCounter:
    def __init__(self, cap: float) -> None:
        self.cap = cap
        self.docai_pages = 0
        self.claude_calls = 0
        self.spend_usd = 0.0

    def add_docai(self, pages: int = 1) -> None:
        self.docai_pages += pages
        self.spend_usd += pages * DOCAI_COST_PER_PAGE

    def add_claude(self, n: int = 1) -> None:
        self.claude_calls += n
        self.spend_usd += n * CLAUDE_VISION_COST_PER_RECEIPT

    def would_exceed(self) -> bool:
        # Estimate worst-case additional spend for ONE more receipt:
        # DocAI (1 page) + Claude Vision fallback = $0.030.
        return (self.spend_usd + 0.030) > self.cap


# ---------------------------------------------------------------------------
# Corpus assembly
# ---------------------------------------------------------------------------

CANADIAN_TRUTH_PATH = (
    ROOT / "scripts" / "analysis"
    / "canadian_receipts_analysis_after_fixes.json"
)
SROIE_DIR = ROOT / "chaos" / "fixtures" / "real_receipts" / "sroie"
SROIE_IMAGES = SROIE_DIR / "images"
SROIE_TRUTH = SROIE_DIR / "ground_truth"


def assemble_corpus(max_sroie: int = 100, seed: int = 42) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    items: list[dict[str, Any]] = []

    # 1. Canadian receipts with truth.
    if CANADIAN_TRUTH_PATH.exists():
        canadian = json.loads(CANADIAN_TRUTH_PATH.read_text())
        for rec in canadian:
            fp = Path(rec["file_path"])
            if not fp.exists():
                continue
            items.append({
                "source": "canadian",
                "file_name": rec["file_name"],
                "file_path": str(fp),
                "truth": rec["truth"],
            })

    # 2. SROIE receipts with truth.
    if SROIE_IMAGES.exists() and SROIE_TRUTH.exists():
        sroie_imgs = sorted(SROIE_IMAGES.glob("*.jpg"))
        rng.shuffle(sroie_imgs)
        for img in sroie_imgs:
            truth_file = SROIE_TRUTH / f"{img.stem}.json"
            if not truth_file.exists():
                continue
            try:
                truth = json.loads(truth_file.read_text())
            except Exception:
                continue
            items.append({
                "source": "sroie",
                "file_name": img.name,
                "file_path": str(img),
                "truth": {
                    "vendor": truth.get("company"),
                    "date": _parse_sroie_date(truth.get("date")),
                    "total": _parse_sroie_num(truth.get("total")),
                    "line_count": truth.get("line_count"),
                    "raw": truth,
                },
            })
            if sum(1 for it in items if it["source"] == "sroie") >= max_sroie:
                break

    return items


_SROIE_DATE_PATTERNS = [
    re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"),
    re.compile(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})"),
]


def _parse_sroie_date(raw: Any) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    for pat in _SROIE_DATE_PATTERNS:
        m = pat.search(s)
        if not m:
            continue
        g = m.groups()
        if len(g[0]) == 4:
            y, mo, d = int(g[0]), int(g[1]), int(g[2])
        else:
            d, mo, y = int(g[0]), int(g[1]), int(g[2])
        try:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        except Exception:
            return None
    return None


def _parse_sroie_num(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return round(float(str(raw).replace(",", "")), 2)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _vendor_match(a: str | None, b: str | None) -> bool:
    if not a and not b:
        return True
    if not a or not b:
        return False
    al, bl = str(a).strip().lower(), str(b).strip().lower()
    if al == bl:
        return True
    at, bt = set(al.split()), set(bl.split())
    if not at or not bt:
        return False
    return len(at & bt) / len(at | bt) >= 0.5


def _num_close(a, b, tol=0.02):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= tol
    except (TypeError, ValueError):
        return False


def score_receipt(result: dict[str, Any], truth: dict[str, Any]) -> dict[str, bool]:
    """Return per-field boolean scores where the field is applicable."""
    out: dict[str, bool] = {}
    if "vendor" in truth:
        out["vendor"] = _vendor_match(result.get("vendor"), truth.get("vendor"))
    if "date" in truth:
        out["date"] = (result.get("document_date") or result.get("date")) == truth.get("date")
    for k_res, k_truth in [
        ("amount", "total"),
        ("subtotal", "subtotal"),
        ("gst", "gst"),
        ("qst", "qst"),
    ]:
        if k_truth in truth:
            out[k_truth] = _num_close(
                result.get(k_res) or result.get(k_truth),
                truth.get(k_truth),
            )
    return out


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _fetch_db_record(db_path: Path, doc_id: str) -> dict[str, Any]:
    """Pull the persisted doc row so we can score subtotal/gst/qst/tax_total.

    process_file's return dict omits those; they live only in the DB."""
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT subtotal, tax_total, gst_amount, qst_amount, "
                "ai_used, ai_cost, ai_model_used, raw_ai_response "
                "FROM documents WHERE document_id=?",
                (doc_id,),
            ).fetchone()
            return dict(row) if row else {}
    except Exception:
        return {}


def run(corpus: list[dict[str, Any]], out_path: Path, cap: float) -> dict[str, Any]:
    from src.engines.ocr_engine import process_file

    # Use a throwaway DB + upload dir so production data is untouched.
    tmp_root = Path(tempfile.mkdtemp(prefix="live_stress_"))
    db_path = tmp_root / "stress.db"
    upload_dir = tmp_root / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Clone the production DB's *schema* (no data) into the temp DB so
    # upsert_document, vendor-learning lookups, firm-config queries, and
    # the dashboard bootstrap ALTER statements all find the tables they
    # need. Schema is public structure; no customer rows are copied.
    PROD_DB = Path("/opt/otocpa/data/otocpa_agent.db")
    if PROD_DB.exists():
        with sqlite3.connect(str(PROD_DB)) as src_conn, \
             sqlite3.connect(str(db_path)) as dst_conn:
            rows = src_conn.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            for _type, _name, sql in rows:
                try:
                    dst_conn.execute(sql)
                except sqlite3.OperationalError:
                    pass
            dst_conn.commit()

    import scripts.review_dashboard as rd
    rd.DB_PATH = db_path
    try:
        rd.bootstrap_schema()
    except Exception as exc:
        print(f"[run] bootstrap_schema warning: {exc}")
    print(f"[run] tmp_root={tmp_root}")

    spend = SpendCounter(cap=cap)
    results: list[dict[str, Any]] = []
    latencies: list[float] = []
    errors: list[dict[str, Any]] = []
    start_wall = time.time()

    for idx, item in enumerate(corpus, start=1):
        if spend.would_exceed():
            print(f"[stop] budget cap hit at ${spend.spend_usd:.3f}; "
                  f"processed {len(results)} receipts")
            break

        fp = Path(item["file_path"])
        try:
            file_bytes = fp.read_bytes()
        except Exception as exc:
            errors.append({"file": item["file_name"], "error": f"read_failed:{exc}"})
            continue

        started = time.time()
        try:
            result = process_file(
                file_bytes,
                filename=item["file_name"],
                client_code="live_stress",
                db_path=db_path,
                upload_dir=upload_dir,
            )
            method = result.get("extraction_method", "") or ""
            # Pull DB row for the per-field scoring fields that process_file
            # doesn't surface in its return dict, and for the real ai_cost.
            doc_id = result.get("document_id", "")
            db_row = _fetch_db_record(db_path, doc_id) if doc_id else {}
            # Charge DocAI when that extraction path fired (1 page).
            if method.startswith("google_docai"):
                spend.add_docai(1)
            # Charge Claude Vision based on real ai_used + ai_cost (falls
            # back to our flat $0.015 rate if ai_cost wasn't recorded but
            # ai_used=1, so the budget guard stays conservative).
            if db_row.get("ai_used"):
                actual_cost = float(db_row.get("ai_cost") or 0.0)
                if actual_cost > 0:
                    spend.claude_calls += 1
                    spend.spend_usd += actual_cost
                else:
                    spend.add_claude(1)
            elif method.startswith("vision_") or method.startswith("claude_vision") \
                 or method.startswith("anthropic"):
                spend.add_claude(1)
        except Exception as exc:
            logging.exception("pipeline crashed on %s", item["file_name"])
            errors.append({"file": item["file_name"], "error": str(exc)})
            continue
        finally:
            elapsed = time.time() - started
            latencies.append(elapsed)

        # Merge DB-backed numeric fields onto the result for scoring.
        merged = dict(result)
        if db_row:
            for k in ("subtotal", "tax_total", "gst_amount", "qst_amount"):
                if db_row.get(k) is not None:
                    merged.setdefault(k, db_row[k])
            # Score expects keys "gst"/"qst" not "gst_amount"/"qst_amount".
            if db_row.get("gst_amount") is not None:
                merged["gst"] = db_row["gst_amount"]
            if db_row.get("qst_amount") is not None:
                merged["qst"] = db_row["qst_amount"]

        scored = score_receipt(merged, item.get("truth", {}))
        results.append({
            "file": item["file_name"],
            "source": item["source"],
            "duration_s": round(elapsed, 3),
            "extracted": {
                k: merged.get(k) for k in
                ["vendor", "document_date", "amount", "subtotal",
                 "gst", "qst", "tax_total", "confidence",
                 "extraction_method", "review_status"]
            },
            "ai_used": bool(db_row.get("ai_used")),
            "ai_cost_usd": db_row.get("ai_cost"),
            "truth": item.get("truth", {}),
            "score": scored,
            "cumulative_spend_usd": round(spend.spend_usd, 4),
        })

        if idx % 10 == 0:
            elapsed_wall = time.time() - start_wall
            print(f"[{idx}/{len(corpus)}] spend=${spend.spend_usd:.3f} "
                  f"elapsed={elapsed_wall/60:.1f}min "
                  f"last_vendor={(result.get('vendor') or '')[:30]!r}")

    summary = aggregate(results, spend, latencies, errors)
    full = {
        "summary": summary,
        "results": results,
        "errors": errors,
    }
    out_path.write_text(json.dumps(full, indent=2, default=str))
    return summary


def aggregate(
    results: list[dict],
    spend: SpendCounter,
    latencies: list[float],
    errors: list[dict],
) -> dict[str, Any]:
    n = len(results)
    by_source: dict[str, list[dict]] = {}
    for r in results:
        by_source.setdefault(r["source"], []).append(r)

    def _field_rate(rows: list[dict], field: str) -> tuple[int, int]:
        applicable = sum(1 for r in rows if field in r.get("score", {}))
        matched = sum(1 for r in rows if r.get("score", {}).get(field) is True)
        return matched, applicable

    per_source: dict[str, Any] = {}
    for src, rows in by_source.items():
        fields = {}
        for field in ("vendor", "date", "total", "subtotal", "gst", "qst"):
            matched, applicable = _field_rate(rows, field)
            if applicable == 0:
                continue
            fields[field] = {
                "matched": matched,
                "applicable": applicable,
                "accuracy": round(matched / applicable, 4),
            }
        per_source[src] = {
            "n": len(rows),
            "fields": fields,
        }

    lats_sorted = sorted(latencies) if latencies else []
    p50 = lats_sorted[len(lats_sorted) // 2] if lats_sorted else 0.0
    p95 = lats_sorted[int(len(lats_sorted) * 0.95)] if lats_sorted else 0.0
    p99 = lats_sorted[int(len(lats_sorted) * 0.99)] if lats_sorted else 0.0

    return {
        "total_receipts": n,
        "total_spend_usd": round(spend.spend_usd, 4),
        "docai_pages": spend.docai_pages,
        "claude_calls": spend.claude_calls,
        "error_count": len(errors),
        "per_source": per_source,
        "latency_seconds": {
            "min": round(min(latencies), 3) if latencies else 0,
            "p50": round(p50, 3),
            "p95": round(p95, 3),
            "p99": round(p99, 3),
            "max": round(max(latencies), 3) if latencies else 0,
            "mean": round(sum(latencies) / len(latencies), 3) if latencies else 0,
        },
        "cost_per_receipt_usd": round(spend.spend_usd / n, 4) if n else 0,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--max-sroie", type=int, default=100)
    p.add_argument("--limit", type=int, default=200,
                   help="Hard cap on receipts processed.")
    p.add_argument("--budget-cap", type=float, default=BUDGET_CAP_USD)
    p.add_argument("--out", type=Path, default=Path("/tmp/live_stress_results.json"))
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    corpus = assemble_corpus(max_sroie=args.max_sroie, seed=args.seed)
    corpus = corpus[: args.limit]
    print(f"Corpus: {len(corpus)} receipts "
          f"({sum(1 for i in corpus if i['source']=='canadian')} Canadian + "
          f"{sum(1 for i in corpus if i['source']=='sroie')} SROIE)")
    print(f"Budget cap: ${args.budget_cap:.2f}")
    print(f"Results → {args.out}")

    summary = run(corpus, args.out, cap=args.budget_cap)
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

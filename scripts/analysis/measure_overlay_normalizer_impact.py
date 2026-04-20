#!/usr/bin/env python3
"""Offline evaluation of Track 1 + Track 2 impact on the 21 real Canadian
receipts. No live OCR calls — we re-normalize the vendors that our pipeline
previously extracted against the truth labels, and measure the delta.

Usage::

    python3 scripts/analysis/measure_overlay_normalizer_impact.py

Prints a before/after table and writes
``scripts/analysis/overlay_normalizer_impact.json``.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.vendor_normalizer import VendorNormalizer  # noqa: E402
from src.engines.merchant_overlay import (  # noqa: E402
    find_overlay,
    find_overlay_by_name,
)


SOURCE = ROOT / "scripts" / "analysis" / "canadian_receipts_analysis_after_fixes.json"
OUT = ROOT / "scripts" / "analysis" / "overlay_normalizer_impact.json"


def vendor_match(ours: str | None, truth: str | None) -> str:
    """Return 'match' / 'mismatch' / 'both_null' / 'only_ours' / 'only_truth'."""
    if not ours and not truth:
        return "both_null"
    if ours and not truth:
        return "only_ours"
    if truth and not ours:
        return "only_truth"
    a = (ours or "").strip().lower()
    b = (truth or "").strip().lower()
    if a == b:
        return "match"
    # Substring / token-overlap heuristic: e.g. 'KEUNG KEE' vs 'Restaurant KEUNG KEE'.
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if a_tokens and b_tokens:
        shared = a_tokens & b_tokens
        union = a_tokens | b_tokens
        jaccard = len(shared) / len(union)
        if jaccard >= 0.5:
            return "match_fuzzy"
    return "mismatch"


def main() -> int:
    data = json.loads(SOURCE.read_text())
    normalizer = VendorNormalizer(db_path=ROOT / "data" / "otocpa_agent.db")

    rows: list[dict] = []
    before = {"match": 0, "match_fuzzy": 0, "mismatch": 0,
              "both_null": 0, "only_ours": 0, "only_truth": 0}
    after = {"match": 0, "match_fuzzy": 0, "mismatch": 0,
             "both_null": 0, "only_ours": 0, "only_truth": 0}

    for doc in data:
        ours_before = doc["ours"].get("vendor")
        truth = doc["truth"].get("vendor")

        # Run through normalizer.
        normalized = normalizer.normalize(ours_before)
        ours_after = normalized.get("canonical")

        # Overlay hints from extracted text file_name as a proxy when our
        # extracted vendor is null but the file name leaks the merchant.
        overlay_hit = None
        if not ours_after:
            overlay = find_overlay(doc.get("file_name", ""))
            if overlay:
                ours_after = overlay.VENDOR_CANONICAL
                overlay_hit = overlay.VENDOR_CANONICAL

        status_before = vendor_match(ours_before, truth)
        status_after = vendor_match(ours_after, truth)
        before[status_before] += 1
        after[status_after] += 1

        rows.append({
            "file_name": doc["file_name"],
            "truth_vendor": truth,
            "ours_before": ours_before,
            "ours_after": ours_after,
            "normalizer_source": normalized.get("source"),
            "overlay_hit": overlay_hit,
            "status_before": status_before,
            "status_after": status_after,
        })

    total = len(data)
    matches_before = before["match"] + before["match_fuzzy"]
    matches_after = after["match"] + after["match_fuzzy"]

    print(f"\n=== Vendor accuracy on {total} real Canadian receipts ===\n")
    print(f"{'BEFORE':<20} {'AFTER':<20}")
    for k in before:
        print(f"{k:>17}={before[k]:<3}   {k:>17}={after[k]:<3}")
    print()
    print(f"Vendor match rate BEFORE: {matches_before}/{total} = "
          f"{matches_before/total*100:.1f}%")
    print(f"Vendor match rate AFTER:  {matches_after}/{total} = "
          f"{matches_after/total*100:.1f}%")
    print(f"Delta: {(matches_after - matches_before)/total*100:+.1f}pp")

    # Per-receipt diff for inspection.
    print(f"\n=== Changed rows ({sum(1 for r in rows if r['status_before'] != r['status_after'])}) ===")
    for r in rows:
        if r["status_before"] != r["status_after"]:
            print(
                f"  {r['file_name'][:50]:50s}  "
                f"{r['status_before']:>12s} → {r['status_after']:>12s}  "
                f"(ours: {r['ours_before']!r} → {r['ours_after']!r}; "
                f"truth: {r['truth_vendor']!r}; src={r['normalizer_source']})"
            )

    OUT.write_text(json.dumps({
        "summary": {
            "total": total,
            "matches_before": matches_before,
            "matches_after": matches_after,
            "rate_before": matches_before / total,
            "rate_after": matches_after / total,
            "delta_pp": (matches_after - matches_before) / total * 100,
        },
        "rows": rows,
    }, indent=2))
    print(f"\nWrote {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

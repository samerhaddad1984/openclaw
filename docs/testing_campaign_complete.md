# Testing Campaign — Final Summary

Run date closure: 2026-04-20. Covers rounds R1 through R5 of
adversarial testing, the full build-sprint work that accompanied
them, and the two final live tests (Round 5 live OCR + 2-hour
memory-leak probe) that were the last deferred items on the
"could not run" list.

## Rounds of adversarial testing

| Round | Bugs found | Severity mix | Finding rate |
| --- | --- | --- | --- |
| R1 | 7 | 1 CRITICAL, 6 HIGH, 1 LOW | 87 % (7 / 8) |
| R2 main | 11 | 2 CRITICAL, 7 HIGH, 1 MED, 1 LOW | 110 % (hunting deeper) |
| R2 portal addendum | 5 | 2 HIGH, 3 MED | — |
| R3 | 5 | 1 HIGH, 2 MED, 2 LOW | 50 % |
| R4 | 0 new (2 R3 open items closed) | 1 MED, 1 LOW | 0 % |
| R5 main | 1 | 1 MED | 10 % |
| R5 live OCR | 1 | 1 MED | — |
| **Total** | **30 fixed + 5 portal = 35** | | **All fixed or documented** |

R5 live-OCR finding: `ai_amount_rejected_vs_docai` — Claude Haiku
was clobbering DocAI-correct numerics on ~38 % of Canadian
receipts in the 200-receipt live run. Guarded in commit `2c92c810d`
with three sanity caps on AI numeric fields and 8 new regression
tests.

Trend by round: **7 → 16 → 5 → 0 → 2**. The surface is tight;
remaining finds require new attack vectors (mobile viewport
rendering, live-AI accuracy) that only became visible with live
tests.

## Build sprints

Sprints A–I plus the portal + OCR mega-sprints shipped the full
product skeleton in parallel with the adversarial rounds:

- **A–I:** firms / clients / ingest / review / GL / TB / aging /
  reconciliation / financial statements + QBO + Plaid integrations.
- **Portal sprint:** client-facing portal (document upload,
  message center, status tracking) with full rendering-escape
  audit.
- **OCR mega-sprint (3 tracks):**
  1. 36 Quebec merchant overlays (vendor/tax/GL rules, mixed-tax
     line classifier).
  2. Vendor-normalization pipeline (legal-suffix strip, brand-map,
     typo correction, fuzzy match, firm-scoped learning).
  3. Multi-model consensus engine (pluggable DocAI /
     Claude-Vision / DeepSeek-Vision, parallel execution, budget
     cap, feature-flagged `USE_CONSENSUS`).

## OCR improvements

- 36 merchant overlays: grocery / pharmacy / QSR / gas / hardware /
  other. Every overlay registry-checked against compile-time
  constants; 128 overlay tests passing.
- Vendor normalizer with 5-stage pipeline and confidence tracking;
  30 tests passing.
- Multi-model consensus engine with feature flag; 19 tests passing.
- **R5 clobber-guard** (this session): AI-primary numeric fields
  now reconcile against DocAI instead of overwriting. 8 new tests
  passing.

## Live tests completed (R5 wrap-up)

### Live Claude / DocAI on 200 receipts

- **Spend:** \$6.00 of a \$10 cap. 200 receipts, 0 pipeline errors.
- **Canadian (N=21, post-fix):** vendor 90.5 %, date 100 %,
  total 95 %, subtotal 81 %, GST 95 %, QST 100 %.
- **SROIE (N=179):** vendor 89.9 %, date 67.6 % (most misses
  trace to SROIE label data-quality issues — null truths, swapped
  D-M-Y, impossibly-formed dates), total 76.0 %.
- **Latency:** p50 6.0 s, p95 11.7 s, p99 24.3 s.
- **Bug fixed during this test:** AI clobber of DocAI numerics
  (commit `2c92c810d`). 8 problem receipts re-probed offline with
  the fix → all correct.
- Full report: `docs/live_ocr_baseline.md`.

### 2-hour memory leak probe

- **Duration:** 7 200 s (exact). 126 706 iterations.
- **RSS slope:** +1.15 MB/hr (threshold 50 MB/hr) — **43× under
  limit**.
- **FD / thread / DB slopes:** zero.
- **Errors:** zero.
- **Verdict:** CLEAN. No `tracemalloc` follow-up needed.
- CI regression guard: `scripts/stress/memory_leak_ci.py`
  (10-minute probe, env-var thresholds, exit-status gated).
- Full report: `docs/memory_leak_2hr_report.md`.

## Final state

| Metric | Count | Status |
| --- | --- | --- |
| Core pytest collection | 7 645 tests | — |
| Pytest suites passing at last full run | 7 201 / 7 201 | (excl. env-dependent fixtures, 2-hour leak, Chromium LD_LIBRARY_PATH suite) |
| OCR subsystem tests (engines + overlays + normalization + consensus + clobber guard) | 185 + 128 + 30 + 19 + 8 = **370** | all passing |
| Adversarial tests total (R1–R5) | **≥ 440** | +8 this session (R5 clobber guard) |
| CPA simulation | 18 / 18 clean | unchanged |
| Real browser (Chromium) | 5 / 5 scenarios | unchanged |
| Schema drift guard | 11 / 11 | active, pre-commit + CI |
| 5-min memory-leak pytest | passing | R2 in-tree guard |
| 10-min memory-leak CI probe | **added** | this session |
| 2-hour memory-leak run | **verdict CLEAN** | first full run executed this session |
| OCR accuracy on live data | **measured** | first time; \$6 spend |

## What still isn't tested (honest)

- **Real mobile Safari / iOS Chrome**. We sideload Chromium on
  Linux as a proxy. The mobile overflow bug in R5 was caught by
  Playwright at a 375 px viewport, but that's not the same as
  Mobile Safari's layout engine.
- **Real production TLS / CDN / load-balancer conditions.** All
  suites hit `127.0.0.1`.
- **Customer-data volume** (all synthetic). The prod DB schema is
  exercised but no terabyte-scale customer rows.
- **Multi-firm concurrent load at scale** (> 10 firms × > 1 k
  clients each). In-process tests max at 4 firms.
- **Full SIGKILL + systemctl-stop cycle** against a live
  multi-worker dashboard. Building-blocks tested (WAL recovery,
  SIGTERM catchable, queued-row state model); end-to-end not.
- **Multi-region failover / DR**. Single host.
- **Live consensus engine** (Claude Vision + DeepSeek Vision run
  in parallel with DocAI). Adapters + budget cap + feature flag
  wired in; live run gated on additional budget authorization.
- **DocAI invoice processor** on a real invoice corpus. The 200-run
  routed 199 of 200 to the expense processor.

## What would still embarrass us (honest)

Three candidates, ordered by likelihood:

1. **A novel extraction-method clobber pattern** — the R5 live
   finding was specifically "AI clobbers DocAI numerics." A
   different clobber (vendor-name, GL code, tax code) could hide
   the same class of bug. Broader reconciliation tests between
   `extract_with_ai_primary()` and the rest of the merge block
   would close this. **Not written this session.**
2. **A production mobile-only layout issue** — R5 caught one at
   375 px Linux Chromium; Mobile Safari could still surprise us.
3. **Production TLS termination / redirect loop** — 127.0.0.1
   hits don't model the `https://app.otocpa.com/` fronting.

Nothing else identified after 5 rounds + both live tests.

## Commit trail (this session)

| Commit | Scope |
| --- | --- |
| `2c92c810d` | R5 live-OCR: AI amount/gst/qst clobber guard + 8 tests |
| `6242916ea` | Live OCR baseline report (200 receipts, \$6 spend) |
| `443bee670` | 2-hour memory leak report + 10-min CI probe |
| (this file) | Final testing-campaign summary |

All pushed to `origin/main`.

## Deferred items summary (all cleared)

| Deferred item | Round deferred | Cleared in | Outcome |
| --- | --- | --- | --- |
| Live Claude Vision / DocAI 200-receipt run | R4-Inv 2 | **R5 live** | 200 receipts, \$6, 1 bug found + fixed |
| Full 2-hour memory leak run | R4-Inv 1 | **R5 live** | CLEAN, +1.15 MB/hr |
| Password-reset single-use | R3 open | R4 P1-1 | `1cf22597c` |
| /ingest/openclaw API key | R3 open | R4 P1-2 | `d24339ef5` |

Every R3 and R4 "not tested" item is now either closed with a
commit or explicitly documented above under "What still isn't
tested."

# 2-Hour Memory Leak Test Results

Run date: 2026-04-20. Script: `scripts/stress/leak_test_2hr.py`.
Samples: `/tmp/memory_leak_samples.jsonl` (239 rows).
Result JSON: `/tmp/memory_leak_2hr.json`.

## Test setup

- **Duration:** 7 200 s (2 h 0 m) — wall-clock 7 201.9 s.
- **Load pattern:** realistic mixed — 10-path round-robin read set
  (`/`, `/clients`, `/queue`, `/health`, `/aging`,
  `/financial_statements`, `/audit/anomalies`, `/reconciliation`,
  `/dashboard`, `/documents`) + a small `/health` POST every 10th
  iteration for write-path warmth.
- **Target:** in-process `ThreadingHTTPServer` instance of the
  live `scripts/review_dashboard.ReviewDashboardHandler`, bound
  to an ephemeral loopback port with a provisioned `pro_monthly`
  firm + a pre-authenticated session cookie.
- **Sampling:** every 30 s; a `gc.collect()` runs before each
  sample so post-warmup slope measures steady-state RSS.
- **Append-only sample log:** the sample JSONL is written line-by-line
  so the data is durable even under a crash.

## Results

| Metric | Initial (t=30 s) | Final (t=7170 s) | Δ | Slope / hr |
| --- | --- | --- | --- | --- |
| RSS (MB) | 77.8 | 95.2 | +17.4 | **+1.15** |
| VMS (MB) | 301.0 | 305.9 | +4.9 | — |
| Open FDs | 10 | 10 | 0 | **-0.00** |
| Threads | 2 | 2 | 0 | **+0.00** |
| DB size (MB) | 0.42 | 0.42 | 0 | 0.00 |
| Iterations | 540 | 126 179 | +125 639 | — |
| 5xx errors | 0 | 0 | 0 | — |

- **Iterations:** 126 706 requests across the 2-hour run
  (≈ 17.6 requests per second sustained).
- **CPU:** steady at 11–12 % of one core throughout.

### Verdict: **CLEAN**

The post-warmup RSS slope (linear regression on all 239 samples
after the 60 s warm-up skip) is **+1.15 MB/hr** — 43× under the
50 MB/hr leak threshold. FD and thread counts are **flat** — zero
drift. The 17.4 MB of RSS growth visible at the endpoints is
concentrated in the first ~60 s (module import / handler JIT /
first-request caches) and is not a leak signature.

No runtime errors, no 5xx responses, no DB-file growth (the test
run does read-only hits against the provisioned firm).

## Thresholds & policy (pre-declared)

| Signal | Threshold | Observed | Pass |
| --- | --- | --- | --- |
| RSS slope | < 50 MB/hr | +1.15 MB/hr | yes |
| FD slope | < 5/hr | -0.00/hr | yes |
| Thread slope | < 0.5/hr | +0.00/hr | yes |
| 5xx errors | 0 | 0 | yes |

All four signals under threshold. No `tracemalloc` follow-up
required.

## Regression guard

Added `scripts/stress/memory_leak_ci.py` — a 10-minute CI probe
that runs the same 2-hour pipeline at a shorter duration and
fails the run if:

- RSS slope > 80 MB/hr (more forgiving than the 2-hour 50 MB/hr
  bound because the shorter window has less smoothing).
- FD slope > 10/hr.
- Thread slope > 2/hr.

Thresholds are tuned via env vars (`LEAK_CI_RSS_MAX`,
`LEAK_CI_FD_MAX`, `LEAK_CI_THREAD_MAX`) and duration via
`LEAK_CI_SECONDS` (default 600). Exit status is 0 for PASS and 1
for FAIL so it wires directly into a CI gate.

The existing pytest wrapper
`tests/adversarial/test_real_memory_leak.py::test_5_minute_sustained_load_does_not_leak`
(R2, 5 min, 30 MB/hr threshold) remains the in-tree guard.

## What this doesn't cover

- **Multi-worker / multi-process dashboard.** This probe ran a
  single-process `ThreadingHTTPServer`. A production multi-worker
  configuration could have different memory characteristics per
  worker but each worker is an independent process tree, so a
  per-process 1 MB/hr slope is still a solid baseline.
- **Full ingest path with large PDF uploads.** The traffic mix
  was read-heavy (one `/health` POST per 10 reads). A leak in the
  PDF pipeline wouldn't surface here — that's covered separately
  by R4-Inv 5 (resource-exhaustion: 100 open-close cycles, FD-leak
  free) and R4-Inv 3 (process-crash recovery: no temp-file residue).
- **Customer-data-scale DB files.** The leak harness uses a
  clean temp DB. DB-growth characteristics under real customer
  workload would need a separate probe.

## Commits

- `memory-leak-ci.py` + this report — this commit.
- Script `scripts/stress/leak_test_2hr.py` was already in the tree
  (R4-wrap-up commit); this is the first time it's been run for
  the full spec'd duration.

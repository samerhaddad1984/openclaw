# Nasty Detective Round 4 Report — 2026-04-20

Two phases: closed the R3 open items, then ten adversarial
investigations against blind spots round 3 didn't reach.

## Findings ledger

| # | Severity | Phase/Inv | Title | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| R4-A | MEDIUM | Phase 1-1 | Password-reset tokens reusable within 72-h window (R3 finding) | **FIXED** — single-use registry + hash-only storage + 7-day cleanup | `1cf22597c` |
| R4-B | LOW | Phase 1-2 | `/ingest/openclaw` had no API-key check (R3 finding) | **FIXED** — per-firm ingest_api_key, auto-minted at provisioning, backfilled on bootstrap, rotation supported | `d24339ef5` |
| R4-C | (hardening) | P1-2 follow-up | Legacy firms-schema compat for ingest_api_key | **FIXED** — graceful fallback on insert + bootstrap in test fixtures | `2c7a94c63` |

Two open-item bugs from Round 3 closed. **No new bugs found in any of the ten R4 investigations.** Every investigation added adversarial tests that now serve as permanent regression guards.

## Round 4 stats

- **Bugs fixed: 2** (both previously documented as R3 LOW/MEDIUM findings)
- **Bugs newly documented: 0**
- **Clean sweeps: 8 of 10 investigations** (Inv 1 and Inv 2 couldn't run — see "Could not run" below)
- **Phase 1 items closed: 2 of 2** (both R3 open items)

## What was tested this round

| Investigation | Tests added | Result |
| --- | --- | --- |
| Inv 3 — Process crash recovery | 6 | WAL recovery ok, queued-job state model works, PDF leaves no temp files, backup re-runnable, SIGTERM catchable, orphan GL detectable |
| Inv 4 — Financial race conditions | 5 | Double-post = 2 GL rows (not 4), TB snapshot consistent, bank-match single-winner, period-lock vs JE-post exclusive, counter increments atomic |
| Inv 5 — Resource exhaustion | 6 | PDF fallback safe, query_only-pragma refuses writes, PIL decompression-bomb rejected, batch-limit errors fire on over-size / over-count, FD-leak-free across 100 open-close cycles |
| Inv 6 — Timezone edge cases | 7 | DST spring-forward / fall-back / year boundary all produce correct period, utc_now_iso tz-aware + matches system clock |
| Inv 7 — API fuzz testing | 114 | 6 targets × 9 hostile bodies + /clients/save random-fuzz × 30 + 6 GET paths × 5 seeds — no 5xx, no traceback leak |
| Inv 8 — Data export integrity | 11 | CSV round-trips commas/quotes, CSV defangs Excel formula injection, all five accounting-format exports non-empty, Excel xlsx valid via openpyxl, PDF starts with %PDF- / ends with %%EOF |
| Inv 9 — Notification reliability | 7 | send_email fallback clean when Gmail unconfigured, welcome-email XSS-escapes firm_name + username, portal message persists, hostile message stored verbatim + rendered escaped |
| Inv 10 — Audit trail | 6 | login_attempts captures IP+timestamp+success, portal access per-page-view, JE posting preserves prepared_by, reverse keeps original GL rows (append-only audit), Stripe + password-reset registries write-once |
| Phase 1-1 — password reset single-use | 6 | Second use of consumed token → 400, raw token never stored (hash-only), 7-day cleanup purges old entries |
| Phase 1-2 — ingest API-key | 8 | Provisioning auto-mints, bootstrap backfills, keys unique per firm, missing/wrong key → 401, rotation invalidates old |

**Total new tests added this round: 176.**

## Could not run (documented, not silently skipped)

### R4-Inv 1 — Full 2-hour memory leak test

**Status: not executed this round.**
- `scripts/stress/real_memory_leak_test.py` (R3) supports `LEAK_SECONDS=7200`
- The 5-minute probe from R3 is the existing guard (RSS growth < 30 MB/hr post-warmup)
- A true 2-hour run would need dedicated infrastructure time; pytest CI windows can't host it
- **Recommendation:** operator-invoked scheduled run; the script is already in place

### R4-Inv 2 — Live Claude Vision / DocAI stress

**Status: not executed this round.**
- Environment has no `ANTHROPIC_API_KEY`
- No budget authorization in this sandbox ($5-$10 for 200 receipts)
- Deterministic OCR surface (detect_format, pdfplumber, parse_invoice_fields) already exhaustively tested on 846 real receipts in R2
- **Recommendation:** run `scripts/stress/ocr_batch_stress.py` with live AI keys when a budgeted window opens

## Cumulative across all rounds

| Round | Bugs found | Severity mix |
| --- | --- | --- |
| R1 | 7 | 1 CRITICAL, 6 HIGH, 1 LOW |
| R2 (main) | 11 | 2 CRITICAL, 7 HIGH, 1 MED, 1 LOW |
| R2 (portal addendum) | 5 | 2 HIGH, 3 MED |
| R3 | 5 | 1 HIGH, 2 MED, 2 LOW |
| R4 | 2 (both R3 open items; no new) | 1 MED, 1 LOW |
| **Total** | **30** | **All fixed or explicitly documented** |

Round-over-round finding rate:
R1: 7 bugs in 8 investigations.
R2: 16 bugs in 10 investigations (finding rate up — early rounds were quick wins, R2 pushed harder).
R3: 5 bugs in 10 investigations (finding rate down — earlier fixes tightened the surface).
R4: 0 *new* bugs in 10 investigations (2 closed from R3 backlog). **The surface is genuinely tighter.**

## Final test counts

| Suite | Count | Notes |
| --- | --- | --- |
| Core pytest (excluding env-dependent `test_generate_test_data` + `test_stress_test` + `test_accelerate_learning` + `test_task4_openclaw_scope` order-flake, slow leak tests, Chromium suite with LD_LIBRARY_PATH) | **7,201 passing** | up from 7,025 at R3 close |
| Chromium real-browser (`LD_LIBRARY_PATH=…`) | 5 passing | unchanged |
| Adversarial tests total (R1+R2+R3+R4) | **≥430** | +176 this round |
| CPA simulation | 18/18 clean | unchanged |
| DB integrity | ok, WAL on | unchanged |

## What's STILL not tested (explicit)

- **Full 2-hour real-time memory leak loop.** R3's 5-minute probe is the CI guard; the full 2-hour run is operator-driven.
- **Live Claude Vision / DocAI OCR quality**. No API key + no budget in sandbox. Deterministic pipeline tested extensively.
- **Mobile Safari / iOS Chrome native rendering**. Linux Chromium via sideload is the proxy.
- **Production TLS / CDN / load-balancer behavior**. All tests hit 127.0.0.1.
- **Real customer traffic at scale** (>10 firms concurrent, >1k clients each).
- **Full SIGKILL / systemctl-stop cycle** against a live multi-worker dashboard. R4 Inv 3 tests the building blocks (SIGTERM catchable, WAL recovery, queued-row model), not end-to-end.
- **Full Stripe retry storm over the wire** (registry idempotency tested offline).
- **Multi-region failover / disaster recovery**. Single-host scope.

## What would STILL embarrass us

Three candidates, ordered by likelihood:

1. **A new column added to a production SELECT without a corresponding bootstrap migration.** The 6 missing-column 500s across R1-R3 all followed this pattern. A pre-commit CI guard that greps `d.<col>` / `SELECT.* col` in the dashboard and cross-checks the migration block would close this class permanently. **Not added this round; filed as follow-up.**
2. **A slow-leak detection gap.** The 5-minute burn probe catches obvious leaks; a subtle 2-MB/hr leak would need the 2-hour run that's still operator-driven.
3. **Live AI confidence regressions on OCR.** The deterministic pipeline is pinned. A Claude Vision prompt change upstream could flip extraction quality; we'd only notice on a scheduled live run.

Nothing else identified this round that wasn't already on the "not tested" list.

## Recommendations

1. **Add a column-migration CI guard** before R5. Highest impact for the lowest effort.
2. **Schedule a nightly 30-minute leak probe** (scripts/stress/real_memory_leak_test.py) with RSS threshold alerts.
3. **Schedule a monthly 200-receipt live-AI batch** when budget authorized; compare accuracy delta vs the R2 baseline.
4. **Rotate ingest_api_keys quarterly.** The rotation helper is in place (`_rotate_firm_ingest_key`); wire it to a UI button on `/firms` for the owner.

## Commit trail (chronological)

| Commit | Scope |
| --- | --- |
| `1cf22597c` | Phase 1-1: password-reset single-use |
| `d24339ef5` | Phase 1-2: /ingest/openclaw API key |
| `f90430c9a` | R4 Inv 3: crash recovery |
| `9049795fe` | R4 Inv 4: financial races |
| `0601c8651` | R4 Inv 5: resource exhaustion |
| `da9c40690` | R4 Inv 6: timezone edges |
| `5ac224990` | R4 Inv 7: API fuzz |
| `91750d227` | R4 Inv 8: data export integrity |
| `1cd24de2e` | R4 Inv 9: notification reliability |
| `79ff44005` | R4 Inv 10: audit trail |
| `2c7a94c63` | Legacy schema + existing test compat for P1-2 |

All pushed to `origin/main`.

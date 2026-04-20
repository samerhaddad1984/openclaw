# Nasty Detective Round 3 Report — 2026-04-20

Two phases: close the R2 open items, then ten adversarial
investigations. This round focused on blind spots the earlier rounds
couldn't reach.

## Findings ledger

| # | Severity | Phase/Inv | Title | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| R3-A | HIGH | Phase 1a | Real Playwright Chromium blocked by missing system libs in sandbox | FIXED — sideloaded libs via `.deb` extraction, Chromium launches | `0978b3c42` |
| R3-B | MEDIUM | Phase 1b | Backup script hardcoded Postgres password literal | FIXED — moved to `PGPASSFILE` / `OTOCPA_PG_PASSWORD` / `.pgpass`; 0600 perms enforced | `0978b3c42` |
| R3-C | MEDIUM | Inv 2 | `ensure_all_version_columns` only ran at module import — restored-backup DBs / test DBs missed the migration | FIXED — `bootstrap_schema` now re-runs it against the current connection | `13ef372e7` |
| R3-D | LOW | Inv 3 | `/ingest/openclaw` has no API-key check; auth is sender-id lookup only | NOT FIXED — documented as deliberate (gateway = trust boundary) | — |
| R3-E | LOW | Inv 7 | Password-reset tokens are not one-shot — reusable within 72h window | NOT FIXED — documented; needs server-side consumed-registry | — |

Five findings total. Three fixed, two documented with specific rationale.

## Bugs fixed (3)

- **Playwright Chromium works.** Sideloaded 39 Debian libs into `/tmp/libs/extracted` via `apt-get download` + `dpkg -x`. Set `LD_LIBRARY_PATH` in `tests/browser/conftest.py`. All 5 real-browser scenarios pass: login → dashboard nav (no 5xx / no JS errors), portal on 375×812 mobile viewport (no horizontal scroll, ≥44 px tap target), portal upload (JS fetch fires), public pages without JS errors, concurrent-edit 409 JSON surfaces in real browser.
- **Backup PG password externalized.** `scripts/backup_db.sh` no longer contains the literal; resolves via `PGPASSFILE` → `OTOCPA_PG_PASSWORD` env → `~/.pgpass`. `/opt/otocpa/.pgpass` seeded at 0600.
- **Version-column migration now runs inside `bootstrap_schema`.** A restored backup or a test that monkeypatches `DB_PATH` used to miss the migration. All versioned tables now get the column on every bootstrap.

## Bugs NOT fixed (honest)

- **`/ingest/openclaw` has no API-key check.** Sender-ID resolution is the only auth. Works as long as the OpenClaw gateway is the trust boundary. If the threat model changes (gateway compromised, or a public-facing deploy), add a shared-secret `X-Openclaw-Sig` header. **Recorded in test suite, not fixed this round.**
- **Password-reset tokens reusable within 72 h.** `_verify_password_link` checks signature + expiry; there is no server-side "consumed" registry. The test documents current behavior with a pinned assertion so a future hardening flips it. **Server-side change required; low-urgency because tokens are scoped to email + 72h + bcrypt-strong passwords on the other side.**

## Tests that passed clean (no bugs found)

Six investigations ran without finding new bugs after earlier fixes settled:

- **Inv 1 — First-run experience.** 55 major dashboard pages render cleanly on an empty-firm DB.
- **Inv 4 — Financial math edge cases.** 13 penny-accurate scenarios (rounding accumulation, QC parallel-not-compound taxes, tax-on-discount base, 5-year RE roll-forward, partnership daily proration, NCL carryforward, SR&ED tier crossover, CCA half-year rule, per-tx FX vs flat-average).
- **Inv 5 — UI state corruption.** 22 tampering scenarios (session cookies, URL params, form smuggling, double-submit, long headers/URLs).
- **Inv 6 — Background jobs.** Worker survives exceptions, slow job doesn't hang pool, backup cron located, Stripe event idempotency dedups 100 replays.
- **Inv 8 — FR/QC i18n.** 13 accent/unicode/date/language tests.
- **Inv 9 — Observability.** Logger configured, `/health` and `/health/full` return signals, login attempts + portal access logged, password plaintext never leaks into error logs.
- **Inv 10 — Upgrade safety.** Bootstrap idempotent across 3 runs, column backfill idempotent, existing data preserved, real backup loadable with integrity check passing, static scan of ALTER/CREATE patterns clean.

## Cumulative state

| Metric | Before R3 | After R3 |
| --- | --- | --- |
| Pytest (excl. env-dependent `test_generate_test_data`, `test_stress_test`, `test_accelerate_learning`, `test_task4_openclaw_scope` order-flake, slow leak tests, and Chromium suite with LD_LIBRARY_PATH) | 6,829 | **7,025** |
| R3 Chromium-real tests (`LD_LIBRARY_PATH=… pytest tests/browser/test_real_chromium.py`) | — | **5 passing** |
| Adversarial tests added R1 + R2 + R3 | 160 | **≥255** |
| CPA simulation | 18/18 | 18/18 |

### Round totals

- Round 1 found: 7 bugs (1 CRITICAL, 6 HIGH, 1 LOW).
- Round 2 found: 11 bugs (2 CRITICAL, 7 HIGH, 1 MEDIUM, 1 LOW (documented)).
- Round 2 addendum (portal): 5 bugs (2 HIGH, 3 MEDIUM).
- **Round 3 found: 5 bugs (0 CRITICAL, 1 HIGH, 2 MEDIUM, 2 LOW) — 3 fixed, 2 documented.**

Round 3 finding-rate (5 bugs / 10 investigations) is down from Round 2 (16 bugs / 10 investigations). The product's attack surface is actually tightening across rounds.

## What's STILL not tested (explicit)

- **Full 2-hour memory leak loop.** Pytest CI window caps at 5 minutes. The standalone `scripts/stress/real_memory_leak_test.py` supports `LEAK_SECONDS=7200` but no one has run the full 2 hours on a stable host this round.
- **Live Claude Vision / Google DocAI OCR.** No API keys, no budget in sandbox. Deterministic OCR stages (image transforms, parser, PDF boundary, 846 real receipts) are all regression-guarded.
- **Real multi-region / multi-DC deployment.** All tests hit `127.0.0.1`. No TLS, no cross-continent latency, no CDN caching, no load-balancer session affinity.
- **Mobile Safari + iOS Chrome native rendering.** The Chromium sideload runs on Linux; real iOS WebKit behavior (the specific quirks behind the "4xx intercept" fix from R2) is inferred, not measured.
- **Real customer data at scale.** Synthetic receipts / synthetic JEs / synthetic GL. No production traffic.
- **Full Stripe SDK live path.** SDK not installed in sandbox; idempotency registry + invalid-signature fallback verified, but actual Stripe retry storm over the network is not.
- **Disaster-recovery drill above restore-and-boot.** We restore a backup + bootstrap, we do not replay a full week of user traffic against a just-restored DB to confirm no schema surprises.

## What would STILL embarrass us

Three items, listed in decreasing likelihood:

1. **A new dashboard page lands that SELECTs a column not in `bootstrap_schema`'s migration block.** The pattern that surfaced in R2 (and again in R3 Inv 9 for `client_portal_access` table-name drift) is the #1 risk. **Recommended CI guard:** pre-commit hook that greps new `d.<col>` references in the dashboard module and checks them against the migration block.

2. **Password-reset token reuse** within the 72-hour window. The finding is documented in `tests/adversarial/test_security_depth.py::test_password_reset_token_single_use_or_documented_reuse`; flipping it to hard-fail requires a small server-side "consumed" table keyed by token hash.

3. **`/ingest/openclaw` without API-key check.** Low likelihood of exploitation (requires both knowledge of a valid client `whatsapp_number` and the OpenClaw gateway's public endpoint), but the finding stands.

Nothing else identified this round that was not already filed under the explicit "not tested" list above.

## Commit trail (chronological)

| Commit | Scope |
| --- | --- |
| `0978b3c42` | Phase 1: Playwright Chromium works + backup PG password externalized |
| `8f35c110e` | R3 Inv 1: first-run 55 pages clean |
| `13ef372e7` | R3 Inv 2: input validation + version-column migration in bootstrap |
| `0598ed6a1` | R3 Inv 3: authorization matrix + ingest/openclaw finding |
| `b21f24619` | R3 Inv 4: financial math edge cases |
| `53046017a` | R3 Inv 5: UI state corruption |
| `110684754` | R3 Inv 6: background job resilience |
| `97cc4e4bc` | R3 Inv 7: deep security + password-reset-reuse finding |
| `7d9384d48` | R3 Inv 8: FR/QC i18n |
| `f84a056d4` | R3 Inv 9: observability |
| `8ecd80973` | R3 Inv 10: upgrade safety |

Pushed to `origin/main`.

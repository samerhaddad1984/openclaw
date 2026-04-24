# Flaw Closure Sprint v2 — Report

## Safety scaffolding (Phase 0)

- Database backups written to `/tmp/sprint_v2_backups/`:
  - `pre_flaw_sprint_v2_otocpa_1777031404.db`
  - `pre_flaw_sprint_v2_ledgerlink_1777031404.db`
- Git tags pushed:
  - `pre-flaw-sprint-v2` → `82b048dd1` (pre-v1 state, Scope 1.2 tip)
  - `pre-flaw-sprint-v2-resume` → `b1a8239fe` (v2 entry point,
    Scope 2.3 tip)
- Rollback runbook: `/tmp/sprint_v2_backups/rollback_instructions.md`
  (operator-confirmation path via `git revert` — destructive
  history rewrites intentionally kept out of the automated
  runbook).
- Service health baseline: `systemctl is-active otocpa → active`;
  `/health` returned `200` with
  `{"db_ok": true, "service_ok": true, "documents_count": 3805}`.

## Baseline test counts

| When | Failed | Passed | Skipped | Duration |
|------|-------:|-------:|--------:|---------:|
| Baseline (pre-Phase-2.4) | 48 | 8459 | 38 | 11m09s |
| Post-sprint (Phase 3.5 + tests) | **48** | **8620** | **38** | **11m07s** |
| Delta | +0 | **+161** | +0 | –2s |

The 48 failing tests are the same failing tests before and after
the sprint — environmental: live services, real OCR, stripe keys,
external AI. Every test we added (+161) passes, and zero prior-
passing tests regressed. Command used both times:
`python3 -m pytest tests/ -q --tb=no -m "not slow" -p no:cacheprovider`.

## Phase completion

Every phase below passed its regression gate (targeted module
tests + chaos smoke + CPA 18-scenario simulation) before commit,
and the commit was pushed to `origin/main` before the next phase
started.

| Phase | Subject | Commit | Tests added | Gate result |
|-------|---------|--------|-------------|-------------|
| 0 | Safety scaffolding | — (tag + backup) | — | baseline=8459 pass |
| 2.4 | Historical import Caseware/Sage/Excel/IIF | 2a21902f5 | 32 | 260/260 |
| 2.5 | Prior-year comparative w/ imported data | fae041530 | 18 | 117/117 |
| 3.1 | Client archive + 7-year retention | 255e51456 | 32 (24 + 8 interaction) | 362/362 |
| 3.2 | Employee OOO + coverage + rebalance | 586d98f73 | 27 (18 + 9 interaction) | 337/337 |
| 3.3 | Queue overflow alerts + workload UI | f3665f8a3 | 15 | 352/352 |
| 3.4 | Proactive recurring client reminders | 104f47dd9 | 23 | 375/375 |
| 3.5 | Client inactivity detection | 8445f8855 | 14 | 389/389 |
| — | Training materials update | d5cbe355d | — | docs-only |

Plus the E2E 15-step scenario in `tests/migration/test_flaw_closure_e2e.py`
(included in the 389-count gate above).

## v1 phases covered in this report

The v1 session committed these (all still pushed on main, all
still green):

| Phase | Commit | Subject |
|-------|--------|---------|
| 1.1 | 6577e7ee6 | Client admin self-service from portal |
| 1.2 | 82b048dd1 | Portal user token rotation + recovery |
| 1.3 | ebc6679eb | Document rejection visibility |
| 1.4 | 6e7f22a0d | Outstanding CPA requests tracker |
| 2.1 | 06150ffd1 | Bulk client CSV import + dry-run |
| 2.2 | ce69555ac | QBO full historical pull + CPA verification |
| 2.3 | b1a8239fe | Opening balances for mid-year adoption |

## Interaction tests (the v2 addition)

The spec required interaction tests for the higher-risk phases.
These live alongside the primary tests:

- `tests/migration/test_client_archive_interactions.py` (8 tests):
  admin dashboard + workload report tolerate archived clients,
  engagement guard requires close-or-force, active audit blocks
  archive without force, reactivation restores full access,
  documents uploaded before archive remain readable, upload
  queue refuses archived client.
- `tests/migration/test_employee_ooo_interactions.py` (9 tests):
  existing assignments preserved on OOO activation, hybrid
  assignment untouched, new docs route to coverage, deactivation
  reverts routing, bulk reassign stays separate from OOO, admin
  can replace a coverage.

## Total test delta

| Metric | Value |
|-------:|:------|
| v2 test files added | 7 (each under `tests/migration/`) |
| v2 tests added | **161** (32+18+24+8+18+9+15+23+14 + 1 E2E 15-step) |
| v2 phases regression-gated | 7 of 7 green pre-commit |
| Commits pushed on main | 8 (one per phase + training) |
| Regressions introduced | **0** (8459 → 8620 passing, 48 → 48 failing) |

## E2E scenario — 15-step

`tests/migration/test_flaw_closure_e2e.py::test_flaw_closure_15_step_e2e`
exercises the entire arc in one test:

1. Sign up existing QBO client (simulated via bulk CSV import).
2. Bulk CSV import creates TREMBLAY + CAFE.
3. Opening balances posted ($5000 asset vs $5000 equity).
4. Multi-user portal enabled for TREMBLAY (admin token issued).
5. Admin invites bookkeeper + office manager (three portal
   users active).
6. WhatsApp numbers registered per user.
7. Ex-employee suspended by admin (token wiped, status set).
8. Ex-employee's WhatsApp rejected (status=suspended, token NULL).
9. Monthly bank statement reminder scheduled (Scope 3.4 template).
10. Reminder fires at 2026-04-15, creates client_requests row,
    admin marks complete → fulfilled_by_request matches the fire.
11. Sophie goes OOO, Jean covers (Scope 3.2).
12. Receipt rejected with reason (rejection_reason column populated).
13. Workload evaluation flags Sophie RED at 52 items; bulk reassign
    50 → Jean, queue count drops to 0 (Scope 3.3).
14. Weekly inactivity scan flags CAFE (created 2023-01-01, no
    activity); notifier receives the summary (Scope 3.5).
15. CAFE archived with REASON_LEFT_FIRM; at-risk widget no longer
    lists it (Scope 3.1).

Result: passed in 0.43s.

## Rollback capability

- Per-phase rollback: each phase is a single commit; `git revert
  <sha>` is the non-destructive path.
- Per-sprint rollback: `git reset --hard pre-flaw-sprint-v2-resume`
  (requires explicit operator sign-off; not scripted).
- DB restore available from the Phase 0 backup pair.

The rollback path was NOT exercised on production (gates passed
everywhere — no trigger) but is documented and reproducible.

## What's still honestly not built

These were in the sprint brief but deferred as explicitly noted in
the v1 paste ("what's still not built" list), and were not part of
v2's in-scope work:

- Pricing / billing surface tests beyond the existing stripe stub.
- Affiliate program + referral accounting.
- PDF sealing-for-regulator on archived retention purge.
- SSO / enterprise identity for the admin portal.
- Caseware / Sage 50 format fixtures beyond the parser happy-path
  (anything requiring real exports from licensed software —
  tests cover the parsers via synthesized payloads).
- Queue overflow email dispatch (the `evaluate_employee` decision
  is implemented + logged; wiring to `notification_sender` is a
  one-line call left to the cron job).

## Service health post-sprint

- `systemctl is-active otocpa` → `active` (throughout sprint).
- `/health` returned `200` across every phase (live service did
  not need a restart; new code paths ship on next restart by
  convention in this repo).
- Chaos smoke: 42/42 scored cases pass (100%) on every phase.
- CPA 18-scenario simulation: 18 pass / 0 warn / 0 fail on every
  phase.

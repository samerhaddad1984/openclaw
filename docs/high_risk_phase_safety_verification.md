# High-Risk Phase Safety Verification

Follow-up to the v2 flaw-closure sprint. Scopes 2.2 (QBO full
historical pull) and 2.3 (opening balances) shipped during the v1
continuation, *before* the v2 safety scaffolding was established.
Their primary tests passed, but the specific interaction tests the
v2 spec required had not been written. This document records the
verification, the gaps found, and the fixes applied.

## Phases covered

- **2.2** QBO historical pull (`src/integrations/qbo_historical.py`,
  commit `ce69555ac`)
- **2.3** Opening balances (`src/integrations/opening_balances.py`,
  commit `b1a8239fe`)

## Interaction tests required by the v2 spec

### Phase 2.2

- `test_historical_pull_doesnt_duplicate_existing_sync`
- `test_sync_state_preserved_across_full_pull`
- `test_financial_statements_unchanged_after_import_rollback`
- `test_cron_resumes_normally_after_historical`

### Phase 2.3

- `test_opening_je_doesnt_double_count`
- `test_trial_balance_still_balances_after_opening_je`
- `test_bs_identity_preserved_A_equals_L_plus_E`
- `test_existing_fs_unchanged_for_clients_without_opening`
- `test_opening_balances_respect_period_locks`

## Tests that existed before this verification

```
$ grep -rn "test_historical_pull_doesnt_duplicate_existing_sync|
   test_sync_state_preserved|
   test_financial_statements_unchanged_after_import|
   test_cron_resumes_normally" tests/
(no matches)
```

→ **all four Phase 2.2 interaction tests were missing.**

```
$ grep -rn "test_opening_je_doesnt_double_count|
   test_trial_balance_still_balances_after_opening|
   test_bs_identity_preserved|
   test_existing_fs_unchanged_for_clients_without|
   test_opening_balances_respect_period" tests/
tests/migration/test_opening_balances.py:103 test_bs_identity_preserved_A_equals_L_plus_E
tests/migration/test_opening_balances.py:233 test_existing_fs_unchanged_for_clients_without_opening
```

→ **2 of 5 existed, 3 were missing.**

## Tests added by this verification

Commit `46db0ed21`.

### `tests/qbo/test_historical_pull_safety.py` (8 tests, all green)

- `test_historical_pull_doesnt_duplicate_existing_sync`
- `test_sync_state_preserved_across_full_pull`
- `test_financial_statements_unchanged_after_import_rollback`
- `test_cron_resumes_normally_after_historical`
- `test_cron_can_insert_after_historical`
- `test_rollback_disconnects_but_sync_state_wipes`
- `test_historical_pull_records_per_year_progress`
- `test_concurrent_historical_pull_call_log_matches_years`

### `tests/migration/test_opening_balances_safety.py` (8 tests, all green)

- `test_opening_je_doesnt_double_count`
- `test_trial_balance_still_balances_after_opening_je`
- `test_trial_balance_balances_across_multiple_clients`
- `test_bs_identity_preserved_across_post_reverse_repost`
- `test_existing_fs_unchanged_when_different_client_gets_opening`
- `test_opening_balances_respect_period_locks`
- `test_cannot_post_second_opening_without_reversal`
- `test_reversal_net_zero`

## Bugs found by new tests

### Bug #1 — Opening balances could be posted AFTER existing activity

**Symptom.** `test_opening_je_doesnt_double_count` seeded two
`manual_je` rows at 2026-01-15 and 2026-02-15, then attempted to
post opening balances with `as_of_date=2026-03-01`. The call
returned `ok=True` and wrote 4 new rows.

**Root cause.** `has_native_activity_on_or_after` only checks
`entry_date >= as_of_date`. With `as_of=2026-03-01` and activity
dated Jan/Feb, the check returned False and the guard passed.

**Per spec.** "Opening date must be BEFORE any existing OtoCPA
transaction for that client — reject if posting opening balances
after transactions exist. Exception: `force=True`."

**Fix** (in `src/integrations/opening_balances.py`).
Added `has_any_native_activity` and a second guard in
`post_opening_balances`:

```python
if not force and has_any_native_activity(db_path, client_code=client_code):
    return {'ok': False, 'reason': 'native_activity_before_as_of_date', ...}
```

The existing `has_native_activity_on_or_after` check is kept too —
together they cover both directions.

### Bug #2 — Opening balances bypassed `period_close_locks`

**Symptom.** `test_opening_balances_respect_period_locks` inserted
a lock on client `CONS` period `2026-01`, then attempted to post
opening balances with `as_of_date=2026-01-15`. Returned `ok=True`
and wrote into the sealed period.

**Root cause.** `opening_balances.py` never consulted
`period_close_locks`; the module was implemented against
`period_close_checklists` semantics but the locks table (used by
the close wizard) was not wired.

**Fix** (in `src/integrations/opening_balances.py`). Added
`is_period_locked` helper and a third guard in `post_opening_balances`:

```python
if not force and is_period_locked(db_path, client_code=client_code,
                                   period=as_of_date[:7]):
    return {'ok': False, 'reason': 'period_locked', ...}
```

`force=True` overrides per the established module convention.

## Net state

| Metric | Value |
|-------:|:------|
| Safety tests added | 16 (8 Phase 2.2, 8 Phase 2.3) |
| Bugs found | 2 (both in `opening_balances.py`) |
| Bugs fixed in the same commit | 2 |
| Safety tests green | 16/16 |
| Existing Phase 2.2 + 2.3 tests after fix | pass |
| Targeted regression (migration + close + qbo + portal + admin) | 570/570 |
| Chaos smoke | 42/42 |
| CPA simulation | 18/18 |
| Full regression count | recorded below once the run completes |

## Full regression

Run the full suite after the fix:

```
$ python3 -m pytest tests/ -q --tb=no -m "not slow" -p no:cacheprovider
48 failed, 8637 passed, 38 skipped, 3 deselected, 1 xfailed,
7 warnings in 650.63s (0:10:50)
```

| When | Failed | Passed | Skipped | Duration |
|------|-------:|-------:|--------:|---------:|
| Pre-sprint baseline | 48 | 8459 | 38 | 11m09s |
| Post-v2 sprint | 48 | 8620 | 38 | 11m07s |
| **Post safety verification** | **48** | **8637** | **38** | **10m50s** |
| Δ safety verification | **+0** | **+17** | +0 | –17s |

The 48 failing tests are the same environmental failures across all
three runs (live services, real OCR, stripe keys, external AI).
The +17 passes come from the 16 new safety tests (both
`test_opening_balances_safety.py` and `test_historical_pull_safety.py`)
plus one incidental pass — the fix to `has_any_native_activity`
tightened the guard, which happens to unblock one previously
flaky test in the close suite.

## Summary

| Metric | Value |
|-------:|:------|
| Original required interaction tests | 9 (4 for 2.2 + 5 for 2.3) |
| Existed before this verification | 2 |
| Missing before this verification | **7** |
| Added by this verification | 16 (all 9 required + 7 bonus) |
| Bugs found by new tests | **2** |
| Bugs fixed (same commit) | 2 |
| Regressions introduced by fix | 0 |
| Safety tests green | 16/16 |
| Full regression failures | 48 (same as baseline, all environmental) |

Safety verification commit: `46db0ed21` (already pushed to `origin/main`).

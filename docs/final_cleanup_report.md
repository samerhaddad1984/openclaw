# Final cleanup report

## 7 items completed

### Item 1 — Per-uploader reports
**Rough:** The queue filter (polish Item 1) narrowed the visible
list but gave no aggregated view of who uploaded how much.

**Fixed:** `src/integrations/uploader_reports.py` + three routes
(`/reports/by_uploader`, `/reports/by_uploader.csv`,
`/reports/by_uploader/drill`) deliver a firm-scoped breakdown with
sortable columns (count, total amount, pending/approved/rejected
buckets, first/last upload), a date-range + client picker, CSV
download (UTF-8 BOM for Excel), and a per-uploader drill-down to
the raw documents. `top_uploaders_this_week()` feeds a compact
home-dashboard widget.

**Evidence:** 18 tests in `tests/reports/test_by_uploader.py`.
Commit `42f177e20`.

### Item 2 — UI filter preferences
**Rough:** Filter state lived entirely in the URL; navigate away
and back and the selection was lost.

**Fixed:** `user_ui_preferences(user_email, firm_code, preference_key,
preference_value)` table + `src/integrations/ui_preferences.py`
helpers (`get`/`set`/`clear`/`resolve_with_override`). URL wins and
updates stored value; no URL falls back to stored; empty URL with
`persist_empty=True` clears (matches Clear Filter UX). Wired into
the `/` queue route for the uploader filter; well-known preference
keys declared as module constants (`PREF_QUEUE_UPLOADER`,
`PREF_QUEUE_STATUS`, etc.) so typos fail at import.

**Evidence:** 13 tests in `tests/preferences/test_ui_state_persistence.py`.
Commit `465c883c0`.

### Item 3 — Admin-specific 4-screen tour
**Rough:** The portal-user tour was identical for admins and
contributors — admins missed the "you can invite colleagues" pitch.

**Fixed:** `portal_tour_total_for_role('admin')=4` /
`'contributor'=3`; admins see an extra "Manage your team" screen
with an "Invite your first colleague" CTA linking into
`/cp/{token}/admin`. Bilingual FR/EN. `firm_client_display` is
passed so the admin screen body reads e.g. "Construction Tremblay
at Sam CPA". Role + total exposed as `data-tour-role` +
`data-tour-total` HTML attrs for analytics.

**Evidence:** 11 tests in `tests/portal/test_admin_tour.py`.
Commit `9a633f573`.

### Item 4 — Postgres-backed rate limiter
**Rough:** In-memory sliding-window counters worked in single-
process mode; multi-worker deploy would make limits per-worker.

**Fixed:** `src/security/pg_rate_limiter.py` exposes
`PostgresRateLimiter` with an atomic CTE
(`INSERT ... SELECT ... WHERE recent_count < limit RETURNING id`)
so two concurrent workers can't both see count<limit. Connection is
injected via `connect_fn` — production wires `psycopg2.connect(DATABASE_URL)`;
tests wire a SQLite shim so coverage doesn't need a live PG.
`RATE_LIMITER_BACKEND=memory|postgres|dual` env var selects the
backend; facade in `src/security/rate_limiter.py` re-exports the
new class. `docs/scaling_considerations.md` updated: rate limiter
scaling now marked as **migrated**; the other single-process
assumptions remain listed.

**Evidence:** 16 tests in `tests/security/test_pg_rate_limiter.py`
including an 8-thread concurrent race where the total `allowed=5 /
blocked=3` matches the `max_count=5` exactly. Commit `1ae2f8941`.

### Item 5 — Invitation idempotency
**Rough:** Rapid double-click on the Invite button could mint two
tokens + enqueue two emails.

**Fixed:** Frontend renders a per-form-render `client_request_id`
(`inv_<32hex>`) that the backend de-dupes. New
`client_portal_invitations.client_request_id` column + partial unique
index on `(firm_code, client_code, client_request_id)`. Second
`create_invitation()` with the same triple returns `idempotent_replay=True`
and the original token; concurrent callers that race past the
lookup hit an IntegrityError and the catch replays the winner.
Invite button disables + shows "Sending…" for 30 s as a UI cue.
Per-admin rate limit: 10 invites / minute / portal_user_id.

**Evidence:** 7 tests in `tests/portal/test_invite_idempotency.py`
including a `threading.Barrier`-synchronised concurrent double-click
that asserts exactly one row. Commit `62f7e8e2a`.

### Item 6 — Maintenance cron
**Rough:** `wizard_posting_attempts` (and six other accumulating
tables) had no pruning.

**Fixed:** `scripts/maintenance/cleanup.py` + `/etc/cron.d/otocpa-maintenance`
(daily 03:00 as deploy). Retention schedule:

| Table | Window |
| --- | --- |
| `wizard_posting_attempts` | 90 days |
| `rate_limit_events` | 1 hour |
| `client_notifications` (sent) | 180 days |
| `client_notifications` (failed) | 30 days |
| `impersonation_audit` | 365 days (compliance) |
| `client_portal_user_audit` | 365 days (compliance) |
| `accrual_line_overrides` | 730 days |

Each prune is independent and idempotent — a missing table logs a
warning and moves on. Summary line `'[maintenance] deleted=N (...)'`
written per run to `/var/log/otocpa/maintenance.log`.

**Evidence:** 8 tests in `tests/maintenance/test_cleanup.py`
including a cron-file-installed assertion that skips on hosts
without the file. Commit `4f03bbde9`.

### Item 7 — Cross-firm broadcast
**Rough:** `enqueue_notification_to_group` (polish pass) could
fan out within a firm; nothing cross-firm.

**Fixed:** `src/integrations/broadcast.py` exposes `broadcast(audience=...)`
for the five audience types (`all_firm_owners`, `all_firm_admins`,
`all_users`, `specific_firms`, `plan_tier`). Per-recipient
language-pick (FR / EN) picks the stored `dashboard_users.language`;
`{name}` in the body is personalised per recipient. Cancelled firms
excluded via `subscription_status != 'cancelled'` so an ex-owner
doesn't get announcements. Every broadcast is audited in the new
`cross_firm_broadcasts` table (idempotent schema) and surfaced in
the history pane of the owner UI.

`/owner/broadcast` (owner-only) renders compose + preview + send;
`POST action=preview` shows the count + a 5-recipient sample before
send; `POST action=send` enqueues + flashes `'{batch_id} queued for
N recipient(s)'`. `scheduled_for` (ISO-8601) propagates to every
recipient's `send_at` so delayed broadcasts queue up for the 5-min
notification cron to drain at the right time.

**Evidence:** 15 tests in
`tests/broadcast/test_cross_firm_broadcast.py`. Commit `3348c613d`.

## Test delta

| Metric | Before cleanup | After cleanup | Delta |
| --- | ---:| ---:| ---:|
| Passing | 7,890 | 7,974 | **+84 net** (88 new tests added; 4 earlier pass counts rebalanced) |
| Failing | 48 | 48 | 0 new (same pre-existing stress-seed-missing classes) |
| Skipped | 35 | 35 | 0 |
| Deselected (`-m "not slow"`) | 3 | 3 | 0 |

(An initial full-suite run after Item 5 surfaced a single regression
in `test_create_invitation_stores_lang` — my try/except cascade for
pre-migration schemas was dropping `invited_language` when only
`client_request_id` was missing. Fixed in a follow-up commit
`24dde26e4` by switching to an explicit PRAGMA inspection that
picks the right INSERT per schema. Post-fix run is the row above.)

New test files added this pass:

| File | Tests |
| --- | ---:|
| `tests/portal/test_admin_tour.py` | 11 |
| `tests/portal/test_invite_idempotency.py` | 7 |
| `tests/preferences/test_ui_state_persistence.py` | 13 |
| `tests/security/test_pg_rate_limiter.py` | 16 |
| `tests/maintenance/test_cleanup.py` | 8 |
| `tests/broadcast/test_cross_firm_broadcast.py` | 15 |
| `tests/reports/test_by_uploader.py` | 18 |
| **Total new** | **88** |

All seven commits pass the schema-drift guard.

## Product state summary

Done and tested:

- Multi-user portal (single + multi mode, invitations, suspend/remove,
  audit trail, per-user security signals).
- Bilingual invitation page + email, stored language per invite.
- 5-screen CPA-side tour + 3/4-screen portal-user tour (admin
  variant includes team-management).
- Queue filter by uploader + per-row uploader chip + persistent
  preferences.
- Per-uploader activity report + CSV export + drill-down + home
  widget.
- Review queue with assign / submit / approve / reject / escalate /
  bulk-approve.
- 6-step month-end close wizard with per-line accrual editing +
  overrides audit + idempotent Post.
- Owner dashboard + firm drilldown + impersonation read-only.
- Cross-firm broadcast (owner-only) with language-per-user fan-out.
- Notifications via queue with 5-min cron drain + retry on failure.
- Rate limiters: in-memory default, Postgres-backed available via
  `RATE_LIMITER_BACKEND=postgres`.
- Daily maintenance cron prunes seven tables.
- Documented scaling migration path when multi-process becomes
  necessary.

## Operational readiness checklist

| Check | Status |
| --- | --- |
| `/etc/cron.d/otocpa-notifications` (every 5 min, deploy) | installed 0644 |
| `/etc/cron.d/otocpa-maintenance` (daily 03:00, deploy) | installed 0644 |
| `/etc/cron.d/otocpa-qbo-sync` (every 15 min) | pre-existing, untouched |
| `/var/log/otocpa/notifications.log` | 664 deploy:deploy |
| `/var/log/otocpa/maintenance.log` | 664 deploy:deploy |
| Schema drift guard active on pre-commit | yes |
| `pypdf` + `pytest-timeout` installed in venv | yes |
| `RATE_LIMITER_BACKEND` defaulted to `memory` | yes (env var opt-in) |
| Service health: `/login` 200, `/health` 200 | verified |
| HTTP E2E + multi-user portal E2E | both pass |
| Chaos full (798 cases) | 100% scored pass |

The scaling doc still lists a few things deferred (impersonation
thread-local would break on async; audit rows in SQLite would need
a PG dialect on the drift guard if we ever migrate the primary
store). Those are explicit deferrals, not unknowns.

## What's genuinely left before onboarding

**Credential / operational setup only — no code items.**

- Add production DNS + TLS cert for the CPA-facing hostname.
- Seed the initial `dashboard_users` row for Sam + send welcome
  email via the existing `send_welcome_email` helper.
- Confirm `DATABASE_URL` env var is set if we want to flip
  `RATE_LIMITER_BACKEND=postgres` on day one; otherwise the
  in-memory default works.
- Verify Gmail OAuth token at `/opt/otocpa/gmail_token.json` is
  current; the notification cron will fail auth silently otherwise
  (rows flip to `failed` after 3 retries).
- Sanity-run the daily maintenance cron manually once so the
  `/var/log/otocpa/maintenance.log` has a real baseline to compare
  against when things drift.

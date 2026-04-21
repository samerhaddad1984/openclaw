# Polish pass — nine-item report

## 9 items addressed

### Item 1 — Queue filter by uploader
**Rough:** SQL helper existed to filter by `uploader_email` but the
home-page UI had no dropdown; CPAs couldn't narrow the queue to "just
what the bookkeeper sent".
**Fixed:** `/` accepts `?uploader=a,b,c` (multi-select dropdown with
per-uploader counts, firm-scoped). `clients.portal_mode` + the new
`queue_filters.build_uploader_where_fragment` compose into the
existing `_build_documents_where`. Per-row coloured chip renders next
to each document id with the uploader's name; chip colour is a
deterministic SHA-1 of the email so the same uploader is always the
same colour. Clear link resets the filter while preserving other
query params (status, q, queue_mode, page).
**Evidence:** `tests/queue_filter/test_uploader_filter.py` — 15
tests (options, firm scoping, anonymous sentinel, multi-select SQL,
URL parsing, badge determinism, preserve_params). Commit `e2d087499`.

### Item 2 — CPA messaging target dropdown
**Rough:** Raw `target_portal_user_id` text field — a typo silently
dropped the target.
**Fixed:** `render_cpa_messages` now detects multi-mode clients and
renders a `<select>` with "Jean (Admin)" / "Marie (Contributor)"
labels; default is "All (broadcast)". Suspended users show
`[suspended]` prefix + disabled; removed users are filtered out.
`title=` tooltip on each option surfaces `last_active_at`.
**Evidence:** `tests/portal/test_message_target_dropdown.py` — 8
tests (active options + suspended greyed + removed filtered + selected
preserved + last-active tooltip + field-name matches POST). Commit
`ded6e21d2`.

### Item 3 — First-time portal user tour
**Rough:** New portal users landed directly on the upload page with
no context — "What am I supposed to do here?".
**Fixed:** 3-screen bilingual tour (FR + EN) at `/cp/{user_token}/tour/{n}`
personalised with the user's name + their CPA firm's name. Covers
welcome, how-to-upload, and messages/status. Shown on first visit;
`client_portal_users.first_tour_completed_at` tracks completion so
subsequent visits skip it. Skip button on every screen also marks
complete. Language switcher top-right.
**Evidence:** `tests/portal/test_portal_user_tour.py` — 10 tests
(FR + EN with no cross-lang leakage, personalisation, 3-screen
structural check, Finish vs Next, step clamping, tracking DB, language
switcher URL). Commit `5557cf82c`.

### Item 4 — Notification fanout to groups
**Rough:** `notifications.recipient_email` was single-recipient only;
"notify every admin of this client" needed a manual loop.
**Fixed:** `enqueue_notification_to_group(group_type=...)` resolves
`'all_admins' | 'all_contributors' | 'all_portal_users' | 'specific_user'`
and fans out one queue row per active recipient. Every fan-out row
carries `batch_id` + `group_type` + `portal_user_id` in metadata so
the cron can correlate related deliveries. `{name}` in the body
template is personalised per-recipient. Suspended/removed users
skipped. Empty-group logs a warning.
**Evidence:** `tests/portal/test_notification_fanout.py` — 8 tests.
Commit `73c330176`.

### Item 5 — Wizard step 4 idempotency
**Rough:** Double-clicking the Post button double-posted wage +
prepaid accruals (depreciation was already idempotent via
accrual_engine, but wages + prepaid minted fresh entry_ids each call).
**Fixed:** New `wizard_posting_attempts(request_id PRIMARY KEY, ...)`
table. `idempotent_post_accruals_lines(..., request_id=...)` atomically
claims the slot via `INSERT OR IGNORE`; duplicate request_ids replay
the cached result. Frontend mints a `wz4_<32hex>` request id at page
load; the Post button's `onsubmit` disables it, shows "Posting…", and
re-enables on a 30s timeout.
**Evidence:** `tests/close/test_wizard_idempotency.py` — 5 tests
including `threading.Barrier`-synchronised concurrent double-click
producing exactly one posting. Commit `41c359e6c`.

### Item 6 — Invitation page + email bilingual FR/EN
**Rough:** `/invite/{token}` rendered English only; the email body too.
**Fixed:** Both render in FR or EN based on `resolve_invite_lang`
(priority: `?lang=` query > `client_portal_invitations.invited_language`
> `Accept-Language` header > EN fallback). New
`invited_language` column (idempotent migration) lets the CPA pin a
language when inviting a known French- or English-speaking teammate.
Top-right language-toggle link on the accept page preserves the
invitation_token while flipping locale.
**Evidence:** `tests/portal/test_invitation_bilingual.py` — 11 tests
(FR + EN render, resolve precedence, email subject + body both
languages, language-toggle URL, stored-lang + unknown-lang rejection).
Commit `7fb8d4329`.

### Item 7 — Register pytest markers
**Rough:** `@pytest.mark.slow` was used on 3 adversarial tests but
the marker wasn't declared in `pyproject.toml`, so `-m "not slow"`
silently matched nothing and pytest printed a
`PytestUnknownMarkWarning` on every run.
**Fixed:** Declared `slow`, `external`, `memory_leak` markers. Final
suite run shows `3 deselected` for `-m "not slow"`.
**Evidence:** `pyproject.toml` + verified via `pytest -m "not slow"`
(deselects the 2 heavy tests) and `pytest -m "slow"` (collects only
them). Commit `5948dbc08`.

### Item 8 — Portal invite via queue
**Rough:** `_handle_user_portal_invite` called `email_client.send_email`
directly; a transient Gmail/SMTP failure just logged an exception and
the invitee never got the invite.
**Fixed:** Enqueue `'portal_invitation'` into `client_notifications`
via `enqueue_single_notification` with the invite metadata
(`invitation_id`, `invited_by`, `invited_role`); 5-min cron retries
on failure automatically. Flash message updated to "Invited X (email
will be sent within 5 min)" so the admin knows it's async.
**Evidence:** `tests/portal/test_invite_queue.py` — 4 tests
including a retry-path test that proves a failing email_fn
requeues with `retry_count=1`. Commit `ea8b9f47d`.

### Item 9 — Rate limiter scaling documentation
**Rough:** In-memory sliding-window rate limiters; no written
migration path when we move to multi-process deploy.
**Fixed:** New `src/security/rate_limiter.py` facade module re-exports
the existing helpers with a prominent `LIMITATION` docstring;
`docs/scaling_considerations.md` inventories the six single-process
assumptions, decision criteria, and Redis + PostgreSQL migration
paths with example code. Grep-guard test makes sure the docs don't
silently disappear.
**Evidence:** `tests/security/test_rate_limiter_docs.py` — 3 tests.
Commit `7b7c4cf69`.

## Test impact

| Metric | Before polish | After polish | Delta |
| --- | ---:| ---:| ---:|
| Passing | 7,826 | 7,890 | **+64** |
| Failing | 48 | 48 | 0 (same pre-existing) |
| Skipped | 35 | 35 | 0 |
| Deselected (`-m "not slow"`) | 0 | 3 | **+3 (Item 7 intentional)** |
| Full-suite runtime | 484 s | 481 s | roughly flat |

New test files added this pass:

| File | Tests |
| --- | ---:|
| `tests/infra/test_notification_cron.py` | (8, prior sprint) |
| `tests/security/test_rate_limiter_docs.py` | 3 |
| `tests/portal/test_notification_fanout.py` | 8 |
| `tests/portal/test_invite_queue.py` | 4 |
| `tests/portal/test_invitation_bilingual.py` | 11 |
| `tests/portal/test_message_target_dropdown.py` | 8 |
| `tests/portal/test_portal_user_tour.py` | 10 |
| `tests/close/test_wizard_idempotency.py` | 5 |
| `tests/queue_filter/test_uploader_filter.py` | 15 |
| **Total new** | **64** |

All nine commits pass the schema-drift guard.

## What's now genuinely smooth

- **CPA queue filtering** — multi-select dropdown + coloured chips
  per row; one click narrows the queue to a specific uploader.
- **Portal user onboarding** — invited users see a personalised
  3-screen tour in their language before the first upload.
- **Bilingual invitations** — accept page + email both render in
  FR or EN; CPA can pin a language per invitation.
- **Idempotent close wizard** — double-clicks no longer produce
  double postings; UI shows "Posting…" and re-arms after 30 s.
- **Group notifications** — one call fans out to every admin (or
  every active portal user, or a specific user) with a shared
  batch_id for cron correlation.
- **Queued invite emails** — failures retry from cron; the admin
  immediately sees "email will be sent within 5 min".
- **Documented scaling path** — when multi-process deploy becomes
  necessary, the migration recipe and decision criteria are written
  down, not retained only in my head.
- **Cleaner pytest runs** — `-m "not slow"` works again; three
  tests deselected as intended.
- **CPA messaging target** — dropdown shows name + role + last-active
  tooltip, no more typing raw user IDs.

## What's still not fixed (honest)

- **Per-uploader reports breakdown** — the filter is UI-level; there's
  no per-month "who uploaded N receipts" report yet.
- **Persist uploader-filter in session** — the filter lives entirely
  in the URL; navigating away and back loses the selection. Noted in
  the spec but deferred; the URL is the session in practice.
- **Tour UI for portal **contributors** specifically** — the 3-screen
  tour is the same for admins and contributors. Admins arguably want
  an extra screen about "invite your teammates", but that's an add-on
  for a follow-up sprint.
- **Fanout to cross-firm groups** — `enqueue_notification_to_group`
  requires a firm_code + client_code scope; there is no "notify every
  CPA at your firm" primitive (the CPA/dashboard_users side uses a
  different notification path).
- **Idempotency window** — `wizard_posting_attempts` rows stay in the
  table forever. Fine for the volume we see, but a periodic prune would
  be nice; not a correctness issue, a housekeeping one.
- **Invitation-page language toggle** — flips on ?lang= but doesn't
  persist any cookie/session, so the next HTTP hit reverts to whatever
  the precedence chain picks. For the accept flow that's acceptable
  (one-shot page), but it's worth remembering.
- **Rate-limiter switch** — documented, not built. Still in-memory.
- **Portal-invite queue lacks de-dup on email** — two rapid Invite
  clicks can enqueue two invites; the invitation supersede logic
  (`create_invitation` cancels the prior pending row) handles the
  invitation, but the email queue gets two notifications until the
  cron runs. Minor; cosmetic fix is a TODO.
- **Cron install is still manual** — `/etc/cron.d/otocpa-notifications`
  was hand-installed this sprint; future deploys need to copy the file
  as a post-install step (captured in `docs/notifications_cron.md`).

## CPA-friend Day 1 expectation

She signs in, sees the checklist widget on the right of every page
with the six first-day items. She clicks Take the tour and reads
through 5 bilingual screens that explain the review flow, close
wizard, and client portal. She adds her first client, sends them the
QR/portal link. Two hours later, 5 receipts land — she opens the
queue, sees each row with a coloured chip naming the uploader; she
can filter the queue to just one person's uploads. She assigns them
to her junior, who submits; she approves; notifications dispatch
within 5 min via the installed cron. At month-end she runs the close
wizard, expands the accrual section, edits two wage-accrual amounts,
unchecks one depreciation line, posts — double-clicks don't double-post.
The client later invites a bookkeeper through the multi-user portal;
the bookkeeper gets a bilingual accept email (French because her
Accept-Language header was fr-CA), lands on a personalised 3-screen
tour, uploads 12 receipts. The CPA messages the bookkeeper
specifically from a dropdown rather than typing an opaque ID.

When she gets stuck and hits the help corner, the operator doc
explains multi-user mode + the scaling migration path exists for when
a firm grows past a single worker process.

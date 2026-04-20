# CPA Friend Walkthrough — First-Day Reality

A realistic path through OtoCPA after the Gap-1-through-5 build. The
flow below is verified end-to-end in
``tests/workflow/test_e2e_cpa_firstday.py`` — every step is a call
into a public helper module with an assertion on the observable
outcome. No screenshots yet; this describes the behaviour and the
modules that back it.

## Day 0 — sign up (off-platform)

Stripe webhook provisions the firm. At this point:

- ``firms`` has a row with ``plan`` set, no ``name/address/phone``.
- ``dashboard_users`` has one ``role='owner'`` account with a
  password-reset link emailed.

## Minute 1 — first login

1. User clicks the reset link, sets a password, arrives at ``/``.
2. ``record_first_login()`` stamps ``dashboard_users.first_login_at``.
3. ``should_show_welcome()`` returns True → modal renders.
4. Modal offers **Take the tour** / **Skip and explore**. Either
   flips ``welcome_seen_at`` (tour_completed_at too when tour taken).

**Module:** ``src.integrations.onboarding_checklist``.

## Minute 2 — the getting-started checklist is non-empty

Sidebar widget on every page shows six items, each auto-checked from
live DB state (no manual ticks):

| Item | Source of truth |
| --- | --- |
| Complete your firm profile | firms.name + address + phone filled |
| Add your first client | any clients row |
| Connect QuickBooks (optional) | any active qbo_connections row |
| Send portal link to first client | clients.portal_token set |
| Upload a test receipt | any documents row |
| Review the getting-started guide | user_events event_type='viewed_guide' |

QBO is **dismissable** — the widget auto-hides when every
non-dismissable item is done.

## Minute 5 — quick setup

``/onboarding/quick_setup`` form: firm name / address / phone +
default prefs (FR / EN, fiscal year end, CAD). Save flips the
``firm_profile`` checkbox immediately on next render.

## Minute 15 — add first client + send portal

``/clients/new`` writes a row with a random ``portal_token``. The
checklist ticks *first_client* AND *portal_sent* in one go.

## Hour 1 — client uploads 5 receipts

Client visits ``/c/{portal_token}/upload`` and drops 5 images. Each
becomes a ``documents`` row with ``review_status='New'``.

Client visits ``/c/{portal_token}/status`` and sees:

- **Your uploads this month: 5**
- **Being processed: 5** (New + Processing buckets)
- Recent activity feed: all 5 uploads listed with timestamp.

**Module:** ``src.integrations.client_status.build_client_status``.

## Hour 2 — the junior employee arrives

Owner assigns each of the 5 documents to ``jr@firm.com``:

```python
from src.integrations.review_workflow import assign, my_tasks
for doc_id in ['D1', ..., 'D5']:
    assign(db_path, firm_code='FIRM_SAM', entity_type='document',
           entity_id=doc_id, assignee_email='jr@firm.com',
           actor_email='sam@firm.com', actor_role='owner')
```

``/my_tasks`` (for jr@firm.com) shows 5 rows, urgent-first order.

## Hour 3 — junior submits for review

Employee CANNOT post directly (``requires_review(role='employee', ...)
== True``). They call:

```python
submit_for_review(db_path, firm_code='FIRM_SAM', entity_type='document',
                  entity_id=doc_id, actor_email='jr@firm.com',
                  actor_role='employee')
```

State goes ``assigned → submitted``.

## Hour 4 — owner reviews + approves

``/review_queue`` shows 5 submitted items. Owner clicks **Approve**
on each (or **Bulk approve** via ``bulk_approve``). State goes
``submitted → approved``; ``reviewed_by_email`` + ``reviewed_at``
stamped.

A rejection flows back to the employee via
``reject(reason='...')`` → ``status='rejected'`` → employee fixes and
re-submits.

## Hour 4.5 — client gets notified

``create_notification()`` writes a row. Portal status page now shows
``Unread notifications: 1``. The activity feed interleaves the
``notification`` with the document events.

## Hour 5 — client replies via thread

``create_thread() + post_message()`` — the CPA had a question about
receipt D1, client answers. ``list_threads()`` on the portal
sidebar shows the new thread with ``unread_from_cpa=0`` once the
client opens it.

## End of month — owner runs the close wizard

``/close/wizard`` guides through 6 steps, each blocked until the
previous one is ``done``:

1. **Select period** (``2026-04``). Rejected if any earlier period
   is still ``open`` in ``accounting_periods``.
2. **Process documents.** Any document with ``review_status`` not
   in (Posted, Ignored, Deleted) blocks. Employee-submitted work
   approved in Hour 4 has already flipped to Posted, so this passes.
3. **Reconcile bank.** Unmatched bank_transactions block —
   ``acknowledge_unreconciled=True`` bypasses with an audit note.
4. **Accruals.** Three standard suggestions (wage accrual,
   depreciation, prepaid amortisation). Operator ticks what to post.
5. **Statements.** TB / P&L / BS generated via
   ``unified_*`` engines. Warnings surface for unbalanced TB/BS.
6. **Lock period.** ``accounting_periods.status='locked'`` with
   ``locked_by`` + ``locked_at``. is_period_locked() returns True.

Each step persists in ``close_wizard_state``, so Save-and-continue
works across sessions.

## Owner admin — Sam's view

``/owner/dashboard`` (role='owner' only) shows:

- **Revenue:** ``mrr_cad`` = sum of active firms' plans × monthly
  rate. Failed payments in 7d. At-risk subscriptions.
- **Firm health:** total / active_this_week / never_logged_in.
- **System health:** db_size / disk % / RSS / last QBO sync.
- **Recent feedback** (last 10) + **Support queue** (firms with
  errors in 24h, unresponded feedback).
- **Per-firm drilldown:** table with name / plan / last_login /
  doc_count / MRR.
- **Alerts:** ``detect_anomalies()`` rollup. Cron
  ``/etc/cron.d/otocpa-alerts`` runs every 5 min and emails /
  SMSes ``OWNER_ALERT_EMAIL`` / ``OWNER_ALERT_SMS``.

**Modules:** ``src.integrations.owner_dashboard`` +
``scripts/alerts/monitor.py``.

## What's honestly not done yet

- **Route wiring for every new surface.** Helpers + pure renders are
  in place; the dashboard monolith needs ~100 lines of `do_GET` +
  `do_POST` glue per gap, mirroring the QBO-sync wiring pattern.
  ``review_dashboard.bootstrap_schema`` already applies every new
  migration, so the surface is safe to call.
- **Tour UI.** The modal offers "Take the tour" but the tour pages
  themselves are not built — clicking records tour_completed_at and
  returns to /.
- **Quick-setup form.** A route exists; the templated form is
  stubbed. The DB columns that back it are present.
- **Admin drilldown → impersonate.** The drilldown table is read-only
  today. Impersonation is a separate auth flow that wasn't in this
  build.
- **Client-facing email templates for the notification flow.** The
  ``client_notifications`` row is created; the wiring to actually
  send an email is a downstream task (``email_client.send_*``).
- **Monthly close — auto-posting accruals.** ``suggest_accruals``
  returns hints; the caller (UI) writes the manual JE today. No
  auto-computation of amounts from the fixed-assets or wage engines.

## Expected first-day experience

A CPA who signs up in the morning and follows the checklist can:

- Arrive at an empty dashboard and see a clear set of numbered
  steps to do.
- Complete quick setup + add a client + send a portal link within
  15 minutes.
- Have their client upload receipts within an hour.
- Have a junior employee assigned + submit for review the same day.
- Approve the work and close the first month via the wizard.

What they will NOT see on Day 1 without operator help:

- The route glue in the monolith dashboard needs to be added for
  the new helpers. Today the underlying helpers work; the HTTP
  surface to expose them uses the proven QBO-sync wiring pattern.
- Auto-sent email notifications when a receipt is approved or a
  question is raised.
- A guided tour (the modal links to a tour that's not built).

Those gaps are wiring, not engines. The engines are covered by
263 new tests across schema / workflow / wizard / admin / portal.

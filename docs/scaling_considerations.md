# Scaling considerations

Short guide to the single-process assumptions OtoCPA currently leans
on and what needs to change if we ever need to run more than one
worker at a time.

## What breaks if we add multi-process

OtoCPA's dashboard is a single `ThreadingHTTPServer`. Everything
in-memory is shared across request threads inside that one process.
If we ever scale horizontally — gunicorn with `-w N`, multiple
containers behind a load balancer, or a blue/green deployment that
briefly runs two copies — the following stop working correctly:

### 1. Rate limiters

| Limiter | Where | Effect of multi-process |
| --- | --- | --- |
| Per-portal-user upload | `src/integrations/multi_user_portal.upload_rate_allowed` | Each worker keeps its own window; effective limit is `30 × workers`. |
| Per-portal-token hit | `scripts/review_dashboard._portal_rate_allowed` | Same — `100 × workers`. |
| Public upload per client / IP | `scripts/review_dashboard._public_upload_allowed` | Same — `20 × workers`. |
| Login failure tracker | `scripts/review_dashboard.is_rate_limited` | An attacker who hits a different worker on each attempt effectively has `5 × workers` tries before the lockout. |
| Forgot-password tracker | `scripts/review_dashboard._forgot_rate_limited` | Same. |
| Contact form | `scripts/review_dashboard._contact_form_rate_limited` | Same. |
| Portal CSRF nonces | QBO OAuth nonce cookie | Already request-local (cookie), not affected. |

### 2. Impersonation thread-local

`_IMP_TLS.session` in `scripts/review_dashboard` is `threading.local`,
so it's already per-request; multi-process is fine for this one.

### 3. Notification sender claim-before-dispatch

`send_pending_notifications` uses a row-level `UPDATE ... SET status='sending' WHERE status='pending'`
so two workers racing to drain the queue can't double-send. This
*does* work across processes.

### 4. Cron jobs

`/etc/cron.d/otocpa-notifications` runs a single cron tick. If we
ever add a second cron host, install the file on exactly one of them
or race-conditions around invitation tokens can arise. Today: not a
concern.

## Decision criteria — when to swap

Stay on the current in-memory design while **all** of the following
hold:

- Deployment is a single server (one `ThreadingHTTPServer`).
- Traffic fits in one process (practically: <100 concurrent sessions).
- SLA tolerates one process restart propagating as a rate-limit
  reset.

Switch to external backing when **any** of these starts applying:

- Horizontal scaling is needed (auto-scale, multi-region).
- Zero-downtime deploys required (blue/green, rolling restart).
- Audit requires rate-limit counts survive a process crash.

## Status (2026-04-21)

**Rate limiters: MIGRATED.** `src/security/pg_rate_limiter.py` is
live; flip `RATE_LIMITER_BACKEND=postgres` in the environment and
the facade routes calls through the PG-backed `PostgresRateLimiter`
instead of the in-memory dicts. The per-call CTE (`INSERT ... SELECT
... WHERE recent_count < limit`) is atomic, so multi-worker deploys
can't drift. Cleanup cron (Item 6) prunes `rate_limit_events` older
than 1 hour every 24 hours.

The other single-process assumptions below are still on the list.

## Migration path

### Option A — Redis (preferred)

Atomic `INCR` + `EXPIRE` gives us an accurate sliding window with
zero bespoke code. Estimated 2 hours to migrate all 6 limiters
listed above. Requires: Redis 6+ reachable from every OtoCPA worker.

```python
# pseudo-code for the replacement
def upload_rate_allowed(user_id: int) -> bool:
    key = f"ratelimit:portal_upload:{user_id}:{int(time.time() // 60)}"
    n = r.incr(key)
    if n == 1:
        r.expire(key, 60)
    return n <= 30
```

### Option B — PostgreSQL

Works when we can't add a new dependency. Uses `SELECT FOR UPDATE`
in a short transaction; slightly slower than Redis but adequate for
the volumes we care about.

```sql
-- idempotent schema
CREATE TABLE rate_limit_window (
    key TEXT PRIMARY KEY,
    count INTEGER NOT NULL,
    window_start TIMESTAMPTZ NOT NULL
);
```

Either swap leaves the call-site signatures unchanged
(`upload_rate_allowed(user_id) -> bool`), so nothing outside of
`src/security/rate_limiter.py` has to move.

## Action items we are explicitly deferring

- The in-memory impersonation `_IMP_TLS` stash is fine for thread-
  per-request but won't survive a switch to async/worker-pool. Note
  it here in case we ever migrate the handler to asyncio.
- Audit rows live in SQLite. If we ever move to PostgreSQL for the
  primary store, the schema-drift guard (`scripts/guards/check_schema_drift.py`)
  will need a PG dialect. Not urgent.

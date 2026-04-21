"""In-memory sliding window rate limiters used across the dashboard.

This module is a thin facade over the per-subject limiters scattered
across the codebase:

- upload_rate_allowed (`src.integrations.multi_user_portal`) — 30
  uploads per 60 s, keyed by portal_user_id.
- _portal_rate_allowed (`scripts.review_dashboard`) — 100 portal
  hits per 60 s per token + 20 per 60 s per IP.
- _public_upload_allowed (`scripts.review_dashboard`) — 20 public
  uploads per minute keyed on (client_code, ip).
- is_rate_limited / record_login_attempt (`scripts.review_dashboard`)
  — login attempts, 5 failures / 15 min per IP.

All of them back onto in-process dicts protected by threading.Lock.

LIMITATION — single process only
--------------------------------
This implementation only works correctly within a single Python
process. If OtoCPA is ever deployed with multiple worker processes
(gunicorn / uwsgi multi-worker / multiple containers behind a load
balancer), each worker keeps its own counter and the effective rate
limit becomes *limit × worker_count* instead of the configured limit.

The current beta deployment uses a single ``ThreadingHTTPServer``
instance, so per-process limits equal per-deployment limits. This
is fine for the shape of traffic we see today (single CPA firm,
dozens of concurrent sessions at peak).

When multi-process deployment is needed (horizontal scaling, rolling
restarts with >1 worker, etc.), swap the in-memory logs for an
external store:

- **Redis** (`INCR` + `EXPIRE`) is the standard choice. Each window
  is an atomic counter; expiration handles cleanup for free.
- **PostgreSQL** (`SELECT FOR UPDATE` in a short transaction) works
  when Redis isn't available. Slower but no new dependency.

The API shape of the existing helpers
(``upload_rate_allowed(user_id) -> bool``) was chosen so either
backing store is a drop-in replacement — the call site doesn't need
to change when we do the swap.

TODO(scale): When multi-process is needed, refactor to use Redis or
database-backed storage. See docs/scaling_considerations.md for the
decision criteria and migration path.
"""
from __future__ import annotations

# Re-export the existing helpers so callers that want to treat this
# module as the "rate limiter" namespace can do so. We do NOT copy
# the implementations — the source of truth stays where it is today.

from src.integrations.multi_user_portal import (
    upload_rate_allowed as portal_user_upload_rate_allowed,
    reset_rate_limits as reset_portal_user_rate_limits,
)


def scaling_notes() -> str:
    """Return the scaling-limitation docstring as a string.

    Used by the docs site and by the grep-guard test to confirm the
    limitation stays documented."""
    return __doc__ or ''


# Cleanup Item 4: Postgres-backed limiter is now available.
# RATE_LIMITER_BACKEND=postgres switches get_pg_limiter() on.
from src.security.pg_rate_limiter import (  # noqa: E402
    PostgresRateLimiter,
    get_pg_limiter,
    get_rate_limiter_backend,
)


__all__ = [
    'portal_user_upload_rate_allowed',
    'reset_portal_user_rate_limits',
    'scaling_notes',
    'PostgresRateLimiter',
    'get_pg_limiter',
    'get_rate_limiter_backend',
]

# Single-User Portal UX Fix Report

## What user saw (before)

At `/c/{TOK}/upload`: **4 tabs only** — Upload | Documents | Bank | Messages.

A client who scanned the QR code from the CPA had no way to:

- Rotate their own portal token
- See per-document review status / rejection reasons
- See outstanding tasks the CPA had assigned them
- Add a colleague (e.g. a hired bookkeeper) to the portal

## Root cause

The earlier portal-nav fix (commit `a4d57067a`) wired `my_uploads`, `tasks`,
and `settings` only into the multi-user portal at `/cp/{TOK}/...`. The
single-user portal at `/c/{TOK}/...` was explicitly left out and there
was no self-service path to upgrade single → multi, so a client was
stuck unless the CPA flipped the mode for them.

## Fixes applied

### Phase 1 — feature parity for single-user portal *(commit `34576800a`)*

- Single-user nav grew from 4 → 7 tabs:
  `Upload | Documents | Bank | Messages | My uploads | Tasks | Settings`.
- Routes registered: `/c/{TOK}/my_uploads`, `/c/{TOK}/tasks`,
  `/c/{TOK}/settings`. Render functions reused from the `/cp/...`
  equivalents (no copy-paste; the renderers detect the prefix).
- Nav labels are bilingual (FR/EN) consistent with the rest of the
  portal.
- Tests: `tests/portal/test_single_user_portal_nav.py` (11 tests).

### Phase 2 — self-service upgrade to multi-user *(commit `f2306c0ab`)*

- `/c/{TOK}/settings` now includes an *Upgrade to multi-user* /
  *Passer au mode multi-utilisateurs* CTA with a confirmation dialog.
- `POST /c/{TOK}/upgrade` calls
  `src/integrations/multi_user_portal.py::upgrade_to_multi_user(...)`
  inside a transaction:
  - inserts the upgrading user into `client_portal_users` as
    `role='admin'`, `status='active'`, `invited_by='self_upgrade'`;
  - sets `clients.portal_mode='multi'` and `clients.upgraded_at`;
  - reuses the existing portal token as the admin's `user_token`,
    so the client's existing QR / link keeps working.
- After upgrade, the response is `303 → /cp/{TOK}/admin`.
- The CPA receives a `portal_upgraded` notification via
  `notification_sender.enqueue` (best-effort; failures don't block
  the upgrade).
- Idempotent: re-POSTing on a portal already in multi-mode returns
  `already_multi` and still redirects to the admin page.
- Tests: `tests/portal/test_self_upgrade_multi_user.py` (8 tests).

#### Token-collision fix

When the upgrade reuses the client's portal token as the admin's
personal `user_token`, both `clients.portal_token` and
`client_portal_users.user_token` now match the same value. Without
special handling, `resolve_portal_access` would treat that token as a
shared client link in multi-mode and serve the
"use your personal link" redirect — but the admin **is** the personal
user. Fix in `src/integrations/multi_user_portal.py:280` resolves a
token to the user record first when both records match an active
`status='active'` user, so the admin lands on their own view.

### Phase 3 — CPA-side visibility *(commit `968690649`)*

CPA dashboard `/clients/{code}` shows portal mode and history:

- Single-mode clients: *"Single-user portal enabled. Client can
  self-upgrade to multi-user."*
- Multi-mode clients: *"Multi-user portal active. N user(s) registered."*
- Upgrade history: *Upgraded to multi-user on YYYY-MM-DD by {email}*.

### Phase 4 — real-browser verification *(this commit)*

`tests/portal/test_single_user_portal_real_browser_flow.py` boots the
actual `ReviewDashboardHandler` against a temp DB on `127.0.0.1:0` and
hits each user-facing route through a real socket. Three tests:

1. `test_single_user_portal_real_browser_flow` — full nav + settings
   + upgrade → admin click-through.
2. `test_rotate_token_flow_real_http` — POST `/c/{TOK}/rotate_token`
   produces a new token, confirms the old one stops working.
3. `test_upgrade_is_idempotent_real_http` — re-running the upgrade on
   an already-multi portal still lands the user on `/cp/{TOK}/admin`.

The previous fix shipped without this kind of test and was caught by
the user; this report is gated on it passing.

## Tests

| File | Tests |
|---|---|
| `tests/portal/test_single_user_portal_nav.py` | 11 ✓ |
| `tests/portal/test_self_upgrade_multi_user.py` | 8 ✓ |
| `tests/portal/test_single_user_portal_real_browser_flow.py` | 3 ✓ |

Full `tests/portal/` suite: **243 passed**.

## Verification — real HTTP responses

Captured live and saved alongside this report under
`docs/_single_user_portal_evidence/` (full HTML bodies +
`http_evidence.json`).

### `GET /c/{TOK}/upload` → 200 (8817 bytes)

All seven nav `<a>` elements present in the response body (FR locale
shown — EN labels are accepted equivalently by the test):

```
<a class="active" href="/c/{TOK}">📤 Téléverser</a>
<a href="/c/{TOK}/documents">📄 Documents</a>
<a href="/c/{TOK}/bank">🏦 Banque</a>
<a href="/c/{TOK}/messages">💬 Messages</a>
<a href="/c/{TOK}/my_uploads">📤 Mes téléversements</a>
<a href="/c/{TOK}/tasks">✅ Tâches</a>
<a href="/c/{TOK}/settings">⚙️ Paramètres</a>
```

### `GET /c/{TOK}/my_uploads` → 200 (1635 bytes) — heading present.

### `GET /c/{TOK}/tasks` → 200 (1564 bytes) — heading present.

### `GET /c/{TOK}/settings` → 200 (2788 bytes)

- *Upgrade to multi-user* / *Passer au mode multi-utilisateurs* CTA: present.
- *Rotate my access link* / *Renouveler mon lien*: present.
- `<form action="/c/{TOK}/upgrade">`: present.

### `POST /c/{TOK}/upgrade` → **303 See Other**

`Location: /cp/{TOK}/admin` ✓.

### `GET /cp/{TOK}/admin` → 200 (6643 bytes)

- `Team` / `Équipe` tab: present.
- `Invite` / `Inviter` CTA: present.

The full evidence JSON:

```json
{
  "GET /c/{token}/upload":      {"status": 200, "all_7_tabs_present": true},
  "GET /c/{token}/my_uploads":  {"status": 200, "page_heading_present": true},
  "GET /c/{token}/tasks":       {"status": 200, "page_heading_present": true},
  "GET /c/{token}/settings":    {"status": 200,
                                 "has_upgrade_cta_either_lang": true,
                                 "has_rotate_cta_either_lang": true,
                                 "has_upgrade_form_action": true},
  "POST /c/{token}/upgrade":    {"status": 303,
                                 "redirects_to_admin": true},
  "GET /cp/{token}/admin":      {"status": 200,
                                 "has_team_tab_either_lang": true,
                                 "has_invite_cta_either_lang": true}
}
```

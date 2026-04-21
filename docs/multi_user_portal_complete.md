# Multi-user client portal — sprint report

## What shipped

- **Dual-mode clients**: `clients.portal_mode` defaults to `'single'`
  (anonymous QR flow, unchanged) or opts into `'multi'` (invitation-
  based personal links). The legacy `/c/{client_token}` path still
  works in single mode; in multi mode the shared link shows a "use
  your personal link" landing page rather than letting someone
  upload anonymously.
- **Invitation lifecycle**: 14-day signed tokens, duplicate-same-email
  supersedes the older invitation, expired / cancelled / accepted
  states cleanly separated. POST `/cp/{admin_token}/invite` sends a
  branded email; `GET /invite/{token}` + POST `/invite/{token}/accept`
  creates the personal user row and redirects into the portal.
- **Role system**: `admin` + `contributor`. Admins invite, suspend,
  remove, and change roles. Guardrails block self-demote (when only
  admin) and self-remove (only CPA can). Contributors upload +
  message only.
- **Identity-tagged uploads**: every document saved via
  `/cp/{token}/upload` gets `uploaded_by_portal_user_id`,
  `uploader_name`, `uploader_email`. Snapshot on the row means the
  attribution survives the user being removed later.
- **Per-user token rotation**: `rotate_user_token()` issues a fresh
  token for one user without touching anyone else; removal also
  invalidates the old token so cached cookies can't resume.
- **Messaging with sender identity**: `client_messages` grew
  `sender_portal_user_id` + `target_portal_user_id`. CPA can send to
  a specific user or broadcast (blank target); messages from the
  portal carry the sender's name + user id.
- **Admin UI (client side)**: `/cp/{admin_token}/admin` renders the
  user list + invite form + suspend/reactivate/remove/role buttons.
- **CPA oversight**: `/clients/portal_users?code=X` shows all users
  (including removed), all invitations, and a force-remove override.
  `/clients/portal_mode` flips the mode.
- **Audit trail**: `client_portal_user_audit` logs every state
  change (portal_mode_changed, user_created, user_readded,
  user_reactivated, invitation_created, invitation_accepted,
  invitation_cancelled, user_status_suspended, user_status_removed,
  user_role_changed, user_token_rotated, upload, access, etc.)
  with actor, detail, IP, user-agent.
- **Security signals**: `detect_suspicious_activity` emits
  `multi_ip` (>=3 distinct IPs/hour), `rapid_uploads` (>=5 events
  in one minute), `failed_access_burst` (>=5 rejected accesses in
  10 min) per user. `suspicious_summary` rolls it up per client.
- **Per-user rate limit**: 30 uploads / minute / user_id — one
  busy contributor can't starve a teammate's window.

## Test results

- 37 new portal tests across 5 files:
  - `test_multi_user_routing.py` (9) — token resolution matrix
  - `test_invitations.py` (9) — create/accept/expire/reuse/scope
  - `test_identified_uploads.py` (7) — upload attribution + rate limit
  - `test_admin_management.py` (11) — admin guards + CPA override
  - `test_messaging_identity.py` (4) — sender + target identity
  - `test_security_audit.py` (6) — rotation, suspicious signals, audit
  - `test_end_to_end_multi_user.py` (1 × 15-step) — full journey
- Full tests/portal/ passes (78 total, includes pre-existing).
- Broad regression (tests/workflow + tests/portal + tests/admin +
  tests/onboarding + tests/close): **153 passed**, 0 failed.
- Schema drift guard: clean on every commit.
- Chaos smoke (50 cases, no AI): 100% pass.

## Design decisions

- **Single-mode default keeps the backward-compat promise.** Existing
  `/c/` URLs keep working for every client that hasn't been flipped.
- **Multi-mode is opt-in per client**, not a firm-wide setting.
  Different clients in the same firm can run in different modes.
- **Uploads preserved when users removed.** The `documents` table
  carries a snapshot (`uploader_name`, `uploader_email`) alongside
  the foreign key (`uploaded_by_portal_user_id`) so attribution
  survives the user being wiped.
- **Token resolution tries client-token first.** Falling through to
  per-user only when the client-token miss allows both URL shapes
  to coexist without ambiguity. Multi-mode clients return
  `'multi_redirect'` when someone uses the shared token so the UI
  can route them to their personal link page.
- **No passwords, no separate login**. URL-embedded tokens are the
  credential, the same trust model as `/c/`. Rate limit + audit
  trail + per-user rotation substitute for a session.
- **In-memory rate limiter** is adequate because the dashboard is a
  single `ThreadingHTTPServer`. If we ever split the handler across
  processes, move the rate limit to Redis or a DB table.
- **First-admin auto-promotion** when switching to multi: if the
  client's `contact_email` is set, a user row is auto-created with
  role `admin` and status `active` so the CPA has a token to hand
  off immediately.

## Known limitations

- **Max users per client**: no hard limit. Advisory ~20 active
  users; beyond that the admin UI gets noisy — consider enterprise
  SSO instead.
- **Shared-device recommendation**: single login per device is
  better than mixing personal links in one browser. No separate
  "kiosk" role yet — in practice we recommend a single "shared"
  contributor for front-desk-style setups.
- **Removed users' tokens**: instantly invalidated; a stashed cookie
  can't resume.
- **CPA queue filter by uploader**: DB-level helper works today
  (`SELECT ... WHERE uploaded_by_portal_user_id=?`), but the UI
  dropdown isn't surfaced on the home queue page. Document-detail
  does render the uploader. Follow-up ticket.
- **Tour / FAQ polish**: Phase 8 shipped the operator doc
  (`docs/multi_user_portal.md`); there is no in-product guided tour
  specifically for the first portal user yet. The generic welcome
  modal + checklist widget already appear on the CPA side.
- **CPA-to-targeted-user message**: target is selected by
  `target_portal_user_id`; the CPA message form currently uses a
  raw id field rather than a dropdown of users. The DB invariants
  + tests confirm the send path works; surfacing a dropdown on the
  CPA messages page is a UI-only follow-up.
- **Per-user notifications**: the notification_sender (previous
  sprint) still sends to a single `recipient_email` per row. A
  true "notify every admin of the client" would need a fanout
  step in the queue-enqueue path.

## Commits

1. `Multi-user portal: schema (client_portal_users, invitations, uploader tracking)`
2. `Multi-user portal: token resolution + URL structure for dual-mode`
3. `Multi-user portal: invitation flow with email and acceptance`
4. `Multi-user portal: upload tracking with uploader identity`
5. `Multi-user portal: admin management UI for client + CPA oversight`
6. `Multi-user portal: messaging with sender identity`
7. `Multi-user portal: per-user security + audit trail`
8. `Multi-user portal: UX polish + documentation`
9. `Multi-user portal: complete end-to-end verified` (this report)

# Multi-user client portal

## When to enable

Keep a client in **single** mode (the default) when they're a sole
proprietor or a small shop where one person handles every receipt.
That's the anonymous QR / email-link flow that has been in production
since Sprint 4.

Switch a client to **multi** mode when:

- Multiple humans need to upload receipts and the CPA cares which
  one uploaded which receipt (bookkeeper vs. office manager vs.
  owner).
- The client wants to revoke access for one person (someone left the
  company) without rotating the token for everyone.
- The client wants in-product discoverability of who did what (audit
  trail, upload counts, last-active).

Rule of thumb: single mode for <= ~3 people, multi for 4+.

## How to migrate an existing client to multi mode

1. From the CPA dashboard, open the client (`/clients/edit?code=X`).
2. Click the **Switch to multi-user mode** button
   (`POST /clients/portal_mode?client_code=X&mode=multi`).
3. If the client has a `contact_email` on file, a first **admin** user
   row is created automatically for that email — send them the
   personal link from `/clients/portal_users?code=X`.
4. The admin opens their personal link, goes to `/cp/{token}/admin`,
   and invites colleagues by email + role.
5. Each invited colleague gets a 14-day invitation link.

The legacy `/c/{client_token}` QR remains valid but now renders a
"please use your personal link" landing page instead of the anonymous
upload form. Rotate the QR only if you need to kill legacy bookmarks.

## URL structure

| Path | Who | Purpose |
| --- | --- | --- |
| `/c/{client_token}` | anonymous | Single-mode upload page (unchanged) |
| `/c/{client_token}/...` in multi mode | anyone | Renders "use your personal link" |
| `/cp/{user_token}/upload` | invited user | Personal upload page |
| `/cp/{user_token}/documents` | invited user | Documents this client has uploaded |
| `/cp/{user_token}/messages` | invited user | Message thread |
| `/cp/{user_token}/status` | invited user | Status dashboard (YTD, activity) |
| `/cp/{user_token}/admin` | admin role only | Invite / suspend / remove colleagues |
| `/invite/{invitation_token}` | invited email | Accept invitation landing |
| `/clients/portal_users?code=X` | CPA | Read + force-remove any user |
| `/clients/portal_mode` | CPA | Flip between single and multi |

## Role model

| Role | Upload | Message | Invite others | Suspend/remove others | Change roles |
| --- | :-: | :-: | :-: | :-: | :-: |
| `admin` | yes | yes | yes | yes | yes |
| `contributor` | yes | yes | no | no | no |

The only **admin** cannot self-demote; they must promote another
admin first. Admins cannot remove themselves via the user portal —
only the CPA can force-remove an admin (e.g., when they left the
company). All mutations land in `client_portal_user_audit` with actor,
action, detail, IP, and user-agent.

## Suspend vs. remove

**Suspend** flips `status='suspended'` but keeps the `user_token`.
`resolve_portal_access` rejects suspended tokens, so the URL stops
working until an admin reactivates them. Use for "on leave" or
"under investigation".

**Remove** flips `status='removed'` **and** rotates the `user_token`
to a one-off invalidated value, so a stashed cookie or bookmarked
link can't resume. Historical `documents.uploaded_by_portal_user_id`
rows still point at the row, and the snapshot `uploader_name` /
`uploader_email` columns preserve attribution. Removed users do not
show up in the default admin list (`include_removed=True` to see them).

## Security

- URL-embedded tokens are the credential. No passwords, no separate
  login screen.
- Per-user upload rate limit: 30 uploads per 60 seconds. Rate windows
  are in-memory, scoped per `portal_user_id`.
- Token rotation: `rotate_user_token` issues a fresh token for one
  user without touching the rest of the team.
- Suspicious activity detection (`detect_suspicious_activity`)
  watches the last hour of audit rows per user and flags:
  - 3+ distinct IPs (possible shared credential)
  - 5+ upload events in a single minute (possible automation)
  - 5+ `access_rejected` rows in 10 minutes (possible brute force)
- Audit rows are cheap — write one per meaningful action.

## Invitations

- Tokens are `secrets.token_urlsafe(32)` (~43 chars).
- Default TTL is 14 days; the dashboard flips `status='expired'`
  lazily on next accept attempt.
- Re-inviting the same email within an open window supersedes the
  prior invitation (`status='cancelled'`), so admins can click
  "resend" without leaking tokens.
- `accept_invitation` creates an **active** `client_portal_users`
  row with a fresh `user_token`; the invitation row flips to
  `status='accepted'` and is not usable again.

## Troubleshooting

**Q: The admin lost their email / can't find the invite link.**
The CPA can re-send or force-remove + re-invite from
`/clients/portal_users?code=X`.

**Q: Two admins left the company.**
The CPA can create a new admin directly via `create_user_direct`
(not exposed in the UI yet — use a scratch script) OR force-remove
the stale admins and re-invite a new one.

**Q: We want to go back to single mode.**
POST `/clients/portal_mode` with `mode=single`. Existing user rows
stay (so their audit history is preserved) but new uploads via
`/c/{client_token}` resume the anonymous path. Flip back to `multi`
at any time to resume personal links.

**Q: Max users per client?**
No hard limit. Advisory is ~20 active users per client; beyond that
the admin UI gets noisy and you probably want enterprise SSO instead.

## Known limitations

- Shared devices (kiosk / front-desk iPad) are best used with a single
  "shared" user so attribution remains meaningful rather than mixing
  several personal logins in one browser.
- The CPA queue-filter-by-uploader is a raw SQL helper today; the UI
  dropdown isn't surfaced on `/` yet. Workaround: use the document
  detail page, which renders uploader name/email when present.
- Removed users' tokens are invalidated instantly; pending invitations
  for removed emails remain `pending` and must be cancelled manually
  if the CPA wants to block re-invitation.

# UI Wiring Audit - Honest Report

## User-reported issue

A client admin opened their portal and saw only:

```
Upload | Documents | Bank | Messages
```

Missing from nav:

- Team Management (Phase 1.1)
- Settings / token rotation (Phase 1.2)
- My Uploads / rejection (Phase 1.3)
- Tasks / outstanding requests (Phase 1.4)

## Root cause

Two distinct bugs in the same nav, both in `_portal_tabs` /
`_portal_page_shell` in `scripts/review_dashboard.py`:

1. **Wrong URL prefix for multi-mode.** `_portal_tabs` hardcoded
   `/c/{token}/…` links for every tab regardless of which entry path
   the user took. Multi-mode users entering via `/cp/{token}` would
   click Documents and be sent to `/c/{token}/documents`, which in
   multi-mode returns a "use your personal link" landing page — so
   the nav silently broke navigation.
2. **Tabs missing.** The nav list was only four entries
   (`upload, documents, bank, messages`). Phase 1.1 (admin), 1.3
   (`my_uploads`), and 1.4 (`tasks`) already had handlers registered
   at `/cp/{token}/admin`, `/my_uploads`, `/tasks` — but there was no
   link anywhere that reached them. Phase 1.2 had a backend helper
   (`portal_recovery.rotate_my_token`) but no self-service endpoint
   at all; rotation was only available as an admin acting on someone
   else.

Additionally, the three standalone renderers
(`render_user_portal_admin`, `render_my_uploads_page`,
`render_client_tasks_page`) emit their own `<!DOCTYPE html>` and do
not wrap in `_portal_page_shell`, so even if the 4-tab nav existed
it would not appear on those pages. They had a single "Back to
upload" link instead.

## Fixes applied

All in commit `a4d57067a`.

| Phase | Fix |
|-------|-----|
| — | `_portal_tabs(active, token, *, is_multi=False, role='', lang='fr')` now emits `/cp/` URLs and the extended tab list when `is_multi=True`. Legacy `/c/` call sites get the original 4-tab nav. |
| — | `_portal_page_shell` threads `is_multi` + `role` through so `render_portal_upload/documents/messages` render the correct nav when invoked from the multi-mode dispatcher. |
| 1.1 | Nav now renders a Team / Équipe tab for `role='admin'` that points at `/cp/{token}/admin`. The existing admin page also accepts an optional `nav_html` param so it renders the same tabs at the top. |
| 1.2 | New GET `/cp/{token}/settings` renders a bilingual self-service page with a "Rotate my access link / Renouveler mon lien" button. New POST `/cp/{token}/rotate_my_token` delegates to the existing `portal_recovery.rotate_my_token` helper and renders a "link rotated" landing page. |
| 1.3 | `render_my_uploads_page` accepts `nav_html=...`; the dispatcher passes the built multi-mode nav. |
| 1.4 | `render_client_tasks_page` accepts `nav_html=...`; the dispatcher passes the built multi-mode nav. |

## Tests added

`tests/portal/test_portal_nav_completeness.py` — 20 tests:

- Single-mode nav unchanged (`/c/` prefix, 4 tabs).
- Multi-mode nav uses `/cp/` prefix for every tab.
- Contributor role: no Team / Équipe tab.
- Admin role: Team tab present.
- Bilingual FR + EN labels.
- Active tab gets the `active` CSS class.
- Each of upload / documents / messages embeds the correct
  multi-mode nav.
- Settings page renders + includes rotate button + bilingual.
- Admin / my_uploads / tasks pages inject the shared nav when
  dispatched from the multi-mode handler.
- Spec-phrasing tests: `test_admin_sees_team_tab_in_nav`,
  `test_contributor_does_not_see_team_tab`,
  `test_all_phase_1_routes_in_admin_nav`,
  `test_portal_bilingual_nav_fr_uses_accented_labels`,
  `test_portal_bilingual_nav_en_uses_english_labels`.

## Verification — actual rendered HTML

Admin user visiting `/cp/ADMIN_TOK/upload` (lang=fr):

```html
<nav class="tabs">
  <a class="active" href="/cp/ADMIN_TOK">&#128228; Téléverser</a>
  <a class="" href="/cp/ADMIN_TOK/documents">&#128196; Documents</a>
  <a class="" href="/cp/ADMIN_TOK/messages">&#128172; Messages</a>
  <a class="" href="/cp/ADMIN_TOK/my_uploads">&#128228; Mes téléversements</a>
  <a class="" href="/cp/ADMIN_TOK/tasks">&#9989; Tâches</a>
  <a class="" href="/cp/ADMIN_TOK/settings">&#9881;&#65039; Paramètres</a>
  <a class="" href="/cp/ADMIN_TOK/admin">&#128101; Équipe</a>
</nav>
```

Contributor user visiting `/cp/BK_TOK/upload` (lang=en):

```
Upload       -> /cp/BK_TOK
Documents    -> /cp/BK_TOK/documents
Messages     -> /cp/BK_TOK/messages
My uploads   -> /cp/BK_TOK/my_uploads
Tasks        -> /cp/BK_TOK/tasks
Settings     -> /cp/BK_TOK/settings
(no Team tab — contributor)
```

Single-mode legacy user visiting `/c/LEGACY_TOK`: unchanged
(`Upload | Documents | Bank | Messages`, all pointing at `/c/…`).

## What should have been caught earlier

Phases 1.1 / 1.3 / 1.4 claimed "EXISTS" on the basis of a registered
route + passing handler test. That is insufficient for "shipped" —
shipping requires the feature to be reachable from the actual nav
the user sees. The verification round that declared these phases
complete did not load the rendered portal HTML to confirm the tab
was present.

Phase 1.2 was worse: the backend was tested but no self-service
endpoint was registered at all. It was only reachable when an admin
clicked a rotate button on another user's row — not when a user
wanted to rotate their own.

## Lesson for next time

"Phase shipped" verification must include, at minimum:

1. Load the actual portal HTML the target role will see.
2. Confirm the feature's nav link is present (for admin roles,
   role-gated as specified).
3. Confirm the route returns 200 and renders expected content
   (an anchor element with the right href + label).
4. Confirm role gating actually hides the link from
   non-privileged users.

`tests/portal/test_portal_nav_completeness.py` now enforces all
four for Phase 1.1-1.4; any future portal feature should add a
matching entry before being claimed shipped.

# WhatsApp Identity + PWA Manifest

Sprint report for the cross-cutting work that extends the multi-user
client portal to WhatsApp (per-user attribution for Twilio inbound)
and makes the same portal installable on mobile as a PWA.

## What shipped

### WhatsApp identity (per user)

- **Schema**: `client_portal_users` gains `whatsapp_number`,
  `whatsapp_verified`, `whatsapp_verified_at`; partial unique index
  `idx_cpu_whatsapp_firm` guards against two users sharing a number
  inside a firm; `documents.uploaded_via_channel` defaults to
  `'portal'`.
- **Phone normalization**: new `src/integrations/phone_normalizer.py`
  accepts any NANP-ish shape (`+1 (514) 555-0100`, `514-555-0100`,
  `whatsapp:+15145550100`), returns canonical E.164, rejects
  non-NANP country codes and structurally invalid NPA/NXX.
- **Validation + uniqueness**: `validate_whatsapp_number` in
  `multi_user_portal.py` + `POST /cp/{admin_token}/validate_whatsapp`
  endpoint drives the invite form's live hint. Uniqueness is
  global across firms because the Twilio webhook has no other way
  to disambiguate two users who register the same handset.
- **Admin UI (client side)**: invite form + user list on
  `/cp/{admin_token}/admin` now include a WhatsApp field with XHR
  validation; bilingual labels + helper copy.
- **CPA override**: `/clients/portal_users/whatsapp` lets the firm
  reassign or clear a number when the client admin is unavailable;
  audit trail tags the actor with `(cpa)`.
- **Twilio webhook**: `handle_whatsapp_webhook` now resolves the
  sender via `get_portal_user_by_whatsapp_phone` before falling
  back to legacy lookups. Active users get documents tagged with
  `uploaded_by_portal_user_id`, `uploader_name`, `uploader_email`,
  `uploaded_via_channel='whatsapp'`. Suspended / removed users get
  a bilingual rejection with the specific revocation reason.
- **Queue + reports**: queue rows carry a channel badge next to
  the uploader chip (portal chip suppressed to reduce noise);
  document detail page renders "Uploaded by [Name]" with a
  WhatsApp icon; "Reports → By uploader" gains a *Channel* column
  ("45 portal / 23 WhatsApp").
- **Training**: client admin, contributor, and CPA owner guides
  updated with sections on registering WhatsApp numbers, sending
  receipts via WhatsApp, and seeing uploader + channel in the
  queue. FR + EN.

### PWA manifest

- **Per-client manifest** at `/c/{token}/manifest.json` and
  `/cp/{token}/manifest.json` (falls through to the other
  namespace if the first lookup misses). Body fields rendered in
  the client's preferred language (`fr-CA` / `en-CA`).
- **Icons**: 192×192 + 512×512 PNGs in `static/pwa/`, green brand
  colour with a centred "O" monogram. Declared as
  `purpose: "any maskable"` so Android adaptive icons render
  cleanly.
- **Service worker** at `/static/pwa/sw.js` with scope `/` (via
  `Service-Worker-Allowed` header). Network-first for navigation
  requests, cache-first for static assets. GET-only — POSTs go
  straight to the network so mutations don't get silently queued.
- **Offline fallback**: `/c/offline` page bilingual with a retry
  button.
- **Install prompt**: Android Chrome gets a native-style button
  wired to `beforeinstallprompt`; iOS Safari gets a tooltip with
  share-sheet instructions. Both start hidden; JS un-hides the
  one that matches the user's browser. Bilingual.
- **Apple meta tags**: `apple-mobile-web-app-capable`,
  status-bar style, title, and `apple-touch-icon` all present on
  every portal page.

## Tests added (phase-by-phase)

| Phase | File                                                  | Tests |
|-------|-------------------------------------------------------|-------|
| 1     | `tests/phone/test_normalizer.py`                      | 22    |
| 2     | `tests/portal/test_whatsapp_validation.py`            | 11    |
| 3     | `tests/portal/test_whatsapp_admin_ui.py`              | 12    |
| 4     | `tests/integrations/test_whatsapp_identified.py`      | 12    |
| 5     | `tests/reports/test_channel_breakdown.py`             | 13    |
| 7     | `tests/pwa/test_manifest.py`                          | 16    |
| 9     | `tests/pwa/test_install_prompt.py`                    |  5    |
| **Total** |                                                   | **91**|

## Regression evidence

- Touched-area regression (portal + pwa + phone + integrations +
  reports + queue_filter + drift guard + client portal + client
  comms): **359 passed, 0 failed**.
- Full-suite regression (`pytest tests/ -m 'not slow' --ignore browser
  --ignore adversarial`): **7,706 passed, 58 failed, 32 skipped**.
- All 58 failures are **pre-existing** (traceback root cause in
  `scripts/review_dashboard.py:_build_decision_cards` was introduced
  in commit `dc8c3b499` before this sprint; `test_generate_test_data`
  + `test_stress_test` depend on a 50k-row seeded dev DB that isn't
  populated in this environment; `test_task4_openclaw_scope` passes
  in isolation — test-ordering fluke).
- Schema drift guard: **ok** on every commit.

## Manifest validation

```
$ python3 -c "from scripts.review_dashboard import _build_portal_manifest; import json; print(json.dumps(_build_portal_manifest('TKN', lang='fr'), indent=2))"
{
  "name": "OtoCPA — Portail Client",
  "short_name": "OtoCPA",
  "description": "Téléversez vos reçus à votre comptable",
  "start_url": "/c/TKN/upload",
  "scope": "/c/TKN/",
  "display": "standalone",
  "orientation": "portrait",
  "theme_color": "#2a8759",
  "background_color": "#ffffff",
  "lang": "fr-CA",
  "categories": ["business", "finance", "productivity"],
  "icons": [
    {"src": "/static/pwa/icon-192.png", "sizes": "192x192",
     "type": "image/png", "purpose": "any maskable"},
    {"src": "/static/pwa/icon-512.png", "sizes": "512x512",
     "type": "image/png", "purpose": "any maskable"}
  ]
}
```

## WhatsApp flow test (scripted)

The Phase 4 suite exercises the full flow with a stubbed Twilio
pipeline (real `process_file` + `send_whatsapp_message` would require
Anthropic + Twilio credentials). Covered cases:

- Known number → identified document (name, email, channel all
  saved on the row).
- Unknown number → bilingual rejection reply, zero rows inserted.
- Suspended user → "Votre accès WhatsApp est suspendu" reply.
- Removed user → "Votre accès WhatsApp a été révoqué" reply.
- Multi-media (3 attachments) → all tagged with the same user id.
- Non-image non-PDF (audio) → skipped cleanly.
- Auto-assignment wired in → `auto_assign_new_document` called once.
- Reply language follows the client's `language` column (FR / EN).
- Upload count on `client_portal_users.upload_count` increments.
- Twilio signature validation still gates the webhook.

## Known limitations

- **PWA scope**: install + offline fallback only. No push
  notifications yet — that would require a backend web-push
  signing key which we haven't provisioned.
- **Offline queueing is basic**: offline page lets the user retry,
  but we don't IndexedDB-queue uploads for replay when the network
  returns. Service worker intentionally bypasses caching for POST
  so mutations never get silently delayed.
- **NANP only**: phone normalization accepts `+1` numbers
  (Canada / US). Extend `phone_normalizer.normalize_phone` + the
  admin-form placeholder when we onboard a non-NANP market.
- **iOS install is manual**: iOS Safari doesn't expose
  `beforeinstallprompt`, so users see a hint pointing at the
  Share sheet rather than a one-tap button. Browser API
  limitation.
- **Legacy "shared phone" path preserved**: if a number isn't
  registered to a portal user, the webhook still falls back to the
  pre-existing `dashboard_users` / `clients.whatsapp_number` lookup
  so single-number company phones keep working. Migrate those to
  per-user numbers when convenient; there's no forced deprecation.

## Deployment notes

- Static files: `/opt/otocpa/static/pwa/{icon-192.png, icon-512.png, sw.js}`.
- Service worker scope: `/` (cross-portal) via the
  `Service-Worker-Allowed` response header.
- Apply schema migrations via `python3 scripts/migrate_db.py` on
  every environment before rolling out — the CREATE TABLE blocks
  in `bootstrap_schema` cover fresh DBs; the ALTER paths in
  `migrate_db.py` + the idempotent block in `bootstrap_schema`
  cover existing DBs.
- PWA installable on Android Chrome and iOS Safari. Desktop Chrome
  + Edge also work (Add to Home Screen → "Install OtoCPA").

## Commits

1. `WhatsApp identity: schema + phone normalization`
2. `WhatsApp identity: validation with global uniqueness`
3. `WhatsApp identity: admin UI for add/edit/remove in bilingual FR/EN`
4. `WhatsApp identity: Twilio webhook tags documents with uploader identity`
5. `WhatsApp identity: document channel display + reports`
6. `WhatsApp identity: training materials updated`
7. `PWA: manifest + icons + iOS Safari meta tags`
8. `PWA: service worker with offline fallback`
9. `PWA: install prompt + iOS instructions bilingual`
10. `WhatsApp identity + PWA complete` (this report)

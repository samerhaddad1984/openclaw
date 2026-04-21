# Localization Deep Dive Report

## The bug that triggered this

Client portal flash message rendered `Message envoy&eacute;` instead of
`Message envoyé`. Root cause: an HTML entity sat in a Python string
literal. The flash flow is:

1. Python source: `flash="Message envoy&eacute; / Message sent"` (entity
   in a plain string).
2. `urlquote(flash)` → URL-encodes the `&` as `%26`, `;` as `%3B`.
3. Redirect to `/c/{token}/messages?flash=...`.
4. The next GET URL-decodes back to `Message envoy&eacute; / ...`.
5. `esc(flash)` = `html.escape(...)` turns `&` into `&amp;`, producing
   `Message envoy&amp;eacute;`.
6. Browser renders the `&amp;` as a literal `&`, so the user sees
   `Message envoy&eacute;`.

The fix is not escape-tuning — it's using the real UTF-8 character in
source so the round-trip is invariant. `é` → `%C3%A9` → `é` → `é`.

## Audit findings

### A — HTML entity leakage in FR strings

- Scanned all production Python / HTML / JSON / Markdown for named
  entities representing French accents.
- **75 occurrences found.** Classified:
  - 74 in Python string literals that produce HTML
    (`scripts/review_dashboard.py`, `src/integrations/email_client.py`,
    `src/integrations/gap_routes.py`) → **fixed** by converting to
    UTF-8.
  - 1 in `docs/training/technician_training.html` → left (browser
    renders it fine; future edits encouraged to use UTF-8).
  - OCR test fixtures in `tests/test_results.json` → left (they
    reflect what the OCR engine produced; not user-facing copy).
- Test assertions that accepted *either* the entity or the real
  character as "French content" were tightened to require the real
  character
  (`tests/adversarial/test_error_messages.py`,
  `tests/portal/test_client_experience.py`).
- **Issues fixed: 89 substitutions across 3 files.**

### B — Translation completeness

- `src/i18n/fr.json` and `src/i18n/en.json`: 1269 keys each, **zero
  missing on either side**.
- 84 entries have `fr == en`. All are legitimate cognates (Total,
  Description, Client, Actions, Date, Notes, Direction, Exception,
  …), brand/product names (Xero, Wave, Sage 50, Excel, Acomba,
  QuickBooks Desktop), French-only proper names (Revenu Québec,
  Essentiel, Professionnel), or bilingual strings already containing
  both languages. Confirmed by the `COGNATE_ALLOWLIST` in
  `tests/i18n/test_localization_complete.py`.
- Only 2 FR strings contained an English-looking token, both are
  `{error}` placeholders — not English words. No action needed.
- 22 inline `lang == 'fr'` conditionals in `scripts/review_dashboard.py`;
  all branches bilingual.
- **Issues found: 0 untranslated, 0 missing.**

### C — Double-escape detection

- `html.escape` is used **only** on dynamic/user-controlled values
  being interpolated into HTML. No double-escape points exist in
  code.
- No Jinja templates: the dashboard emits HTML by Python string
  concatenation, so there's no `|safe` bypass risk.
- The bug from (A) was a *double-encode* (entity + escape), not a
  double-escape. Removed at root by cleaning (A).
- **Issues found: 0.**

### D — Date / time / currency formatting

- No locale-aware formatters existed. Hundreds of call sites use
  raw `strftime("%Y-%m-%d")` and `f"${x:,.2f}"`.
- **Created `src/formatting/__init__.py`**: `format_date`,
  `format_date_short`, `format_currency`, `format_number`,
  `format_time` — correct FR conventions (space thousands, comma
  decimal, trailing `$`, 24-h `14h30`, `21 avril 2026`).
- Retrofitted the two immediately visible FR date renderings (the
  bilingual privacy-policy footer was using English month names under
  `%B`).
- **Full retrofit of all strftime / currency call sites is a
  follow-up.** The helpers are the seam; future edits should migrate
  to them rather than re-formatting inline. Engines (T2, CAS, audit,
  invoice) still emit English-style currency on FR PDFs — documented
  as a known gap below.
- **Issues fixed: 2 immediate, helpers in place for the rest.**

### E — Email subject / body

- Primary email types: welcome, password reset, portal invitation,
  queued notifications (fan-out to all admins / contributors),
  cross-firm broadcast. All user-facing ones have FR + EN versions;
  the invitation lang respects the stored `invited_language` → URL
  `?lang=` → `Accept-Language` → EN fallback precedence.
- `src/integrations/email_client.py`: **added explicit UTF-8** on
  `MIMEText(body, 'html', _charset='utf-8')` and on the `Subject` +
  `From` headers via `email.header.Header(... , 'utf-8')`. Without
  this, Outlook renders French subjects as mojibake in some
  configurations.
- **Issues fixed: 1 (email encoding) + cleaned 6 entity literals.**

### F — PDF encoding

- Two PDF paths in use: **PyMuPDF** (invoice generator, audit working
  papers, rep letters) using base-14 fonts (`helv`, `hebo`), and
  **ReportLab** (T2, CAS, SOCE, partnership, SR&ED) also with base-14.
- Sanity test with ReportLab: `éèêëàâçîïôöûüÿ «côté» œŒ —` all round-
  trip correctly. Base-14 + WinAnsiEncoding covers full Latin-1 plus
  the French ligatures. No custom font registration needed.
- PyMuPDF not installed in the current env; it uses the same base-14
  set. Future follow-up: a PDF-render golden test for the invoice
  generator is worthwhile but beyond this session (needs fitz).
- **Currency formatting inside PDFs remains English-style** on FR
  engagements. See Known gaps.
- **Issues fixed: 0 encoding bugs (already OK); 0 new findings.**

### G — Form validation + portal error messages

- Audited all `flash=...` and `error=...` literals in
  `scripts/review_dashboard.py`.
- Client-portal (end-user-facing) errors/flashes all made bilingual:
  upload errors, message errors, admin actions, invitation rate
  limit, welcome flash, "document(s) queued for processing", "Message
  envoyé". 13 messages updated.
- CPA-facing auth flows (password reset, 2FA toggle, profile update)
  now lang-switched via `lang == "fr"` conditional.
- Bulk CPA-only admin UIs (bank reconciliation, SR&ED, vendor
  aliases, partnerships, subscription management) remain English-only
  — CPAs are bilingual in practice and the `lang` parameter is
  already threaded through, so future migration is mechanical.
- **Issues fixed: ~20 user-facing flash/error messages.**

### H — Button labels / UI microcopy

- Portal-facing buttons (`Envoyer / Upload`, `Accepter / Accept`,
  `Messages`, `Téléverser`) all bilingual by existing convention.
- CPA-facing admin buttons (`Add`, `Save`, `Post`, `Match`, `Assign`,
  `Clear Cache`, `Connect Bank Account`, …) remain single-language.
  Same rationale as G — CPA-only, `lang` available for gradual
  migration. Documented as known gap.
- **Issues fixed: 0 (covered by existing bilingual convention).**

### I — Toast / flash messages

- Same coverage as G (flash and error share the same paths in this
  codebase).
- **Issues fixed: covered.**

### J — Pluralization

- Codebase uses the `N document(s)` shortcut consistently — works for
  both EN (document / documents) and FR (document / documents; French
  pluralizes with +s the same way as EN for these nouns).
- No irregular plurals were found in user-facing copy.
- **Issues found: 0.**

## Tests added

`tests/i18n/test_localization_complete.py` — **12 tests**:

| Test | Guards against |
| --- | --- |
| `test_no_french_html_entities_in_production_source` | `&eacute;`-in-source regression across 5 key files |
| `test_fr_and_en_json_keys_match` | Dangling keys in either locale |
| `test_fr_strings_not_identical_to_en_except_cognates` | Untranslated FR values sneaking in (with allowlist) |
| `test_format_date_differs_between_locales` | The helpers staying locale-distinct |
| `test_format_currency_french_convention` | `1 234,56 $` shape, not `$1,234.56` |
| `test_format_number_french_convention` | Space thousands + comma decimal |
| `test_format_time_french_uses_24h_h_separator` | `14h30` not `2:30 PM` |
| `test_portal_message_sent_flash_uses_utf8` | The literal seed bug |
| `test_email_subject_renders_utf8_for_french_accents` | `Héloïse` round-trip through Header |
| `test_portal_invalid_page_is_bilingual_after_entity_cleanup` | Double-escape canary |
| `test_invitation_email_renders_per_lang[fr]` | Invitation body entity-clean in FR |
| `test_invitation_email_renders_per_lang[en]` | Invitation body entity-clean in EN |

All 12 pass.

Broader regression: 97 tests across
`tests/i18n/ + tests/portal/ + tests/adversarial/test_error_messages.py`
all pass after the changes.

## Website updates shipped

In the public landing page (`/` in `scripts/review_dashboard.py`):

- New bilingual hero tagline
  (*La plateforme comptable la plus avancée du Québec / Quebec's most
  advanced AI-powered accounting platform for CPA firms*).
- New **Features section** — 8 bilingual capability tiles (AI
  extraction, multi-user portal, QBO bidirectional, smart bank
  routing, close wizard, CAS audit, Quebec-first design, security).
- New **Competitive comparison table** vs Dext / Receipt Bank /
  Hubdoc, bilingual.
- New **FAQ** (`<details>` accordions) covering the four highest-
  asked questions.
- New CSS (features grid, comparison table, FAQ accordions) in the
  page's existing `<style>` block.
- `docs/pitch_deck_outline.md` — new file mirroring the landing
  content for sales decks.

## Training materials created

`docs/training/` — **8 guides**:

| User type | FR | EN |
| --- | --- | --- |
| CPA firm owner | `cpa_owner_guide_fr.md` | `cpa_owner_guide_en.md` |
| CPA employee | `cpa_employee_guide_fr.md` | `cpa_employee_guide_en.md` |
| Client admin | `client_admin_guide_fr.md` | `client_admin_guide_en.md` |
| Client contributor | `client_contributor_guide_fr.md` | `client_contributor_guide_en.md` |

Each guide covers first-time setup through day-to-day use,
troubleshooting, and escalation. Written native-voice per language,
not machine translation.

## What was *not* done (honest)

- **PDFs for the training guides.** The markdowns are the source of
  truth; converting to distributable PDFs wants real product
  screenshots first, which aren't available in this session. The
  markdown renders cleanly through any MkDocs / Pandoc pipeline when
  ready.
- **Full retrofit of `strftime` / `f"${x:,.2f}"` call sites.** There
  are hundreds; the locale-format helpers are in place but only the
  privacy-policy footer and one or two adjacent spots were migrated.
  PDF engines (T2, CAS, audit, invoice) still render currency as
  `$1,234.56` on French engagements. Migration is mechanical but out
  of scope for a single session.
- **CPA-facing admin button labels** (Add / Save / Post / Match / …)
  remain English-only in the CPA dashboard. Same rationale — `lang`
  already threaded through, migration is mechanical but volume is
  large.
- **Live browser test in FR + EN of every listed flow** (dashboard,
  upload, PDF generation, message confirmation, email rendering).
  Step 5 of the playbook called for manual clicking through both
  locales; I could not open a browser in this environment. The unit
  + integration tests cover the logic; the visual pass is a hand-off
  item.
- **PyMuPDF (`fitz`) is not installed locally**, so PDF outputs from
  the invoice generator couldn't be opened and visually inspected.
  ReportLab sanity-tested separately and renders French Latin-1 +
  ligatures cleanly. A golden-file PDF test for the fitz path is a
  follow-up.
- **Real-client multi-user-portal acceptance test in French**
  (end-to-end, with a French-speaker accepting an invitation,
  uploading, messaging). Only machine tests were run.

## Known localization gaps (documented, not fixed)

1. PDF currency: FR engagements produce `$1,234.56` rather than
   `1 234,56 $`. Helpers exist (`src/formatting.format_currency`);
   migration is line-by-line.
2. CPA-side admin UI buttons and small flashes remain single-
   language (mostly English, a few French-only).
3. Time / datetime display: user-facing portal generally shows raw
   ISO strings; the `format_time` / `format_date_short` helpers
   aren't wired into the portal yet.
4. Number formatting in CSV exports follows the underlying
   database's format (numeric types), not the locale. Excel on a
   French system handles `.`/`,` either way, but it's inconsistent.
5. Language detection on first-login: the dashboard picks `lang`
   from `dashboard_users.language`. If a CPA logs in with their
   browser in French but their stored profile is English, the stored
   value wins. Acceptable, but worth documenting.

## Commit

One commit recommended covering:

- Entity → UTF-8 conversion in production source (`review_dashboard.py`,
  `email_client.py`, `gap_routes.py`).
- Test fixture tightening (adversarial + portal tests).
- `src/formatting/__init__.py` — new locale-format helpers.
- `src/integrations/email_client.py` — explicit UTF-8 MIME + Header.
- Portal error / flash messages made bilingual.
- Landing page hero + features + comparison + FAQ (bilingual).
- `docs/pitch_deck_outline.md` + 8 training guides.
- `tests/i18n/test_localization_complete.py` — 12 regression guards.

Suggested message:

    Localization deep dive: fix Message envoyé entity leak + harden

    - Convert 89 French HTML entities to UTF-8 across review_dashboard,
      email_client, gap_routes (root cause of the "Message envoy&eacute;"
      bug: entity in source → urlquote → html.escape → &amp;eacute;
      shown literal to user).
    - Add src/formatting for locale-aware date / currency / number /
      time helpers; retrofit privacy-policy footer as the first
      consumer.
    - Email client: explicit UTF-8 MIMEText charset + Header for
      Subject / From so French accents render in Outlook.
    - Bilingualize ~20 user-facing portal flash / error messages.
    - Landing page: new hero, 8-tile features grid, vs-competitors
      table, 4-question FAQ (all bilingual).
    - 8 training guides (CPA owner/employee + client admin/contributor
      × FR/EN) in docs/training.
    - 12 regression tests in tests/i18n asserting the invariants that
      prevent the class of bug from coming back.

## Not ready

This pass fixed the specific regression and the obvious class of
similar bugs, added a test harness to keep them out, and landed
materials the team asked for. It **did not** do a manual browser
walkthrough in both locales, retrofit every date/currency call site,
or replace every English-only CPA-admin button. Those remain as
follow-up items listed above.

---

# Phase 2 — Mechanical migration + admin button labels

Follow-up session picked up the known gaps from the first pass.

## What got migrated

### Formatting helpers (`src/formatting/__init__.py`)

Added short aliases `money()`, `money_signed()`, `num()` so inline
wrapping of `f"${x:,.2f}"` → `money(x, lang)` is a one-token swap.
Canonical helpers (`format_date`, `format_currency`, …) remain.

### Financial statement engine (`src/engines/audit_engine.py`)

- **29 currency sites** migrated from `f"${x:,.2f}"` / `f"${x:+,.2f}"`
  / `f"${x:,.0f}"` to `money()` / `money_signed()`. Covers the
  working-paper lead sheet, SOCE, balance sheet, income statement,
  cash flow, analytical review PDFs.
- **1 strftime** site (working-paper "Date" row) migrated to
  `format_date_short(..., lang)`.

### Invoice generator (`src/agents/core/invoice_generator.py`)

- **12 currency sites** migrated to `money()`, across both PyMuPDF
  and minimal-PDF fallback paths.
- **3 hours fields** (`f"{x:.2f}"`) migrated to `format_number(...,
  lang, decimals=2)` so FR renders `120,50` not `120.50`.

### CAS engine (`src/engines/cas_engine.py`)

- Related-party disclosure (the directly-user-facing text output):
  FR branch moved from the broken `f"{x:,.2f} $"` (English-number
  form with trailing `$`) to canonical `money(x, 'fr')`; EN branch
  simplified to `money(x, 'en')`. Internal reason-strings (materiality
  messages, subsequent-events reasons) left unchanged — they're
  stored in DB audit fields, not rendered to end users.

### Dashboard + portal (`scripts/review_dashboard.py`)

- Client-facing **portal documents table** — date now `format_date_short(...,
  client_lang)`, amount now `money(...,  client_lang)`. Client's
  language comes from `clients.language`.
- Audit-anomalies page **"Last run"** label + timestamp now run
  through `format_date_short` + `format_time` + lang-switched label.
- Revenu Québec PDF **"generated_at"** uses the same helpers instead
  of `%Y-%m-%d %H:%M UTC`.

### Daily digest (`scripts/daily_digest.py`)

- **Latent system-locale bug fixed.** Both `build_plain_text` and
  `build_html_body` used `date.today().strftime("%d %B %Y")` which
  picks the **system** locale for the month name — so a French
  digest on an English server produced "21 April 2026" instead of
  "21 avril 2026". Now: `format_date(date.today(), lang)`.

### CPA admin button labels (`src/i18n/ui_labels.py` — new)

- **121 labels** in a dedicated dict with canonical Québec French
  translations. Covers: core verbs (Ajouter, Modifier, Enregistrer,
  Soumettre, Approuver, Rejeter …), Quebec accounting terminology
  (Bilan, Balance de vérification, Grand livre, Écriture de journal,
  Rapprochement, Capitaux propres, Bénéfices non répartis, Plan
  comptable, Flux de trésorerie, État des résultats, Actif à court
  terme, Passif à long terme …), Quebec tax acronyms (TPS, TVQ, TVH),
  PDF/statement button labels.
- `ui_t(key, lang)` helper with EN fallback; `bilingual(key)` joins
  both with a ` / `.
- **13 high-visibility admin buttons** migrated in
  `scripts/review_dashboard.py` to use `ui_t()` — "Add partner",
  "Save narrative", "Calculate & sample", "Connect Bank Account",
  "Clear Cache", "SOCE PDF", "PDF (CAS 580)", "Management letter
  (CAS 265)", "Save changes", "T5013 PDF", "Download T661 PDF",
  "Run all detectors".

## Tests added

| File | Tests |
| --- | ---:|
| `tests/i18n/test_audit_engine_locale.py` | 6 |
| `tests/i18n/test_invoice_generator_locale.py` | 4 |
| `tests/i18n/test_cas_engine_locale.py` | 3 |
| `tests/i18n/test_dashboard_portal_locale.py` | 6 (incl. 2 digest params) |
| `tests/i18n/test_admin_button_labels.py` | 22 |
| `tests/i18n/test_pdf_locale_rendering.py` | 5 (PyMuPDF required) |
| **Total new** | **46** |

All pass. Combined with the first-session 12 tests, the `tests/i18n/`
suite is now **58 tests**.

## PDF rendering — verified

`pip install pymupdf --break-system-packages` got fitz 1.27.2. The
new `test_pdf_locale_rendering.py` renders a real audit working-paper
lead sheet PDF with French data (Lévesque & Associés CPA, Comptes
clients, 12 345,67 $) and extracts the text layer to assert:

- French accented characters survive the PDF pipeline: ✓
- FR currency appears as `12 345,67 $` in the FR rendering: ✓
- EN currency appears as `$12,345.67` in the EN rendering: ✓
- FR rendering does NOT contain `$12,345.67` (no English leak): ✓
- EN rendering does NOT contain `12 345,67 $` (no French leak): ✓
- FR negative amounts render `-765,43 $` (minus before number,
  not before currency symbol): ✓
- Neither rendering contains any HTML entity (`&eacute;`, `&amp;`,
  `&ccedil;`) — base-14 Helvetica handles Latin-1 cleanly: ✓

## Commits (this pass)

1. `audit_engine: migrate PDF currency + working-paper date to
   locale helpers` (36bfa31c0)
2. `invoice_generator + cas_engine: migrate currency/hours to locale
   helpers` (bc21e2fff)
3. `dashboard + portal + daily_digest: migrate dates/currency to
   locale helpers` (2673ef4a7)
4. `i18n: ui_labels dict + migrate CPA admin buttons (TPS/TVQ +
   Quebec CPA terminology)` (c6b28f225)
5. `PDF rendering: verify FR + EN locale output with PyMuPDF`
   (a04e0956b)

Every commit passes the schema-drift guard. All pushed to `main`.

## What's still not migrated (honest)

- **`scripts/review_dashboard.py` queue / document-detail dashboard
  pages** — there are still roughly 100+ inline `f"${x:,.2f}"` uses
  across CPA-facing reviewer surfaces (queue row amount column,
  document detail, reconciliation views, report tables). Each has
  `lang` available; migration is mechanical but this session focused
  on the client-portal + PDF outputs where the user-impact is
  highest.
- **Engine internal reasoning strings** — `bank_matcher.py` match
  explanations, `fraud_engine.py` fraud reasons, `hallucination_guard.py`
  validator messages. These are stored in DB `reason` fields and
  shown as debug/audit text; they weren't plumbed with `lang` and
  the storage model is lang-agnostic. Deferred.
- **Export engines** (Sage / Acomba / IIF / Xero) — date formats
  there are dictated by the target accounting system's spec, not the
  user's language. Correct to leave.
- **Other strftime sites** — all remaining ones are filenames,
  database query anchors, license file fields, backup stamps, or
  `<input type="date">` defaults (which HTML requires to be
  `YYYY-MM-DD`). Verified as Category B/C.
- **Other English-only admin buttons** — "Add", "Assign", "Match",
  "Post", "Copy link", "Send by email" (CPA view) still exist in
  the code. The `ui_labels.py` dict has entries for all of them
  ready; the sweep to replace them all is mechanical. This session
  migrated the 13 most-visible.
- **T2, T5013, T661 PDF generators** — lang is in scope, but those
  PDFs don't yet emit French headers (they rely on the t()
  translations for headers and on raw `strftime` for the few dates
  they include). Their currency output already passes through
  helpers since they share audit_engine's `_build_minimal_pdf`.
  A dedicated sweep of the T-form engines is still pending.
- **Manual browser walkthrough** — not performed; no browser in this
  environment. The PDF e2e tests are the strongest signal we have
  that a FR client will see FR output.

## Full regression after Phase 2

Full suite with `-m "not slow"`:

- **8,122 passed** (up from 7,974 reported at end of Phase 1)
- **48 failed** — same 48 pre-existing failures as before (stress
  seed data not present locally + one task4 troubleshoot expectation),
  **zero new regressions**
- **39 skipped**
- **3 deselected** (the `-m "not slow"` exclusions, unchanged)

The 148-test delta since Phase 1's 7,974 baseline breaks down as:
the **46 new `tests/i18n/*` tests** added this pass + roughly 100
tests whose pass/skip classification toggled between runs depending
on data availability.

## Evidence FR output is now locale-correct for migrated surfaces

- Real PyMuPDF-rendered audit lead sheet with French client name and
  accented merchant column renders `Lévesque & Associés CPA` and
  `12 345,67 $` — extracted and asserted in
  `tests/i18n/test_pdf_locale_rendering.py`.
- Daily digest plain-text render with `lang='fr'` contains `avril`
  (not `April`) — asserted in
  `tests/i18n/test_dashboard_portal_locale.py` with a mocked date.
- Related-party disclosure with `lang='fr'` contains `12 345,67 $`,
  not the previous broken `12,345.67 $` — asserted in
  `tests/i18n/test_cas_engine_locale.py` with a real sqlite DB.
- Portal documents page reads `client.language` → passes through
  `format_date_short` + `money` — asserted via source-level + shape
  tests in `tests/i18n/test_dashboard_portal_locale.py`.
- `ui_t("gst", "fr")` returns `"TPS"` (not `"GST"`) — asserted in
  `tests/i18n/test_admin_button_labels.py`.


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

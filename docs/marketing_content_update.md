# Marketing content update — 2026-04-21

## Where the marketing site actually lives

The OtoCPA marketing page is **server-rendered by the Python dashboard**,
not a separate static site or CDN build. Specifically:

- **Source file:** `scripts/review_dashboard.py`
- **Function:** `render_signup_page()`
- **Route:** `GET /signup` (handler at `scripts/review_dashboard.py:18893`)
- **Root `/`:** logged-in users hit the queue; anonymous requests
  redirect to `/login`. `/signup` is the public marketing page.

A mirror of the copy for sales decks is maintained in
`docs/pitch_deck_outline.md` per the note at its top.

## Files modified

| File                                  | Purpose                                                                                      |
|---------------------------------------|----------------------------------------------------------------------------------------------|
| `scripts/review_dashboard.py`         | All landing copy (hero, feature tiles, comparison table, FAQ) for `/signup`.                 |
| `docs/pitch_deck_outline.md`          | Sales-deck mirror kept in sync with the landing copy.                                        |
| `docs/marketing_content_update.md`    | This report.                                                                                 |

## Commits (pushed to `main`)

```
37b072c07 landing: expand AI receipt tile with 90-100% accuracy + named Quebec merchants
2174e8f86 landing: month-end close tile mentions per-line edit and override
b6b31e8ed landing: CAS audit tile cites CAS 320/315/530/580 + working papers
1187518c3 landing: Quebec-first tile specifies Canadian FR + ARC/Revenu Quebec terminology
f10bed365 landing: security tile adds Toronto hosting + PIPEDA + Loi 25
ea5ea0721 landing: add Review Queue Workflow feature tile (FR+EN)
8e8757ffc landing: add Canadian Tax Returns tile (T2/GST-QST/T5013/T661)
324eb7937 landing: add Financial Statements tile (TB/IS/BS/CF/SCE)
294e1496f landing: add Admin Dashboard tile (revenue, productivity, audited impersonation)
56f0ec126 landing: extend competitor table with review queue, tax, FS, admin, compliance rows
6649dd4b8 landing: FAQ adds Canadian-receipts accuracy + Canada/Toronto hosting entries
4dbf4e27b pitch deck outline: mirror expanded landing content (12 capabilities + new FAQ + comparison rows)
```

Schema drift guard (`scripts/guards/check_schema_drift.py`, invoked by
`.git/hooks/pre-commit`) passed on every commit.

## What changed, per file

### `scripts/review_dashboard.py` — `render_signup_page()`

**Expanded 4 existing feature tiles**

- **(A) AI Receipt Processing** — "90%+ accuracy" → "90–100 %
  field-level accuracy, measured on real Canadian receipts"; added the
  named merchant list (Metro, Provigo, Super C, Maxi, Jean Coutu,
  Pharmaprix, Tim Hortons, Petro-Canada, …).
- **(E) Month-End Close Wizard** — added per-line edit-and-override
  language.
- **(G) CAS-Compliant Audit Workflows** — now cites the specific CAS
  numbers (320 materiality, 315 risk, 530 sampling, 580 rep letters) +
  working papers.
- **(K) Quebec-First Design** — now specifies Canadian French (not
  European French) and names ARC / Revenu Québec terminology.
- **(L) Security + Compliance** — adds Toronto as the Canadian
  hosting region; adds PIPEDA and Quebec Law 25 compliance; 7-year
  audit retention retained.

**Added 4 new feature tiles**

- **(F) Review Queue Workflow** — employee/owner/firm-admin review
  and approval; escalation flag; audit trail.
- **(H) Canadian Tax Returns** — T2, GST/QST, T5013, T661 SR&ED.
- **(I) Financial Statements** — TB, IS, BS, CF, SCE from the GL.
- **(J) Admin Dashboard** — revenue metrics, team productivity,
  read-only audited impersonation.

**Extended the competitor comparison table** (from 7 to 13 rows). Also
updated the receipts-accuracy cell from `90 %+` to `90–100 %`. New
rows:

- Review queue with approval
- Declarations T2 / TPS-TVQ / T5013 / T661
- États financiers (TB, IS, BS, CF, SCE)
- Tableau de bord admin
- 2FA + Canadian hosting (Toronto)
- PIPEDA + Loi 25 compliance

**Expanded the FAQ** (from 4 to 6 entries). Added:

- "What accuracy can I expect on Canadian receipts?" → 90–100 %
  field-level, named merchants on the high end.
- "Is my data hosted in Canada?" → Yes, Toronto region; PIPEDA + Law
  25; 7-year audit trail.
- Strengthened the existing "Is it really in French?" entry with the
  Canadian-French vs European-French distinction.

### `docs/pitch_deck_outline.md`

Rewritten so sales talking points match the landing page 1-for-1:

- 12 core capabilities (was 8).
- Comparison table extended to 13 rows.
- FAQ extended with accuracy + Canadian-hosting entries.

## Features now covered in marketing content (A–L)

| Key | Capability                                     | Landing tile | Comparison row | FAQ      |
|-----|------------------------------------------------|--------------|----------------|----------|
| A   | AI receipt processing                          | ✓            | ✓              | ✓ (new)  |
| B   | Multi-user client portal                       | ✓            | ✓              | ✓        |
| C   | Bidirectional QuickBooks sync                  | ✓            | ✓              | —        |
| D   | Smart bank routing                             | ✓            | ✓              | ✓        |
| E   | Month-end close wizard                         | ✓            | ✓              | —        |
| F   | Review queue workflow                          | ✓ (new)      | ✓ (new)        | —        |
| G   | CAS-compliant audit workflows                  | ✓            | ✓              | —        |
| H   | Canadian tax returns (T2/GST-QST/T5013/T661)   | ✓ (new)      | ✓ (new)        | —        |
| I   | Financial statements (TB/IS/BS/CF/SCE)         | ✓ (new)      | ✓ (new)        | —        |
| J   | Admin dashboard                                | ✓ (new)      | ✓ (new)        | —        |
| K   | Quebec-first design                            | ✓            | ✓              | ✓        |
| L   | Security + PIPEDA/Loi 25                       | ✓            | ✓              | ✓ (new)  |

All 12 capabilities are represented in the feature grid. The
comparison table hits every capability. The FAQ only needs to cover
the highest-asked questions, not every feature.

## Features NOT covered in marketing content

None of the 12 requested capabilities is missing.

A few nuances that live in the code/docs but were intentionally kept
out of the marketing copy because they'd make the tiles noisy:

- The exact bank-data vendor (Plaid) appears in the FAQ but not in
  tile headlines — the tile abstracts over "smart routing".
- The list of 36 merchants is truncated with "and more" rather than
  enumerated, to keep the tile scannable.

## Bilingual coverage verification

Automated check run against the rendered HTML of `/signup`:

```
feature tiles checked: 12
tiles missing FR or EN:   []
faqs checked:             6
faqs missing FR or EN:    []
HTML entity accent bugs:  0 (no ``envoy&eacute;`` or similar — all
                             accented letters are real UTF-8 é/è/ç/…)
```

FR copy was written in **Canadian French** (not machine-translated or
European French) — e.g. "États financiers", "Tableau de bord",
"Déclarations fiscales canadiennes", "Flux de file d'examen", "Fin de
mois", ARC + Revenu Québec terminology, LPRPDE rather than GDPR-
style RGPD.

## Deployment pipeline

The marketing page is **rendered live by the running dashboard
process**. There is no separate static-site build and no CDN. The
change reaches otocpa.com when the dashboard that serves otocpa.com
restarts with this commit deployed.

- **Render.com** is the declared runtime in `render.yaml` (Docker
  service named `openclaw`, port 8080). If otocpa.com is served from
  this Render service, the usual Render auto-deploy-on-push behaviour
  will pick up the new commits on `main` without manual action —
  **but I have not confirmed that otocpa.com is bound to this Render
  service**, so the operator should verify.
- **Self-hosted / gateway install:** if the operator runs the dashboard
  directly (e.g. `scripts/review_dashboard.py` under systemd or the
  Openclaw gateway), the new copy goes live only after the process is
  restarted on the production host.

### What the operator should run to ship this

On whichever host serves `/signup` publicly:

```bash
git pull --rebase origin main
# then restart whatever process runs scripts/review_dashboard.py
# (e.g. systemctl restart otocpa-dashboard, or
#  the app's standard restart/deploy flow for this environment).
```

I did **not** trigger a production deploy and I have **not** verified
that the updated copy is now live at https://otocpa.com — the commits
are on `main` at the repo, which is the boundary of what this task
called for.

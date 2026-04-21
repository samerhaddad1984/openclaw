# Marketing Site Fix Report — 2026-04-21

## Root cause of the 404s

The marketing site at `https://otocpa.com` is served by **nginx from
static files in `/var/www/otocpa/`** — a completely separate server
block from `app.otocpa.com` (which reverse-proxies to the Python
dashboard on `127.0.0.1:8787`).

`/var/www/otocpa/index.html` was **not in this repo** and had two
concrete defects:

1. Two CTAs pointed at `href="/contact"` — a path that `otocpa.com`
   does not serve. nginx's `try_files $uri $uri/ =404;` returned
   literal 404 for every click.
2. Every "Demander une démo" / "Request a demo" button was a
   `mailto:sales@otocpa.com?subject=...` link intercepted at runtime
   by a `DOMContentLoaded` JS snippet that rewrote the `href` to
   `#contact` and opened a modal. When that JS failed to bind (ad
   blocker, extension error, early click, CSP), the browser fell back
   to the raw `mailto:` — on any device without a registered mail
   client this produced a "page not found"-style failure.

Result: visitors saw 404 on Contact / Request-a-demo even though no
such page had ever existed.

## What changed

### Version control

- `/var/www/otocpa/index.html` was imported into the repo at
  `marketing/otocpa.com/index.html`.
- Added `marketing/otocpa.com/README.md` with the deploy command
  (`rsync` + `nginx reload`) and the routing contract between
  `otocpa.com` (static) and `app.otocpa.com` (dashboard).

### CTAs

- **11 CTAs** on the marketing page now point directly to
  `https://app.otocpa.com/contact`, which renders a real contact page
  with a form that POSTs to `/api/contact`. No JS or email client
  required.
- The mailto-interception JS block was removed.
- The stray `[ ... ]` wrapper characters around the Diagnostic section
  (leftover from a paste note) were cleaned up.
- A single `mailto:sales@otocpa.com` and `mailto:support@otocpa.com`
  remain in the footer as direct-email backups (intentional, visible).

### Features

- Feature grid expanded from 6 tiles to **12 shipped capabilities**:
  1. AI Receipt Processing / Traitement IA des reçus
  2. Multi-User Client Portal / Portail client multi-utilisateurs
  3. Bidirectional QuickBooks Sync / Synchronisation bidirectionnelle QuickBooks
  4. Smart Bank Routing / Routage bancaire intelligent
  5. Month-End Close Wizard / Assistant de fin de mois
  6. Review Queue Workflow / Flux de file d'examen
  7. CAS Audit Workflows / Audits conformes NCA
  8. Canadian Tax Returns / Déclarations fiscales canadiennes
  9. Financial Statements / États financiers
  10. Admin Dashboard / Tableau de bord admin
  11. Quebec-First Design / Conception axée sur le Québec
  12. Security + Compliance / Sécurité et conformité

### FAQ

- New bilingual FAQ section (6 `<details>` accordions) between Trust
  and Diagnostic:
  - Plaid needed if already on QBO?
  - Multi-user upload?
  - Employee leaves?
  - Canadian-receipt accuracy?
  - Data in Canada (Toronto)?
  - Loi 25 + PIPEDA compliance?

### Bilingual parity

Verified with a simple AST pass over the in-file `T.fr` / `T.en`
translation dictionaries:

```
FR keys: 156   EN keys: 156
keys only in FR:  []
keys only in EN:  []
keys without matching HTML id: []
.feature-card count: 12
<details class="faq-item"> count: 6
```

All feature tiles, all FAQ entries, all nav items, all CTAs have
matched FR and EN strings. Native Canadian French (not European),
ARC + Revenu Québec terminology, LPRPDE (not European RGPD).

## Deployment

Performed:

```bash
sudo rsync -av --exclude=README.md \
    /opt/otocpa/marketing/otocpa.com/ /var/www/otocpa/
sudo nginx -t          # config test: ok
sudo systemctl reload nginx
sudo systemctl restart otocpa.service   # to pick up the /contact handler added to the Python dashboard
```

`systemctl restart otocpa.service` is required because the dashboard
proxy target (`app.otocpa.com`) needed to start serving the new
`/contact` route. Before the restart, `app.otocpa.com/contact`
returned HTTP 303 → `/login`; after, it returns HTTP 200 with the
form.

## Live verification (grep output on fetched HTML)

```
otocpa.com/                                                      HTTP 200
https://app.otocpa.com/contact                                   HTTP 200
https://app.otocpa.com/login                                     HTTP 200
https://app.otocpa.com/signup                                    HTTP 200
https://app.otocpa.com/signup/checkout?plan=starter_monthly      HTTP 303  (expected — Stripe redirect)
https://app.otocpa.com/signup/checkout?plan=pro_monthly          HTTP 303  (expected — Stripe redirect)
https://app.otocpa.com/signup/checkout?plan=business_monthly     HTTP 303  (expected — Stripe redirect)
https://app.otocpa.com/privacy                                   HTTP 200

Live marketing site content:
  app.otocpa.com/contact CTA count:   11   (was: 0 before fix)
  broken /contact local hrefs:         0   (was: 2 before fix — the 404s)
  mailto subject=Demo CTAs:            0   (was: 8 before fix — the JS-intercepted ones)
  feature tiles in grid:              12   (was: 6 before fix)
  FAQ entries:                         6   (was: 0 before fix)
```

## Commits (pushed to `main`)

```
9fdbe3e40 marketing: bring otocpa.com source into version control (+README with deploy cmd)
73df71023 marketing: fix CTA 404s (route to app.otocpa.com/contact; drop mailto JS intercept)
72cdb5927 marketing: update features to 12 shipped capabilities (FR+EN dict parity)
1dc674d43 marketing: add bilingual FAQ section (6 entries; FR+EN dict parity at 156/156)
```

Plus this report committed on top.

## Still rough (honest)

- **Screenshots / demo video:** the marketing site has no product
  screenshots and no demo video. The rewrite only changed copy +
  routing — the visual content is still text-only on the HTML side.
- **Testimonials section:** none (no customers yet to quote; better to
  have no testimonials than fake ones).
- **`?lang=fr|en` query on `/contact`:** CTAs link to
  `https://app.otocpa.com/contact` without a lang param. The contact
  page itself is bilingual on-page (FR + EN both shown), so the
  experience is unchanged either way. A future enhancement could have
  the marketing site's active language propagate to the dashboard via
  `?lang=` so the contact page can emphasize one side.
- **Stray non-semantic HTML tweaks:** features grid CSS keeps the
  original `minmax(300px,1fr)` auto-fit — with 12 tiles this is a
  denser grid than 6, which may look busier on certain breakpoints.
  Layout was not retuned for the larger count.
- **app subdomain == dashboard:** `/contact` lives on the Python
  dashboard. If the dashboard goes down, every CTA on the marketing
  site breaks. Consider a static `/contact` fallback on `otocpa.com`
  itself if single-point-of-failure becomes a concern.

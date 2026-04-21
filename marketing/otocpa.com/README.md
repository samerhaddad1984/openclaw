# otocpa.com — marketing site (source of truth)

This directory is the **source of truth for `otocpa.com`** — the public
marketing site served by nginx from `/var/www/otocpa/`.

It is a single static `index.html` (bilingual FR/EN via a JS `toggleLang()`
and dictionaries at the bottom of the file). No build step.

## How it is served

- nginx config: `/etc/nginx/sites-available/otocpa`
- Served from: `/var/www/otocpa/`
- Server names: `otocpa.com`, `www.otocpa.com` (both redirect `:80` → `:443`)
- `try_files $uri $uri/ =404;` — anything not present as a file returns **404**,
  so all internal CTAs must point at the subdomain `https://app.otocpa.com/...`
  (which proxies to the Python dashboard on `127.0.0.1:8787`).

`app.otocpa.com` (the dashboard) is a **separate** server block. Nothing here
touches the dashboard — it's served from `scripts/review_dashboard.py` in
this repo.

## Edit → deploy

1. Edit `index.html` in this directory. Commit.
2. Deploy to the live site:

   ```bash
   sudo rsync -av --delete \
       /opt/otocpa/marketing/otocpa.com/ /var/www/otocpa/
   sudo systemctl reload nginx
   ```

   `--delete` keeps `/var/www/otocpa/` in exact sync with this dir — remove
   it if you have backup files in `/var/www/otocpa/` that should be kept.
3. Verify:

   ```bash
   curl -sSf https://otocpa.com/ | head -c 200
   curl -sS -o /dev/null -w "%{http_code}\n" https://otocpa.com/contact
   #   expected: 404 from nginx — /contact is served by app.otocpa.com,
   #   not otocpa.com. All page CTAs point to https://app.otocpa.com/contact.
   curl -sS -o /dev/null -w "%{http_code}\n" https://app.otocpa.com/contact
   #   expected: 200
   ```

## CTA routing

All "Contact us" / "Request a demo" / "Demander une démo" CTAs on this
page link to **`https://app.otocpa.com/contact`** (a real page rendered by
the Python dashboard with a form that POSTs to `/api/contact`). The mailto:
intercept pattern is history — it silently 404-looked if the visitor had no
default mail client.

A single `mailto:sales@otocpa.com` remains in the footer as a manual
fallback for people who want direct email.

## Backups

`/var/www/otocpa/` may contain `index.html.bak-YYYYMMDD-HHMMSS` files from
prior manual edits. Those are **not** tracked here — if you want a backup
before deploy, the repo already gives you that via git history.

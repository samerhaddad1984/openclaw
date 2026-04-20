# OtoCPA Admin Runbook — 2026-04-20

Operational reference for the dashboard owner. Each section answers
one question.

## How do I add a new user to an existing firm?

From the dashboard, log in as an `owner` or `firm_admin`, navigate
to **Users**, and submit `/users/add` with:

- `username` (email recommended)
- `password` (≥ 10 chars, letter + digit)
- `role`: `employee` / `manager` / `firm_admin` (owners cannot be
  created by non-owner roles — see R5 state-machine tests)
- `display_name` (optional)

Or directly via the DB:

```sql
INSERT INTO dashboard_users
    (username, password_hash, role, firm_code, active, language,
     must_reset_password, created_at)
VALUES (
    'new.user@example.com',
    <bcrypt hash from rd.hash_password(...)>,
    'employee',
    '<FIRM CODE>',
    1, 'fr', 0, datetime('now')
);
```

## How do I reset a user's password manually?

Two options:

**Via the UI:** /forgot. Takes the email, sends a 72-hour signed
link to the address on file.

**Directly (emergency):**

```python
import scripts.review_dashboard as rd
with rd.open_db() as c:
    c.execute(
        "UPDATE dashboard_users SET password_hash=?, must_reset_password=1 "
        "WHERE username=?",
        (rd.hash_password("TempPass123!"), "user@example.com"),
    )
    c.commit()
```

Tell the user the temporary password out of band. `must_reset_password=1`
forces the rotate-on-login flow.

## How do I restore from backup?

Backups live at `/opt/backups/otocpa/`:
- `sqlite_YYYYMMDD_HHMMSS.db` — the SQLite copy
- `postgres_YYYYMMDD_HHMMSS.sql` — the Postgres dump

Run `scripts/backup_db.sh` manually to create a fresh backup before
restoring.

### SQLite

```bash
sudo systemctl stop otocpa    # or: pkill -f review_dashboard
cp /opt/otocpa/data/otocpa_agent.db /tmp/otocpa_agent.db.pre-restore
cp /opt/backups/otocpa/sqlite_<DATE>.db /opt/otocpa/data/otocpa_agent.db
sqlite3 /opt/otocpa/data/otocpa_agent.db 'PRAGMA integrity_check;'
# Must print "ok". Then:
sudo systemctl start otocpa
```

### PostgreSQL

```bash
PGPASSFILE=/opt/otocpa/.pgpass \
  pg_restore -U otocpa -h localhost -d otocpa_prod \
  /opt/backups/otocpa/postgres_<DATE>.sql
```

See `docs/nasty_detective_r2_report.md` for the R2 fix that closed
the silent-backup-failure bug.

## How do I rotate a firm's ingest API key?

From a Python shell:

```python
import scripts.review_dashboard as rd
new_key = rd._rotate_firm_ingest_key("FIRM_CODE")
print(new_key)
```

The new key is returned once. Update the OpenClaw gateway to send
it as `X-API-Key` on every `POST /ingest/openclaw`. The old key is
no longer valid (R4 closed this gap).

## How do I diagnose "database is locked" errors?

SQLite is in WAL mode (R1 hardening) with `busy_timeout=5s`. Locks
happen when:

1. A long-running `BEGIN IMMEDIATE` transaction holds the write lock.
2. A dashboard worker crashed while holding the lock (rare).
3. A file-system issue (disk full, permissions).

Diagnostic:

```bash
sqlite3 /opt/otocpa/data/otocpa_agent.db 'PRAGMA journal_mode;'
sqlite3 /opt/otocpa/data/otocpa_agent.db 'PRAGMA wal_checkpoint(PASSIVE);'
lsof /opt/otocpa/data/otocpa_agent.db
```

If a stuck reader is identified, restart the dashboard process.

## How do I check the audit trail?

Three tables carry auditable events:

- `login_attempts` — every login attempt, success + failure, IP
- `client_portal_access` — every portal page view per client
- `gl_transactions` — every posted journal-entry leg

```bash
sqlite3 /opt/otocpa/data/otocpa_agent.db <<'EOF'
SELECT attempted_at, username, ip_address, success
FROM login_attempts
ORDER BY attempted_at DESC LIMIT 50;
EOF
```

## How do I deploy a code update?

1. `git pull` (or clone from the repo at `origin`).
2. `pnpm install` + `python3 -m pip install -r requirements.txt` if
   dependencies changed.
3. Run bootstrap explicitly in case migrations changed:
   ```python
   import scripts.review_dashboard as rd
   rd.bootstrap_schema()
   ```
4. Restart the dashboard.
5. Run the schema-drift guard to confirm no new drift:
   ```bash
   python3 -m scripts.guards.check_schema_drift
   ```

## What monitoring should I set up?

Not provided by the product today (documented gap, see
`docs/compliance_posture.md`). Minimum recommended:

- Cron-based health check: `curl -f http://localhost:8787/health`
  every 5 minutes. Alert if non-200 or > 2s latency.
- Disk usage: `/opt/backups/otocpa` and `/opt/otocpa/data/` should
  have > 20% free.
- Backup freshness: alert if newest sqlite_*.db > 25 hours old
  (R2 fix caught a 4-day silent failure).

## Where are my logs?

- Dashboard stdout/stderr: wherever the service is run (systemd
  journal, tmux pane, etc.).
- Backup cron output: `/opt/otocpa/logs/backup.log` (R2 fix created
  this directory).
- Daily detector cron: `/opt/otocpa/data/daily_detectors.log`.
- SQLite audit trail: in the DB tables listed above.

## What should I NOT do?

- Do not edit `manual_journal_entries.amount` directly after a JE
  is posted — the GL rows are the source of truth, and R2's audit-
  trail regression test asserts reposting doesn't propagate tampered
  values.
- Do not rotate `/opt/otocpa/.pgpass` without updating the systemd
  unit / environment that sources `OTOCPA_PG_PASSWORD`.
- Do not `pip install --upgrade` major library versions without
  reviewing `docs/nasty_detective_r5_report.md` — the R5 external-
  API-schema tests pin this surface.

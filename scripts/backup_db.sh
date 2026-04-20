#!/bin/bash
# OtoCPA backup script.
#
# Runs daily at 3am via /etc/crontab. Defensive about its own log
# directory — the cron entry redirects stdout to
# /opt/otocpa/logs/backup.log, and if that directory doesn't exist the
# whole shell command aborts BEFORE this script even runs. We also
# create the dir here so a manual run leaves a trail.
#
# Postgres credentials resolution order:
#   1. PGPASSFILE env var points to a .pgpass file (libpq standard).
#   2. OTOCPA_PG_PASSWORD env var (set by systemd unit or /etc/default).
#   3. ~/.pgpass (libpq standard fallback, if 0600 perms).
# No hardcoded password — if every source fails, pg_dump prompts (cron
# would hang); we prefer that loud failure over a silent literal.
set -e
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/opt/backups/otocpa
LOG_DIR=/opt/otocpa/logs
mkdir -p "$BACKUP_DIR" "$LOG_DIR"

PG_OUT="$BACKUP_DIR/postgres_$DATE.sql"
SQLITE_OUT="$BACKUP_DIR/sqlite_$DATE.db"
SQLITE_SRC=/opt/otocpa/data/otocpa_agent.db

# Postgres dump. Credentials: prefer a 0600 .pgpass file.
if command -v pg_dump >/dev/null 2>&1; then
    # Point libpq at /opt/otocpa/.pgpass if present and readable only by us.
    default_pgpass=/opt/otocpa/.pgpass
    if [ -z "${PGPASSFILE:-}" ] && [ -f "$default_pgpass" ]; then
        # libpq REQUIRES mode 0600 on the passfile.
        perms=$(stat -c "%a" "$default_pgpass" 2>/dev/null || echo "")
        if [ "$perms" = "600" ] || [ "$perms" = "400" ]; then
            export PGPASSFILE="$default_pgpass"
        else
            echo "[backup] WARN: $default_pgpass exists but perms are $perms (need 600); ignoring" >&2
        fi
    fi
    # Env-var fallback for installs that manage secrets via systemd /
    # /etc/default rather than a file.
    if [ -z "${PGPASSFILE:-}" ] && [ -n "${OTOCPA_PG_PASSWORD:-}" ]; then
        export PGPASSWORD="$OTOCPA_PG_PASSWORD"
    fi
    if [ -z "${PGPASSFILE:-}" ] && [ -z "${PGPASSWORD:-}" ]; then
        echo "[backup] WARN: no PGPASSFILE / PGPASSWORD / OTOCPA_PG_PASSWORD — pg_dump may prompt or fail" >&2
    fi
    if ! pg_dump -U otocpa -h localhost otocpa_prod > "$PG_OUT"; then
        echo "[backup] FAIL: pg_dump returned non-zero" >&2
        # Don't exit — still try sqlite backup.
    fi
    # Refuse to keep an empty Postgres dump silently.
    if [ ! -s "$PG_OUT" ]; then
        echo "[backup] WARN: pg_dump produced an empty file at $PG_OUT" >&2
    fi
else
    echo "[backup] pg_dump not available — skipping Postgres backup" >&2
fi

# SQLite copy.
if [ -f "$SQLITE_SRC" ]; then
    cp "$SQLITE_SRC" "$SQLITE_OUT"
    if [ ! -s "$SQLITE_OUT" ]; then
        echo "[backup] FAIL: sqlite backup is empty" >&2
        exit 1
    fi
else
    echo "[backup] WARN: SQLite source $SQLITE_SRC missing — skipping" >&2
fi

# Sanity: SQLite backup must be openable.
if [ -f "$SQLITE_OUT" ] && command -v sqlite3 >/dev/null 2>&1; then
    if ! sqlite3 "$SQLITE_OUT" 'PRAGMA integrity_check' | head -1 | grep -qE '^ok$|"ok"'; then
        echo "[backup] FAIL: SQLite backup failed integrity_check" >&2
        exit 1
    fi
fi

# Keep only last 7 days.
find "$BACKUP_DIR" -name "*.sql" -mtime +7 -delete 2>/dev/null || true
find "$BACKUP_DIR" -name "*.db" -mtime +7 -delete 2>/dev/null || true

echo "[backup] OK: $DATE"

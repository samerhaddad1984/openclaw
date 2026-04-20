#!/bin/bash
# OtoCPA backup script.
#
# Runs daily at 3am via /etc/crontab. Defensive about its own log
# directory — the cron entry redirects stdout to
# /opt/otocpa/logs/backup.log, and if that directory doesn't exist the
# whole shell command aborts BEFORE this script even runs. We also
# create the dir here so a manual run leaves a trail.
set -e
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/opt/backups/otocpa
LOG_DIR=/opt/otocpa/logs
mkdir -p "$BACKUP_DIR" "$LOG_DIR"

PG_OUT="$BACKUP_DIR/postgres_$DATE.sql"
SQLITE_OUT="$BACKUP_DIR/sqlite_$DATE.db"
SQLITE_SRC=/opt/otocpa/data/otocpa_agent.db

# Postgres dump.
if command -v pg_dump >/dev/null 2>&1; then
    if ! PGPASSWORD="${OTOCPA_PG_PASSWORD:-OtoCPA2026!Secure}" \
        pg_dump -U otocpa -h localhost otocpa_prod > "$PG_OUT"; then
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

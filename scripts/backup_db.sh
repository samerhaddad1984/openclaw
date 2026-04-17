#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/opt/backups/otocpa
mkdir -p $BACKUP_DIR

# PostgreSQL backup with password
PGPASSWORD="OtoCPA2026!Secure" pg_dump -U otocpa -h localhost otocpa_prod > $BACKUP_DIR/postgres_$DATE.sql

# SQLite backup
cp /opt/otocpa/data/otocpa_agent.db $BACKUP_DIR/sqlite_$DATE.db

# Keep only last 7 days
find $BACKUP_DIR -name "*.sql" -mtime +7 -delete
find $BACKUP_DIR -name "*.db" -mtime +7 -delete

echo "Backup completed: $DATE"

#!/bin/bash
# MetaLayer Backup Script for Production
# Automated backup solution for PostgreSQL and Airflow logs

set -euo pipefail

# Configuration
BACKUP_DIR="/opt/backups"
POSTGRES_CONTAINER="metalayer_postgres"
RETENTION_DAYS=30
DATE=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/opt/backups/backup_${DATE}.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

# Error handling
error_exit() {
    log "${RED}ERROR: $1${NC}"
    exit 1
}

# Create backup directory
mkdir -p "$BACKUP_DIR"

log "${GREEN}Starting MetaLayer backup process...${NC}"

# 1. PostgreSQL Database Backup
log "${YELLOW}Creating PostgreSQL backup...${NC}"
docker exec "$POSTGRES_CONTAINER" pg_dumpall -U postgres > "$BACKUP_DIR/postgres_full_${DATE}.sql" || error_exit "PostgreSQL backup failed"

# 2. Individual Database Backups
log "${YELLOW}Creating individual database backups...${NC}"
for db in airflow data_warehouse; do
    docker exec "$POSTGRES_CONTAINER" pg_dump -U postgres "$db" > "$BACKUP_DIR/postgres_${db}_${DATE}.sql" || log "${RED}Warning: Failed to backup $db${NC}"
done

# 3. Airflow Configuration Backup
log "${YELLOW}Backing up Airflow configuration...${NC}"
tar -czf "$BACKUP_DIR/airflow_config_${DATE}.tar.gz" \
    dags/ include/ plugins/ airflow_settings.yaml docker-compose*.yml .env* || error_exit "Airflow config backup failed"

# 4. Logs Backup
log "${YELLOW}Backing up logs...${NC}"
if [ -d "logs" ]; then
    tar -czf "$BACKUP_DIR/airflow_logs_${DATE}.tar.gz" logs/ || log "${RED}Warning: Logs backup failed${NC}"
fi

# 5. Data Backup (if exists)
log "${YELLOW}Backing up data files...${NC}"
if [ -d "data" ]; then
    tar -czf "$BACKUP_DIR/data_files_${DATE}.tar.gz" data/ || log "${RED}Warning: Data files backup failed${NC}"
fi

# 6. Cleanup old backups
log "${YELLOW}Cleaning up old backups (older than ${RETENTION_DAYS} days)...${NC}"
find "$BACKUP_DIR" -name "*.sql" -mtime +${RETENTION_DAYS} -delete || log "${RED}Warning: Cleanup of SQL backups failed${NC}"
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +${RETENTION_DAYS} -delete || log "${RED}Warning: Cleanup of archive backups failed${NC}"
find "$BACKUP_DIR" -name "*.log" -mtime +${RETENTION_DAYS} -delete || log "${RED}Warning: Cleanup of log files failed${NC}"

# 7. Backup verification
log "${YELLOW}Verifying backups...${NC}"
for file in "$BACKUP_DIR"/*_${DATE}.*; do
    if [ -f "$file" ]; then
        size=$(du -h "$file" | cut -f1)
        log "${GREEN}✓ $file ($size)${NC}"
    fi
done

# 8. Generate backup report
log "${YELLOW}Generating backup report...${NC}"
cat > "$BACKUP_DIR/backup_report_${DATE}.txt" << EOF
MetaLayer Backup Report
======================
Date: $(date)
Backup ID: ${DATE}

Files Created:
$(ls -la "$BACKUP_DIR"/*_${DATE}.* 2>/dev/null || echo "No backup files found")

Disk Usage:
$(du -sh "$BACKUP_DIR")

Database Status:
$(docker exec "$POSTGRES_CONTAINER" psql -U postgres -c "SELECT datname, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database WHERE datistemplate = false;" 2>/dev/null || echo "Database status unavailable")

Container Status:
$(docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}")
EOF

log "${GREEN}Backup completed successfully!${NC}"
log "${GREEN}Backup report: $BACKUP_DIR/backup_report_${DATE}.txt${NC}"

# Optional: Send notification (uncomment if needed)
# echo "MetaLayer backup completed successfully at $(date)" | mail -s "Backup Success" admin@company.com
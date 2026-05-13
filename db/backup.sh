#!/usr/bin/env bash
# IBKR Data Database Backup Script
# Usage: ./backup.sh [--cron]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_DIR}/.env"

BACKUP_DIR="${BACKUP_DIR:-${PROJECT_DIR}/backups}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
LOG_FILE="${BACKUP_DIR}/backup.log"

DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="ibkrdata"
DB_USER="ibkr"
DB_PASS="password"

_load_env() {
  [ -f "$ENV_FILE" ] || return 0
  local url
  url="$(grep '^DB_URL=' "$ENV_FILE" | head -1 | sed 's/^DB_URL=//' | tr -d "'" | tr -d '"')"
  [ -z "$url" ] && return 0
  echo "$url" | grep -q '@' || return 0

  local without_proto="${url#*://}"
  DB_USER="${without_proto%%:*}"
  local rest="${without_proto#*:}"
  DB_PASS="${rest%%@*}"
  rest="${rest#*@}"
  DB_HOST="${rest%%:*}"
  rest="${rest#*:}"
  DB_PORT="${rest%%/*}"
  DB_NAME="${rest#*/}"
}

_log() {
  local msg
  msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg" >> "$LOG_FILE"
  if [ "${1:-}" != "--cron" ]; then
    echo "$msg"
  fi
}

_cleanup_old() {
  local count
  count="$(find "$BACKUP_DIR" -name 'ibkrdata_*.sql.gz' -mtime "+${RETENTION_DAYS}" -type f | wc -l)"
  if [ "$count" -gt 0 ]; then
    find "$BACKUP_DIR" -name 'ibkrdata_*.sql.gz' -mtime "+${RETENTION_DAYS}" -type f -delete
    _log "Cleaned up ${count} old backups (over ${RETENTION_DAYS} days)"
  fi
}

do_backup() {
  local ts
  ts="$(date '+%Y%m%d_%H%M%S')"
  local filename="ibkrdata_${ts}.sql.gz"
  local filepath="${BACKUP_DIR}/${filename}"

  _log "Starting backup: ${DB_NAME}@${DB_HOST}:${DB_PORT} -> ${filename}"

  if pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       --format=custom --compress=9 --file="$filepath" \
       2> >(grep -v 'pg_dump: .*: 循环外键约束\|pg_dump: .*: cyclic foreign keys\|pg_dump: detail:.*hypertable\|pg_dump: detail:.*chunk\|pg_dump: detail:.*continuous_agg\|pg_dump: hint:' >> "$LOG_FILE" || true)
  then
    local size
    size="$(du -h "$filepath" | cut -f1)"
    _log "Backup complete: ${filename} (${size})"
  else
    _log "Backup FAILED: ${DB_NAME}@${DB_HOST}:${DB_PORT}"
    return 1
  fi
}

_load_env
export PGPASSWORD="$DB_PASS"
mkdir -p "$BACKUP_DIR"
_log "$@"
do_backup
_cleanup_old
unset PGPASSWORD

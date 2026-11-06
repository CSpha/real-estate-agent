#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Run a durable, custom-format dump and keep the completed file visible only
# after pg_dump succeeds. The container can be restarted at any point; the
# next run starts a fresh dump and never touches an existing backup.
BACKUP_DIR="${BACKUP_DIR:-/backups}"
INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-86400}"
STATE_FILE="${BACKUP_STATE_FILE:-$BACKUP_DIR/.last_success_epoch}"
DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-${POSTGRES_DB:-realestate}}"
DB_USER="${DB_USER:-${POSTGRES_USER:-realestate}}"
KEEP_DAILY="${BACKUP_KEEP_DAILY:-14}"
KEEP_WEEKLY="${BACKUP_KEEP_WEEKLY:-8}"
OFFSITE_DIR="${BACKUP_OFFSITE_MOUNT:-/offsite}"
RETENTION="python3 /app/scripts/backup_retention.py"

case "$INTERVAL_SECONDS" in
  ''|*[!0-9]*) echo "BACKUP_INTERVAL_SECONDS must be a positive integer" >&2; exit 2 ;;
esac
if (( INTERVAL_SECONDS < 1 )); then
  echo "BACKUP_INTERVAL_SECONDS must be at least 1" >&2
  exit 2
fi

mkdir -p "$BACKUP_DIR"

run_backup() {
  local stamp final_file temp_file
  stamp="$(date -u +%Y%m%dT%H%M%S%NZ)"
  final_file="$BACKUP_DIR/${DB_NAME}_${stamp}.dump"
  temp_file="${final_file}.tmp.$$"

  echo "Starting PostgreSQL backup: ${DB_NAME} (${DB_HOST}:${DB_PORT})"
  if ! PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGUSER="$DB_USER" \
    pg_dump --format=custom --no-owner --file="$temp_file" "$DB_NAME"; then
    rm -f -- "$temp_file"
    return 1
  fi
  mv -- "$temp_file" "$final_file" || return 1
  # Persist the successful completion time atomically for restart-safe cadence.
  local state_temp="${STATE_FILE}.tmp.$$"
  printf '%s\n' "$(date -u +%s)" > "$state_temp" || return 1
  mv -- "$state_temp" "$STATE_FILE" || return 1
  echo "Backup complete: $final_file"
  post_backup "$final_file" || echo "Post-backup steps had problems; backup itself succeeded" >&2
  return 0
}

# Runs only after a successful dump, so a failed backup never prunes anything.
post_backup() {
  local file="$1" name rc=0
  name="$(basename -- "$file")"
  $RETENTION prune "$BACKUP_DIR" --db "$DB_NAME" --newest "$name"     --keep-daily "$KEEP_DAILY" --keep-weekly "$KEEP_WEEKLY" || rc=1
  if [[ "${BACKUP_OFFSITE_ENABLED:-}" != "true" ]]; then
    echo "Off-machine copy skipped: BACKUP_OFFSITE_DIR is not set"
    return $rc
  fi
  if $RETENTION copy "$file" "$OFFSITE_DIR"; then
    $RETENTION prune "$OFFSITE_DIR" --db "$DB_NAME" --newest "$name"       --keep-daily "$KEEP_DAILY" --keep-weekly "$KEEP_WEEKLY"       --verified-file-dir "$BACKUP_DIR" || rc=1
  else
    echo "Off-machine copy FAILED; skipping off-machine retention" >&2
    rc=1
  fi
  return $rc
}

while true; do
  if [[ -f "$STATE_FILE" ]]; then
    last_success="$(<"$STATE_FILE")"
    now="$(date -u +%s)"
    if [[ "$last_success" =~ ^[0-9]+$ ]] && (( now < last_success + INTERVAL_SECONDS )); then
      remaining=$((last_success + INTERVAL_SECONDS - now))
      echo "Waiting ${remaining} seconds until next PostgreSQL backup"
      sleep "$remaining"
    fi
  fi
  if ! run_backup; then
    echo "Backup failed; retrying in 300 seconds" >&2
    sleep 300
    continue
  fi
  echo "Next PostgreSQL backup in ${INTERVAL_SECONDS} seconds"
  sleep "$INTERVAL_SECONDS"
done

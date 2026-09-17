#!/usr/bin/env bash
set -Eeuo pipefail

INTERVAL_SECONDS="${BACKUP_VERIFY_INTERVAL_SECONDS:-604800}"
RETRY_SECONDS="${BACKUP_VERIFY_RETRY_SECONDS:-3600}"

for value in "$INTERVAL_SECONDS" "$RETRY_SECONDS"; do
  case "$value" in
    ''|*[!0-9]*) echo "Backup verification intervals must be positive integers" >&2; exit 2 ;;
  esac
done

while true; do
  if python3 /app/scripts/verify_postgres_backup.py; then
    echo "Backup restore verification complete; next verification in ${INTERVAL_SECONDS} seconds"
    sleep "$INTERVAL_SECONDS"
  else
    echo "Backup restore verification failed; retrying in ${RETRY_SECONDS} seconds" >&2
    sleep "$RETRY_SECONDS"
  fi
done

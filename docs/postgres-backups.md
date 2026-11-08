# PostgreSQL backups

Start the daily worker with `docker compose --profile backup up -d --build backup`.
It uses PostgreSQL 16 tools and writes custom-format dumps to `data/backups/`,
which is ignored by Git. It has no Slack configuration. Completed backups are
published by atomic rename; failed dumps retry after five minutes. The last
completion timestamp is persisted so a restart preserves the daily deadline.
Set `BACKUP_INTERVAL_SECONDS` to change the default 86400-second interval.

## Retention and off-machine copies

After each successful backup the worker prunes old dumps (never after a failed
one): it keeps the newest `BACKUP_KEEP_DAILY` dumps (default 14) and the newest
dump of each of the last `BACKUP_KEEP_WEEKLY` ISO weeks (default 8). The newest
dump and the newest verified dump (recorded in `.last_verified` by
`backup-verify`) are never deleted. Every kept and deleted file is logged.
Only `<database>_<timestamp>.dump` files are considered.

Set `BACKUP_OFFSITE_DIR` in `.env` to a host folder (a synced OneDrive/Dropbox/
Drive folder, or an external or network drive) to also copy each dump there
(temp file, then rename) and apply the same retention. When unset, Compose
mounts an unused placeholder folder (`data/backups-offsite-disabled`) and the
worker logs that the copy was skipped. If the copy fails, the local backup still
succeeds and the failure is logged. After changing these settings run
`docker compose --profile backup up -d --build`.

Monitor disk use. These dumps contain the
application data and should be treated as private. They cover the configured
application database, not other databases or cluster roles.

Verify the latest dump while the application database is quiet:

```powershell
docker compose --profile backup exec -T backup python3 /app/scripts/verify_postgres_backup.py
```

This restores to a uniquely created `_test` database, checks exact table row
counts and index/constraint names against the current source, and removes only
that test database. It never restores over the application database. A source
write since the dump can cause a comparison failure; compare a fresh backup
in a quiet window. This is a restoration and structural smoke test, not a
byte-for-byte data or cluster disaster-recovery test.

Inspect activity with `docker compose --profile backup logs --tail 20 backup`.
Stop it with `docker compose --profile backup stop backup`. Docker and the host
must be running for scheduled backups to execute.

## Verification results — September 11, 2026

The first scheduled backup completed (658,239 bytes). An isolated restore
matched 34 tables, 6,123 rows, 83 user indexes, and 95 constraint names. The test
database was removed; a follow-up query found no remaining restore-test databases.
Recreating the worker preserved its existing deadline instead of taking another
dump.

After Docker restarted on September 15, the worker initially reached Postgres
before it was accepting connections, logged the failure, and retried safely. The
overdue backup then completed (864,538 bytes). Its isolated restore matched 34
tables, 7,656 rows, 83 user indexes, and 95 constraint names, and the temporary
test database was removed. The next backup is due September 16 around 10:18 UTC
/ 6:18 a.m. Eastern, provided Docker remains running.

# Legacy database migration rehearsal

The prototype public schema predates Alembic. Do not stamp it as revision 0001
or run `alembic upgrade head` over its existing tables. Its raw payloads, column
types, constraints, and missing county states differ from the authoritative schema.

## Validated approach

1. Back up the source database with PostgreSQL `pg_dump -Fc --no-owner
   --no-privileges`. Keep the dump in ignored `data/backups/` and verify it by
   restoring it into a new database whose name ends in `_test`.
2. Point the Python process at that restored database. If `DATABASE_URL` is
   configured, it takes precedence over `DB_NAME`; verify the selected database.
   Do not start API, provider, or notification workers against the copy.
3. Run the rehearsal, explicitly choosing the timezone of legacy timestamps:

   ```powershell
   $env:DB_NAME = 'legacy_rehearsal_20260908_test'
   python -m app.utils.rehearse_legacy_migration --legacy-timezone Etc/UTC
   Remove-Item Env:DB_NAME
   ```

   The command checks the actual connected database name and refuses names
   without `_test`. It also refuses unexpected table sets or an existing archive.
   Rerunning requires a fresh restored database rather than silently skipping work.

4. Review the aggregate report and run the application/integration checks below.
   Retain the backup and archive for inspection. Development cutover is a
   separate action; this command deliberately cannot migrate the live database.

The command performs the following in one PostgreSQL transaction:

- Renames the original `public` schema to `legacy_archive`, including all rows,
  sequences, and the old view. Creates a fresh public schema and applies Alembic
  through head rather than stamping an unverified baseline.
- Copies original IDs and column values. Converts naive timestamps using the
  explicitly supplied timezone and alert payload text to JSONB. Invalid JSON,
  nulls violating the new schema, or lossy numeric conversions fail validation
  and roll back the transaction.
- Resolves missing county states only when the original city/county lookup
  identifies exactly one state. Ambiguous or unknown counties fail migration.
- Hashes canonical raw JSON for records with a `source_listing_id`. Duplicate
  listing payloads collapse to their earliest original ID. Rows without listing
  identity remain archive-only, with explicit counts; all original raw text
  remains in the archive, including duplicates.
- Preserves provider sync state using its existing schema, currently managed by
  `app.providers.sync`. Does not fabricate comparable sales from county payloads.
- Sets copied current/history listings to `alert_eligible = false` for rehearsal.
  No alerts or searches are queued. Normal ingestion can subsequently change
  eligibility, so use `--skip-alerts --skip-search-evaluation` during validation.
- Compares counts and every original non-raw column against the archive, accounts
  for every raw row, and advances identity sequences past the copied IDs.

## September 8, 2026 results

Backup: `data/backups/legacy_20260908.dump` (Git-ignored). Restore succeeded on
PostgreSQL 16. The original database timezone is `Etc/UTC`; this was used to
interpret naive timestamps in the rehearsal. Confirm the historical timezone
before any cutover if older writers used different timestamp conventions.

Retained review database: `legacy_rehearsal_20260908_test`.

| Table | Original/archive rows | Copied rows |
| --- | ---: | ---: |
| listings_raw | 123 | 1 |
| listings_current | 4 | 4 |
| listing_history | 18 | 18 |
| alerts_sent | 4 | 4 |
| county_sales | 6 | 6 |
| city_county_lookup | 13 | 13 |
| listing_market_scores | 3 | 3 |
| provider_sync_state | 1 | 1 |

Raw accounting: one copied listing payload, one duplicate, and 121 records
without listing identity retained only in the archive. No original rows were
deleted. The copy reached `0014_shadow_pipeline_runs`.

Validation completed:

- Full test suite: **166 passed**, including 18 PostgreSQL integration tests
  running in their own temporary schema; three dependency deprecation warnings.
- An invalid null alert payload on a second restored database caused a complete
  rollback: no archive or Alembic table remained, and all 123 raw rows survived.
- Repeat-run and non-test-database guards refused work before mutation.
- `app.shadow.readiness` and `app.transforms.detect_price_changes` ran on the
  migrated copy. Readiness correctly reported zero successful shadow runs.
- Two `app.run_pipeline --skip-alerts --skip-search-evaluation` runs succeeded.
  The first loaded three new raw payloads, updated three current rows, and added
  four snapshots; the second changed none. These validation changes are present
  in the retained copy's public schema. The archive remains the original restore.
- Configuration tests and Compose interpolation verified `DB_*`, `POSTGRES_*`,
  and primary-variable precedence. Changed Python files passed Ruff checks.

## Cutover and rollback plan

### Completed local cutover: September 8, 2026

The local `.env` now selects `POSTGRES_DB=realestate_dev_20260908`. Both the
central engine and the legacy/API engine were verified in fresh processes to
connect to that database. The cutover initially reached
`0014_shadow_pipeline_runs`; the database was upgraded to `0015_ingest_errors`
on September 9.

Only PostgreSQL was running and no other source-database sessions were present.
The eight source tables were locked against writes during the fresh backup,
restore, migration, verification, and configuration switch. The source database
`realestate` was not modified. The fresh migrated copy was renamed from
`legacy_cutover_20260908_test` to `realestate_dev_20260908` after validation;
it was not the earlier rehearsal copy containing pipeline test writes.

Retained local artifacts (Git-ignored):

- `data/backups/legacy_cutover_20260908.dump`: fresh pre-cutover backup.
- `data/backups/cutover_20260908_report.json`: counts and backup SHA-256 checksum.
- `realestate_dev_20260908.legacy_archive`: every original table and row.
- `realestate`: original database for rollback.

Archive contents were compared to the source with row-content digests for all
eight tables before selection. The 121 archive-only raw records are all from
`wayne_county_sales_report`; the two listing payloads are duplicate fixture
records. Timestamp interpretation remains `Etc/UTC`, matching the source server.
API health and four-listing retrieval passed. Fresh-process Alembic, readiness,
and price-detection checks passed. Readiness is false with no shadow observations.
All copied current/history rows remain alert-ineligible and the outbox is empty.
No provider calls, pipeline writes, or Slack delivery occurred during cutover.

For rollback before new writes, stop application writers and change only
`POSTGRES_DB` in `.env` back to `realestate`; restart any long-lived processes so
they reload configuration. This returns to the legacy schema: do not start the
current Compose API's automatic Alembic upgrade against it. If new data has been
written after cutover, reconcile that data first. Keep both databases and backups.
The running PostgreSQL container may still show its original `POSTGRES_DB`
initialization variable; changing it does not rename existing databases. New
application processes use the project configuration verified above.

### Future cutovers

Before development cutover, review archive-only payloads, timestamp assumptions,
and the migration report. Pause all writers, take a fresh backup, and repeat the
rehearsal against a fresh restore so no intervening writes are lost. Prepare a
new development database from that validated copy and explicitly select it in
local configuration. Keep API/alert workers stopped until configuration and
schema checks pass; enabling alerts is a separate decision.

Keep the old database and backup intact. To roll back before new writes, stop
services and restore the previous database selection. After new writes, reconcile
those writes before switching back. Do not use Alembic downgrade to undo this
legacy reconciliation, and do not delete the archive as part of cutover.

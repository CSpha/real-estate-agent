# SQL directory

Database schema changes are managed by Alembic under `migrations/`.

This directory contains standalone analysis, reporting, and data-reconciliation
SQL that does not define application schema. `reconcile_legacy_copy.sql` is used
only by the restored-test-database rehearsal command documented under
`migrations/LEGACY_MIGRATION.md`. `review_ingest_errors.sql` reports aggregate
failure reasons without returning raw provider payloads. The former numbered
schema scripts were removed
because they contained duplicate migration numbers and incompatible table
definitions.

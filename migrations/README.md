# Database migrations

Alembic is the authoritative schema-management path for this project.

Existing prototype databases without an Alembic revision require the
[legacy migration rehearsal](LEGACY_MIGRATION.md) before any development cutover.

Apply all migrations:

```powershell
python -m alembic upgrade head
```

Show the current database revision:

```powershell
python -m alembic current
```

Create a migration after changing the schema:

```powershell
python -m alembic revision -m "describe the change"
```

The original numbered SQL scripts were removed because they contained
conflicting definitions. Do not add parallel schema definitions under `sql/`.

Revision `0015_ingest_errors` adds the idempotent provider-error ledger. The
aggregate review query is in `sql/review_ingest_errors.sql`; schema ownership
remains with Alembic.

Revision `0020_provider_sync_state` takes over `provider_sync_state`, which
`app.providers.sync` used to create at runtime. It keeps existing rows, converts
`updated_at` to UTC `TIMESTAMPTZ`, and adds the columns used to resume a
multi-page sync. Downgrading drops the table. Revision
`0021_listing_feed_presence` adds `listings_current.missing_from_feed_since`.

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

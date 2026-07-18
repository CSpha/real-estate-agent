# SQL directory

Database schema changes are managed by Alembic under `migrations/`.

This directory is reserved for standalone analysis or reporting SQL that does
not define application schema. The former numbered schema scripts were removed
because they contained duplicate migration numbers and incompatible table
definitions.

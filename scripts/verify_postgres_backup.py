#!/usr/bin/env python3
"""Restore the newest custom dump into a throwaway database and verify it."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2 import sql


def env(name: str, fallback: str) -> str:
    return os.environ.get(name) or fallback


def connect(database: str):
    return psycopg2.connect(
        host=env("DB_HOST", "localhost"),
        port=env("DB_PORT", "5432"),
        dbname=database,
        user=env("DB_USER", env("POSTGRES_USER", "realestate")),
        password=os.environ.get("PGPASSWORD")
        or os.environ.get("DB_PASSWORD")
        or os.environ.get("POSTGRES_PASSWORD"),
    )


def snapshot(
    conn: object,
) -> tuple[dict[str, int], set[tuple[str, str]], set[tuple[str, str]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
            """
        )
        counts = {}
        for schema, table in cur.fetchall():
            qualified = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
            cur.execute(qualified)
            counts[f"{schema}.{table}"] = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'i' AND n.nspname NOT LIKE 'pg_%'
              AND n.nspname <> 'information_schema'
            """
        )
        indexes = set(cur.fetchall())
        cur.execute(
            """
            SELECT n.nspname, c.conname
            FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            """
        )
        constraints = set(cur.fetchall())
    return counts, indexes, constraints


def main() -> int:
    backup_dir = Path(os.environ.get("BACKUP_DIR", "./data/backups"))
    source_db = env("DB_NAME", env("POSTGRES_DB", "realestate"))
    dumps = sorted(
        (
            path
            for path in backup_dir.glob("*.dump")
            if path.name.startswith(source_db + "_")
        ),
        key=lambda path: path.stat().st_mtime,
    )
    if not dumps:
        print(f"No PostgreSQL dumps found in {backup_dir}", file=sys.stderr)
        return 2
    dump = dumps[-1]
    test_db = f"restore_verify_{datetime.now(timezone.utc):%Y%m%d%H%M%S}_{uuid.uuid4().hex[:8]}_test"
    admin = None
    restored = None
    created = False
    try:
        admin = connect(env("PGADMIN_DB", "postgres"))
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                    sql.Identifier(test_db)
                )
            )
        created = True
        source = connect(source_db)
        try:
            expected = snapshot(source)
        finally:
            source.close()

        restore_env = os.environ.copy()
        restore_env.update(
            PGHOST=env("DB_HOST", "localhost"),
            PGPORT=env("DB_PORT", "5432"),
            PGUSER=env("DB_USER", env("POSTGRES_USER", "realestate")),
            PGDATABASE=test_db,
        )
        if os.environ.get("DB_PASSWORD") or os.environ.get("POSTGRES_PASSWORD"):
            restore_env["PGPASSWORD"] = os.environ.get("DB_PASSWORD") or os.environ.get(
                "POSTGRES_PASSWORD"
            )
        subprocess.run(
            [
                "pg_restore",
                "--exit-on-error",
                "--no-owner",
                "--dbname",
                test_db,
                str(dump),
            ],
            check=True,
            env=restore_env,
        )
        restored = connect(test_db)
        actual = snapshot(restored)
        if expected != actual:
            raise RuntimeError(
                "restored database metadata or table row counts differ from source"
            )
        print(
            f"Restore verification passed: {dump} -> {test_db}; "
            f"{len(actual[0])} tables, {sum(actual[0].values())} rows, "
            f"{len(actual[1])} indexes, {len(actual[2])} constraint names"
        )
        return 0
    except (
        OSError,
        psycopg2.Error,
        subprocess.CalledProcessError,
        RuntimeError,
    ) as exc:
        print(f"Restore verification failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if restored is not None:
            restored.close()
        if admin is not None and created:
            with admin.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(test_db)
                    )
                )
        if admin is not None:
            admin.close()


if __name__ == "__main__":
    raise SystemExit(main())

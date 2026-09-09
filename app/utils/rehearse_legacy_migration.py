"""Reconcile a restored prototype database; deliberately restricted to *_test."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, inspect, text

from app.db import get_engine


TABLES = (
    "listings_raw",
    "listings_current",
    "listing_history",
    "alerts_sent",
    "county_sales",
    "city_county_lookup",
    "listing_market_scores",
    "provider_sync_state",
)
ARCHIVE = "legacy_archive"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def rehearse(engine: Engine, *, legacy_timezone: str) -> dict:
    """Archive originals, build Alembic tables, copy and verify in one transaction."""
    report = {"legacy_timezone": legacy_timezone, "tables": {}}
    with engine.begin() as conn:
        database = conn.scalar(text("SELECT current_database()"))
        if not database.endswith("_test"):
            raise ValueError("Rehearsal requires a restored database ending in _test")
        inspector = inspect(conn)
        if ARCHIVE in inspector.get_schema_names():
            raise ValueError(
                "Archive already exists; restore into a fresh test database"
            )
        if set(inspector.get_table_names(schema="public")) != set(TABLES):
            raise ValueError(
                "Unexpected legacy tables; inspect schema before proceeding"
            )
        conn.execute(
            text("SELECT set_config('TimeZone', :zone, true)"),
            {"zone": legacy_timezone},
        )
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("ALTER SCHEMA public RENAME TO legacy_archive"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.execute(text("SET LOCAL search_path TO public"))
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.attributes["connection"] = conn
        command.upgrade(config, "head")
        # Provider state is currently managed by app.providers.sync, not Alembic.
        conn.execute(
            text(
                "CREATE TABLE public.provider_sync_state "
                "(LIKE legacy_archive.provider_sync_state INCLUDING ALL)"
            )
        )
        # Replace migration seed rows with the original lookup, preserving IDs.
        conn.execute(text("DELETE FROM public.city_county_lookup"))
        copy_sql = (PROJECT_ROOT / "sql" / "reconcile_legacy_copy.sql").read_text()
        conn.execute(text(copy_sql))

        # Raw county records lack listing identity and stay in the intact archive.
        raw_counts = {"copied": 0, "duplicate_payloads": 0, "archive_only": 0}
        for row in conn.execute(
            text("SELECT * FROM legacy_archive.listings_raw ORDER BY id")
        ).mappings():
            payload = json.loads(row["raw_record_json"])
            listing_id = (
                payload.get("source_listing_id") if isinstance(payload, dict) else None
            )
            if listing_id is None or not str(listing_id).strip():
                raw_counts["archive_only"] += 1
                continue
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            inserted = conn.execute(
                text("""
                INSERT INTO public.listings_raw
                    (id, source, source_listing_id, payload_hash, raw_record_json, ingested_at)
                VALUES (:id, :source, :listing_id, :hash, CAST(:payload AS JSONB), :ingested_at)
                ON CONFLICT (source, source_listing_id, payload_hash) DO NOTHING
            """),
                {
                    "id": row["id"],
                    "source": row["source"],
                    "listing_id": str(listing_id).strip(),
                    "hash": hashlib.sha256(encoded.encode()).hexdigest(),
                    "payload": encoded,
                    "ingested_at": row["ingested_at"],
                },
            ).rowcount
            raw_counts["copied" if inserted else "duplicate_payloads"] += 1
        report["raw_payloads"] = raw_counts

        for table in TABLES:
            before = conn.scalar(text(f'SELECT count(*) FROM legacy_archive."{table}"'))
            after = conn.scalar(text(f'SELECT count(*) FROM public."{table}"'))
            if table != "listings_raw":
                if before != after:
                    raise ValueError(f"Row count mismatch: {table}")
                # Compare all original columns after the intended timestamp/JSON casts.
                columns = inspect(conn).get_columns(table, schema=ARCHIVE)
                names = ", ".join(f'"{c["name"]}"' for c in columns)
                old_names = ", ".join(
                    f'"{c["name"]}"::jsonb'
                    if table == "alerts_sent" and c["name"] == "payload_json"
                    else f'"{c["name"]}"'
                    for c in columns
                )
                changed = conn.scalar(
                    text(
                        f"SELECT count(*) FROM ("
                        f'SELECT {old_names} FROM legacy_archive."{table}" EXCEPT ALL '
                        f'SELECT {names} FROM public."{table}") differences'
                    )
                )
                if changed:
                    raise ValueError(f"Original values changed: {table}")
            report["tables"][table] = {"archived": before, "current": after}
            if table != "provider_sync_state":
                conn.execute(
                    text(
                        f"SELECT setval(pg_get_serial_sequence('public.{table}', 'id'), "
                        f'COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM public."{table}"'
                    )
                )
        if sum(raw_counts.values()) != report["tables"]["listings_raw"]["archived"]:
            raise ValueError("Raw payload accounting mismatch")
        report["revision"] = conn.scalar(
            text("SELECT version_num FROM public.alembic_version")
        )
        report["database"] = database
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--legacy-timezone",
        required=True,
        help="Timezone used by the prototype's naive timestamps",
    )
    args = parser.parse_args()
    print("Starting legacy migration rehearsal on a restored test database...")
    report = rehearse(get_engine(), legacy_timezone=args.legacy_timezone)
    print(json.dumps(report, indent=2))
    print("Rehearsal complete; original rows retained in legacy_archive.")


if __name__ == "__main__":
    main()

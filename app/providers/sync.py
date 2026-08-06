from __future__ import annotations

import argparse
from dataclasses import dataclass

from sqlalchemy import Engine, text

from app.ingest.load_sample_listings import ingest_records
from app.providers.base import ProviderAdapter, ProviderSyncResult
from app.providers.fixture import FixtureProvider
from app.providers.rentcast import RentCastProvider
from app.utils.db import get_engine


@dataclass
class ProviderSyncState:
    provider_name: str
    last_status: str
    last_cursor: str | None
    last_error: str | None


def ensure_sync_state_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS provider_sync_state (
                    provider_name TEXT PRIMARY KEY,
                    last_status TEXT NOT NULL,
                    last_cursor TEXT,
                    last_error TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )


def sync_provider(provider: ProviderAdapter, *, engine: Engine | None = None) -> ProviderSyncResult:
    engine = engine or get_engine()
    ensure_sync_state_table(engine)

    cursor = None
    with engine.connect() as conn:
        state_row = conn.execute(
            text("SELECT last_cursor FROM provider_sync_state WHERE provider_name = :provider_name"),
            {"provider_name": provider.name},
        ).fetchone()
        if state_row is not None:
            cursor = state_row.last_cursor

    raw_records, next_cursor = provider.fetch_updated_since(cursor)
    normalized_records = [provider.normalize(record) for record in raw_records]
    counts = ingest_records(normalized_records, engine=engine)

    synced_count = counts["received"]
    updated_count = counts["current_changed"]
    skipped_count = 0 if updated_count else 1 if synced_count else 0

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO provider_sync_state (provider_name, last_status, last_cursor, last_error, updated_at)
                VALUES (:provider_name, :last_status, :last_cursor, :last_error, CURRENT_TIMESTAMP)
                ON CONFLICT (provider_name)
                DO UPDATE SET
                    last_status = EXCLUDED.last_status,
                    last_cursor = EXCLUDED.last_cursor,
                    last_error = EXCLUDED.last_error,
                    updated_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "provider_name": provider.name,
                "last_status": "completed",
                "last_cursor": next_cursor,
                "last_error": None,
            },
        )

    return ProviderSyncResult(
        provider_name=provider.name,
        synced_count=synced_count,
        updated_count=updated_count,
        skipped_count=skipped_count,
        errors=[],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync a provider adapter into the listings tables.")
    parser.add_argument(
        "--provider",
        choices=["fixture", "rentcast"],
        default="fixture",
    )
    parser.add_argument("--city", default="Wooster")
    parser.add_argument("--state", default="OH")
    parser.add_argument("--page-size", type=int, default=100)
    args = parser.parse_args()

    if args.provider == "fixture":
        provider = FixtureProvider()
    elif args.provider == "rentcast":
        provider = RentCastProvider(
            city=args.city,
            state=args.state,
            page_size=args.page_size,
        )
    else:
        raise ValueError(f"Unsupported provider: {args.provider}")

    result = sync_provider(provider)
    print(
        f"Provider sync complete for {result.provider_name}: "
        f"{result.synced_count} synced, {result.updated_count} updated, {result.skipped_count} skipped"
    )

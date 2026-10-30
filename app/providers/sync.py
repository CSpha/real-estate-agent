from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import Engine, text

from app.ingest.load_sample_listings import ingest_records
from app.providers.base import ProviderAdapter, ProviderSyncResult
from app.providers.fixture import FixtureProvider
from app.providers.quarantine import normalize_or_quarantine, record_ingest_errors
from app.providers.rentcast import RentCastProvider
from app.db import get_engine


MAX_PAGES_PER_SYNC = 100
# Offset cursors drift as listings come and go, so only resume recent runs.
RESUME_WINDOW = timedelta(hours=6)

STATE_SELECT_SQL = text(
    """
    SELECT last_status, last_cursor, updated_at
    FROM provider_sync_state
    WHERE provider_name = :provider_name
    """
)
START_SQL = text(
    """
    INSERT INTO provider_sync_state (
        provider_name, last_status, last_cursor, last_error, started_at,
        completed_at, pages_committed, records_committed, updated_at
    )
    VALUES (
        :provider_name, 'in_progress', NULL, NULL, CURRENT_TIMESTAMP,
        NULL, 0, 0, CURRENT_TIMESTAMP
    )
    ON CONFLICT (provider_name)
    DO UPDATE SET
        last_status = 'in_progress',
        last_cursor = NULL,
        last_error = NULL,
        started_at = CURRENT_TIMESTAMP,
        completed_at = NULL,
        pages_committed = 0,
        records_committed = 0,
        updated_at = CURRENT_TIMESTAMP
    """
)
RESUME_SQL = text(
    """
    UPDATE provider_sync_state
    SET last_status = 'in_progress',
        last_error = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE provider_name = :provider_name
    """
)
PAGE_COMMITTED_SQL = text(
    """
    UPDATE provider_sync_state
    SET last_cursor = :last_cursor,
        pages_committed = pages_committed + 1,
        records_committed = records_committed + :records,
        updated_at = CURRENT_TIMESTAMP
    WHERE provider_name = :provider_name
    """
)
COMPLETE_SQL = text(
    """
    UPDATE provider_sync_state
    SET last_status = 'completed',
        last_cursor = NULL,
        completed_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE provider_name = :provider_name
    """
)
FAIL_SQL = text(
    """
    UPDATE provider_sync_state
    SET last_status = 'failed',
        last_error = :last_error,
        updated_at = CURRENT_TIMESTAMP
    WHERE provider_name = :provider_name
    """
)


@dataclass
class ProviderSyncState:
    last_status: str
    last_cursor: str | None
    updated_at: datetime | None


def _as_utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def load_sync_state(engine: Engine, provider_name: str) -> ProviderSyncState | None:
    with engine.connect() as conn:
        row = conn.execute(
            STATE_SELECT_SQL, {"provider_name": provider_name}
        ).one_or_none()
    if row is None:
        return None
    return ProviderSyncState(
        last_status=row.last_status,
        last_cursor=row.last_cursor,
        updated_at=_as_utc(row.updated_at),
    )


def resume_cursor(
    state: ProviderSyncState | None,
    *,
    now: datetime,
    resume_window: timedelta = RESUME_WINDOW,
) -> str | None:
    """Return the cursor of an interrupted recent sync, or None to start over."""

    if state is None or state.last_cursor is None:
        return None
    if state.last_status not in {"in_progress", "failed"}:
        return None
    if state.updated_at is None or now - state.updated_at > resume_window:
        return None
    return state.last_cursor


def sync_provider(
    provider: ProviderAdapter,
    *,
    engine: Engine | None = None,
    max_pages: int = MAX_PAGES_PER_SYNC,
    now: datetime | None = None,
) -> ProviderSyncResult:
    """Sync every page, committing each page before saving the next cursor.

    If the process stops between a page's load and its cursor update, the
    resumed run re-loads that page; listing upserts make that harmless.
    """
    engine = engine or get_engine()
    now = now or datetime.now(timezone.utc)
    source = getattr(provider, "source_name", provider.name)
    params = {"provider_name": provider.name}

    cursor = resume_cursor(load_sync_state(engine, provider.name), now=now)
    with engine.begin() as conn:
        conn.execute(RESUME_SQL if cursor is not None else START_SQL, params)
    resumed_from = cursor
    if cursor is not None:
        print(f"Resuming {provider.name} sync from cursor {cursor}.")
    else:
        print(f"Starting {provider.name} sync from the first page.")

    synced_count = 0
    updated_count = 0
    quarantined: list[dict] = []
    pages = 0
    seen_cursors: set[str] = set()
    try:
        while True:
            if pages >= max_pages:
                raise RuntimeError(
                    f"{provider.name} exceeded {max_pages} pages in one sync"
                )
            raw_records, next_cursor = provider.fetch_updated_since(cursor)
            records, errors = normalize_or_quarantine(
                raw_records, provider.normalize, source=source
            )
            record_ingest_errors(errors, engine)
            counts = ingest_records(records, engine=engine)
            pages += 1
            synced_count += counts["received"]
            updated_count += counts["current_changed"]
            quarantined.extend(errors)
            with engine.begin() as conn:
                conn.execute(
                    PAGE_COMMITTED_SQL,
                    {**params, "last_cursor": next_cursor, "records": len(records)},
                )
            print(
                f"Page {pages} committed for {provider.name}: "
                f"{len(records)} loaded, {len(errors)} quarantined."
            )
            if next_cursor is None:
                break
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError(
                    f"{provider.name} returned a repeated cursor: {next_cursor}"
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor
    except Exception as exc:
        with engine.begin() as conn:
            conn.execute(
                FAIL_SQL, {**params, "last_error": f"{type(exc).__name__}: {exc}"}
            )
        raise

    with engine.begin() as conn:
        conn.execute(COMPLETE_SQL, params)

    return ProviderSyncResult(
        provider_name=provider.name,
        synced_count=synced_count,
        updated_count=updated_count,
        skipped_count=synced_count - updated_count,
        errors=[
            f"{error['source_record_id'] or error['payload_hash'][:12]}: "
            f"{error['error_code']}"
            for error in quarantined
        ],
        pages=pages,
        quarantined_count=len(quarantined),
        resumed_from=resumed_from,
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
        f"{result.pages} page(s), {result.synced_count} synced, "
        f"{result.updated_count} updated, {result.skipped_count} unchanged, "
        f"{result.quarantined_count} quarantined"
    )

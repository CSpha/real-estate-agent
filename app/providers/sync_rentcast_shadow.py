from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable

from sqlalchemy import Engine, text

from app.ingest.load_sample_listings import ingest_records
from app.market.listing_eligibility import evaluate_listing_eligibility
from app.providers.quarantine import normalize_or_quarantine, record_ingest_errors
from app.providers.rentcast import RentCastProvider
from app.transforms.score_listings_against_market import (
    score_listings_against_market,
)
from app.transforms.snapshot_listings import snapshot_current_listings
from app.db import get_engine


PILOT_CITIES = ("Wooster", "Orrville", "Rittman")
PILOT_STATE = "OH"
PILOT_COUNTY = "Wayne"
PILOT_PAGE_SIZE = 500
MAX_PAGES_PER_CITY = 20
SOURCE = "rentcast"
# A feed that suddenly shrinks to under half the known listings is more likely
# an outage or a query problem than a market event, so leave presence unchanged
# then. Turnover with a normal-sized feed (old listings replaced) still counts.
MIN_FEED_FRACTION = 0.5
MIN_KNOWN_FOR_GUARD = 5

PRESENT_LISTINGS_SQL = text(
    """
    SELECT source_listing_id
    FROM listings_current
    WHERE source = :source
      AND state = :state
      AND lower(city) = ANY(:cities)
      AND missing_from_feed_since IS NULL
    """
)
MARK_MISSING_SQL = text(
    """
    UPDATE listings_current
    SET missing_from_feed_since = CURRENT_TIMESTAMP
    WHERE source = :source
      AND source_listing_id = ANY(:listing_ids)
      AND missing_from_feed_since IS NULL
    """
)
MARK_RETURNED_SQL = text(
    """
    UPDATE listings_current
    SET missing_from_feed_since = NULL
    WHERE source = :source
      AND source_listing_id = ANY(:listing_ids)
      AND missing_from_feed_since IS NOT NULL
    """
)


@dataclass(frozen=True)
class RentCastShadowSyncResult:
    returned_count: int
    county_count: int
    unique_county_count: int
    excluded_count: int
    review_count: int
    scoring_eligible_count: int
    missing_sqft_count: int
    raw_inserted: int
    current_changed: int
    snapshot_inserted: int
    market_scores_changed: int
    alert_eligible: bool = False
    request_count: int = 0
    quarantined_count: int = 0
    newly_missing_count: int = 0
    returned_to_feed_count: int = 0
    presence_check_skipped: str | None = None


@dataclass(frozen=True)
class FeedPresenceResult:
    newly_missing_count: int
    returned_count: int
    skipped_reason: str | None = None


def is_scoring_eligible(record: dict[str, Any]) -> bool:
    return evaluate_listing_eligibility(record).scoring_eligible


def _in_expected_county(raw_listing: dict[str, Any]) -> bool:
    county = str(raw_listing.get("county") or "").strip()
    return county.casefold() == PILOT_COUNTY.casefold()


def _fetch_all(provider: RentCastProvider) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()

    for page_number in range(1, MAX_PAGES_PER_CITY + 1):
        page_records, next_cursor = provider.fetch_updated_since(cursor)
        records.extend(page_records)
        if next_cursor is None:
            return records, page_number
        if next_cursor in seen_cursors:
            raise RuntimeError(
                f"RentCast returned a repeated cursor for {provider.name}"
            )
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    raise RuntimeError(
        f"RentCast exceeded {MAX_PAGES_PER_CITY} pages for {provider.name}"
    )


def mark_feed_presence(
    engine: Engine,
    seen_listing_ids: set[str],
    *,
    cities: tuple[str, ...] = PILOT_CITIES,
    state: str = PILOT_STATE,
) -> FeedPresenceResult:
    """Flag pilot listings absent from a complete sync; clear returning ones.

    Rows and history are kept; only `missing_from_feed_since` changes.
    """
    with engine.begin() as connection:
        present = set(
            connection.execute(
                PRESENT_LISTINGS_SQL,
                {
                    "source": SOURCE,
                    "state": state,
                    "cities": [city.lower() for city in cities],
                },
            ).scalars()
        )
        newly_missing = sorted(present - seen_listing_ids)
        skipped_reason = None
        if not seen_listing_ids:
            skipped_reason = "the feed returned no pilot listings"
        elif (
            len(present) >= MIN_KNOWN_FOR_GUARD
            and len(seen_listing_ids) < len(present) * MIN_FEED_FRACTION
        ):
            skipped_reason = (
                f"the feed returned {len(seen_listing_ids)} listings against "
                f"{len(present)} known, which looks like a feed problem"
            )
        missing_count = 0
        if skipped_reason is None and newly_missing:
            missing_count = connection.execute(
                MARK_MISSING_SQL, {"source": SOURCE, "listing_ids": newly_missing}
            ).rowcount
        returned_count = 0
        if seen_listing_ids:
            returned_count = connection.execute(
                MARK_RETURNED_SQL,
                {"source": SOURCE, "listing_ids": sorted(seen_listing_ids)},
            ).rowcount
    if skipped_reason:
        print(f"Feed presence check skipped: {skipped_reason}.")
    print(
        f"Feed presence: {missing_count} listing(s) newly missing, "
        f"{returned_count} returned to the feed."
    )
    return FeedPresenceResult(
        newly_missing_count=max(missing_count, 0),
        returned_count=max(returned_count, 0),
        skipped_reason=skipped_reason,
    )


def sync_rentcast_shadow(
    *,
    engine: Engine | None = None,
    provider_factory: Callable[[str], RentCastProvider] | None = None,
) -> RentCastShadowSyncResult:
    """Load the Wayne County pilot without making any listing alert-eligible."""

    engine = engine or get_engine()
    factory = provider_factory or (
        lambda city: RentCastProvider(
            city=city,
            state=PILOT_STATE,
            page_size=PILOT_PAGE_SIZE,
        )
    )

    returned_count = 0
    county_count = 0
    request_count = 0
    unique_records: dict[tuple[str, str], dict[str, Any]] = {}
    ingest_errors: list[dict[str, Any]] = []
    for city in PILOT_CITIES:
        provider = factory(city)
        raw_records, pages = _fetch_all(provider)
        request_count += getattr(provider, "request_count", pages)
        returned_count += len(raw_records)
        county_records = [
            raw_record
            for raw_record in raw_records
            if _in_expected_county(raw_record)
        ]
        county_count += len(county_records)
        normalized_records, errors = normalize_or_quarantine(
            county_records, provider.normalize, source=SOURCE
        )
        ingest_errors.extend(errors)
        for normalized in normalized_records:
            key = (
                str(normalized["source"]),
                str(normalized["source_listing_id"]),
            )
            unique_records[key] = normalized

    record_ingest_errors(ingest_errors, engine)
    records = list(unique_records.values())
    decisions = [evaluate_listing_eligibility(record) for record in records]
    excluded_count = sum(not decision.scoring_eligible for decision in decisions)
    review_count = sum(decision.status == "review" for decision in decisions)
    missing_sqft_count = sum(
        is_scoring_eligible(record) and record.get("sqft") is None
        for record in records
    )
    counts = ingest_records(
        records,
        engine=engine,
        alert_eligible=False,
        scoring_eligibility=is_scoring_eligible,
    )
    presence = mark_feed_presence(
        engine, {listing_id for _source, listing_id in unique_records}
    )
    snapshot_inserted = snapshot_current_listings(engine)
    market_scores_changed = score_listings_against_market(engine)

    return RentCastShadowSyncResult(
        returned_count=returned_count,
        county_count=county_count,
        unique_county_count=len(records),
        excluded_count=excluded_count,
        review_count=review_count,
        scoring_eligible_count=counts["scoring_eligible"],
        missing_sqft_count=missing_sqft_count,
        raw_inserted=counts["raw_inserted"],
        current_changed=counts["current_changed"],
        snapshot_inserted=snapshot_inserted,
        market_scores_changed=market_scores_changed,
        request_count=request_count,
        quarantined_count=len(ingest_errors),
        newly_missing_count=presence.newly_missing_count,
        returned_to_feed_count=presence.returned_count,
        presence_check_skipped=presence.skipped_reason,
    )


def main() -> None:
    result = sync_rentcast_shadow()
    print("RentCast shadow sync complete:")
    for key, value in asdict(result).items():
        print(f"  {key}: {value}")
    print("Saved-search evaluation and alert queueing were not run.")


if __name__ == "__main__":
    main()

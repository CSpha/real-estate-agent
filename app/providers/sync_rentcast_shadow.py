from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable

from sqlalchemy import Engine

from app.ingest.load_sample_listings import ingest_records
from app.market.listing_eligibility import evaluate_listing_eligibility
from app.providers.rentcast import RentCastProvider
from app.transforms.score_listings_against_market import (
    score_listings_against_market,
)
from app.transforms.snapshot_listings import snapshot_current_listings
from app.utils.db import get_engine


PILOT_CITIES = ("Wooster", "Orrville", "Rittman")
PILOT_STATE = "OH"
PILOT_COUNTY = "Wayne"
PILOT_PAGE_SIZE = 500
MAX_PAGES_PER_CITY = 20


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


def is_scoring_eligible(record: dict[str, Any]) -> bool:
    return evaluate_listing_eligibility(record).scoring_eligible


def _in_expected_county(raw_listing: dict[str, Any]) -> bool:
    county = str(raw_listing.get("county") or "").strip()
    return county.casefold() == PILOT_COUNTY.casefold()


def _fetch_all(provider: RentCastProvider) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()

    for _ in range(MAX_PAGES_PER_CITY):
        page_records, next_cursor = provider.fetch_updated_since(cursor)
        records.extend(page_records)
        if next_cursor is None:
            return records
        if next_cursor in seen_cursors:
            raise RuntimeError(
                f"RentCast returned a repeated cursor for {provider.name}"
            )
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    raise RuntimeError(
        f"RentCast exceeded {MAX_PAGES_PER_CITY} pages for {provider.name}"
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
    unique_records: dict[tuple[str, str], dict[str, Any]] = {}
    for city in PILOT_CITIES:
        provider = factory(city)
        raw_records = _fetch_all(provider)
        returned_count += len(raw_records)
        for raw_record in raw_records:
            if not _in_expected_county(raw_record):
                continue
            county_count += 1
            normalized = provider.normalize(raw_record)
            key = (
                str(normalized["source"]),
                str(normalized["source_listing_id"]),
            )
            unique_records[key] = normalized

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
    )


def main() -> None:
    result = sync_rentcast_shadow()
    print("RentCast shadow sync complete:")
    for key, value in asdict(result).items():
        print(f"  {key}: {value}")
    print("Saved-search evaluation and alert queueing were not run.")


if __name__ == "__main__":
    main()

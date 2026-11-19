"""Fill listings_current latitude/longitude from stored listings_raw payloads.

For each current listing without coordinates, take the most recent stored
payload that carries a usable latitude/longitude pair (JSON numbers or numeric
strings, in range, not the 0,0 placeholder) and copy it onto the listing.

Idempotent: listings that already have coordinates are never touched, so a
second run updates nothing. It makes no provider requests.

Payloads stored before revision 0023 hold the normalized listing, which did
not include coordinates, so those listings cannot be filled from history; the
next provider sync fills them instead. The summary reports how many remain.

    python -m app.transforms.backfill_listing_coordinates [--dry-run] [--source rentcast]
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from sqlalchemy import Engine, text

from app.db import get_engine

NUMERIC_PATTERN = r"^\s*-?[0-9]+(\.[0-9]+)?\s*$"

# Latest usable pair per listing that still lacks coordinates.
CANDIDATES_CTE = """
    WITH missing AS (
        SELECT source, source_listing_id
        FROM listings_current
        WHERE latitude IS NULL
          AND (CAST(:source AS TEXT) IS NULL OR source = :source)
    ),
    usable AS (
        SELECT
            raw.id,
            raw.source,
            raw.source_listing_id,
            raw.ingested_at,
            ROUND(CAST(raw.raw_record_json ->> 'latitude' AS NUMERIC), 6) AS latitude,
            ROUND(CAST(raw.raw_record_json ->> 'longitude' AS NUMERIC), 6) AS longitude
        FROM listings_raw raw
        JOIN missing
          ON missing.source = raw.source
         AND missing.source_listing_id = raw.source_listing_id
        WHERE raw.raw_record_json ->> 'latitude' ~ :numeric_pattern
          AND raw.raw_record_json ->> 'longitude' ~ :numeric_pattern
    ),
    latest AS (
        SELECT DISTINCT ON (source, source_listing_id)
            source, source_listing_id, latitude, longitude
        FROM usable
        WHERE latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        ORDER BY source, source_listing_id, ingested_at DESC, id DESC
    )
"""

COUNT_SQL = text(
    CANDIDATES_CTE
    + """
    SELECT
        (SELECT COUNT(*) FROM missing) AS missing_count,
        (SELECT COUNT(*) FROM latest) AS fillable_count
    """
)

UPDATE_SQL = text(
    CANDIDATES_CTE
    + """
    UPDATE listings_current listing
    SET latitude = latest.latitude,
        longitude = latest.longitude,
        updated_at = CURRENT_TIMESTAMP
    FROM latest
    WHERE listing.source = latest.source
      AND listing.source_listing_id = latest.source_listing_id
      AND listing.latitude IS NULL
    """
)


@dataclass(frozen=True)
class CoordinateBackfillResult:
    missing_before: int
    fillable: int
    updated: int
    dry_run: bool

    @property
    def still_missing(self) -> int:
        return self.missing_before - self.updated


def backfill_listing_coordinates(
    engine: Engine | None = None,
    *,
    source: str | None = None,
    dry_run: bool = False,
) -> CoordinateBackfillResult:
    engine = engine or get_engine()
    params = {"source": source, "numeric_pattern": NUMERIC_PATTERN}
    scope = f"source {source!r}" if source else "all sources"
    print(f"Step 1: Finding current listings without coordinates ({scope})...")
    with engine.begin() as conn:
        counts = conn.execute(COUNT_SQL, params).one()
        print(
            f"  {counts.missing_count} listing(s) lack coordinates; "
            f"{counts.fillable_count} have a usable pair in stored payloads."
        )
        updated = 0
        if dry_run:
            print("Step 2: Dry run, no listings updated.")
        else:
            print("Step 2: Copying coordinates onto listings_current...")
            updated = max(conn.execute(UPDATE_SQL, params).rowcount, 0)
            print(f"  Updated {updated} listing(s).")

    result = CoordinateBackfillResult(
        missing_before=counts.missing_count,
        fillable=counts.fillable_count,
        updated=updated,
        dry_run=dry_run,
    )
    remaining = result.missing_before - (result.fillable if dry_run else updated)
    print(
        f"Coordinate backfill complete: {remaining} listing(s) "
        f"{'would still lack' if dry_run else 'still lack'} coordinates."
    )
    if remaining:
        print(
            "  Their stored payloads have no coordinates (payloads saved before "
            "revision 0023 never did); the next provider sync fills active ones."
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="Report counts only.")
    parser.add_argument("--source", help="Limit to one listing source, e.g. rentcast.")
    args = parser.parse_args()
    backfill_listing_coordinates(source=args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

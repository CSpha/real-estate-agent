from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from typing import Any, Callable

from sqlalchemy import Engine, text

from app.db import get_engine
from app.transforms.persist_comparable_valuation import (
    persist_comparable_valuation,
)


ELIGIBLE_LISTINGS_SQL = text(
    """
    SELECT source, source_listing_id
    FROM listings_current
    WHERE source = :source
      AND scoring_eligible IS TRUE
      AND alert_eligible IS FALSE
      AND sqft IS NOT NULL
    ORDER BY source_listing_id
    """
)


def backfill_comparable_valuations(
    *,
    source: str = "rentcast",
    as_of_date: date | None = None,
    engine: Engine | None = None,
    persist: Callable[..., dict[str, Any]] = persist_comparable_valuation,
) -> dict[str, Any]:
    """Value each comparable-ready shadow listing; never queues alerts."""

    engine = engine or get_engine()
    with engine.connect() as connection:
        listings = [
            dict(row)
            for row in connection.execute(
                ELIGIBLE_LISTINGS_SQL, {"source": source}
            ).mappings()
        ]

    statuses: Counter[str] = Counter()
    created = 0
    errors: list[dict[str, str]] = []
    for listing in listings:
        try:
            result = persist(
                listing["source"],
                listing["source_listing_id"],
                as_of_date=as_of_date,
                engine=engine,
            )
        except Exception as exc:
            errors.append(
                {
                    "source_listing_id": listing["source_listing_id"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        statuses[result["valuation"]["status"]] += 1
        created += int(result["valuation_created"])

    return {
        "source": source,
        "attempted": len(listings),
        "completed": len(listings) - len(errors),
        "valuation_created": created,
        "status_counts": dict(statuses),
        "error_count": len(errors),
        "errors": errors,
        "alerts_queued": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill comparable valuations for isolated shadow listings."
    )
    parser.add_argument("--source", default="rentcast")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    args = parser.parse_args()
    result = backfill_comparable_valuations(
        source=args.source,
        as_of_date=args.as_of_date,
    )
    print("Comparable valuation backfill complete:")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

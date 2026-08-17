from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from dateutil.relativedelta import relativedelta
from sqlalchemy import Connection, Engine, text

from app.db import get_engine


SELECTION_VERSION = "comparable-selection-v2"


@dataclass(frozen=True)
class SelectionTier:
    key: str
    geography: str
    max_sale_age_months: int
    sqft_tolerance: Decimal
    bed_tolerance: Decimal
    bath_tolerance: Decimal
    lot_tolerance: Decimal | None


SELECTION_TIERS = (
    SelectionTier(
        key="strict_zip",
        geography="zip",
        max_sale_age_months=12,
        sqft_tolerance=Decimal("0.20"),
        bed_tolerance=Decimal("1"),
        bath_tolerance=Decimal("1"),
        lot_tolerance=Decimal("0.50"),
    ),
    SelectionTier(
        key="broad_zip",
        geography="zip",
        max_sale_age_months=18,
        sqft_tolerance=Decimal("0.30"),
        bed_tolerance=Decimal("1"),
        bath_tolerance=Decimal("1.5"),
        lot_tolerance=Decimal("1.00"),
    ),
    SelectionTier(
        key="city_fallback",
        geography="city",
        max_sale_age_months=24,
        sqft_tolerance=Decimal("0.40"),
        bed_tolerance=Decimal("2"),
        bath_tolerance=Decimal("2"),
        lot_tolerance=None,
    ),
)

SUBJECT_SQL = text(
    """
    SELECT
        source,
        source_listing_id,
        address,
        city,
        state,
        zip,
        list_price,
        beds,
        baths,
        sqft,
        lot_size_acres,
        property_type
    FROM listings_current
    WHERE source = :source
      AND source_listing_id = :source_listing_id
    """
)


def _candidate_sql(geography: str) -> Any:
    if geography == "zip":
        geography_clause = "sale.zip = :subject_zip"
    elif geography == "city":
        geography_clause = """
            LOWER(sale.city) = LOWER(:subject_city)
            AND sale.state = :subject_state
        """
    else:
        raise ValueError(f"Unsupported comparable geography: {geography}")

    return text(
        f"""
        SELECT
            sale.source,
            sale.source_sale_id,
            sale.parcel_number,
            sale.sale_date,
            sale.sale_price,
            sale.address,
            sale.city,
            sale.state,
            sale.zip,
            sale.county_name,
            sale.property_type,
            sale.beds,
            sale.baths,
            sale.sqft,
            sale.lot_size_acres,
            sale.year_built,
            sale.latitude,
            sale.longitude,
            COUNT(*) OVER () AS total_candidate_count
        FROM comparable_sales sale
        WHERE sale.arms_length IS TRUE
          AND sale.property_type = :subject_property_type
          AND sale.sale_date BETWEEN :oldest_sale_date AND :as_of_date
          AND {geography_clause}
          AND NOT (
              LOWER(sale.address) = LOWER(:subject_address)
              AND sale.zip = :subject_zip
          )
          AND (
              :subject_sqft IS NULL
              OR sale.sqft BETWEEN
                  :subject_sqft * (1 - :sqft_tolerance)
                  AND :subject_sqft * (1 + :sqft_tolerance)
          )
          AND (
              :subject_beds IS NULL
              OR sale.beds IS NULL
              OR ABS(sale.beds - :subject_beds) <= :bed_tolerance
          )
          AND (
              :subject_baths IS NULL
              OR sale.baths IS NULL
              OR ABS(sale.baths - :subject_baths) <= :bath_tolerance
          )
          AND (
              :lot_tolerance IS NULL
              OR :subject_lot IS NULL
              OR sale.lot_size_acres IS NULL
              OR sale.lot_size_acres BETWEEN
                  GREATEST(0, :subject_lot * (1 - :lot_tolerance))
                  AND :subject_lot * (1 + :lot_tolerance)
          )
        ORDER BY
            CASE
                WHEN :subject_sqft IS NULL THEN 0
                ELSE ABS(sale.sqft - :subject_sqft) / :subject_sqft
            END ASC NULLS LAST,
            CASE
                WHEN :subject_beds IS NULL THEN 0
                ELSE ABS(sale.beds - :subject_beds)
            END ASC NULLS LAST,
            CASE
                WHEN :subject_baths IS NULL THEN 0
                ELSE ABS(sale.baths - :subject_baths)
            END ASC NULLS LAST,
            CASE
                WHEN :lot_tolerance IS NULL OR :subject_lot IS NULL THEN 0
                ELSE ABS(sale.lot_size_acres - :subject_lot)
                    / NULLIF(:subject_lot, 0)
            END ASC NULLS LAST,
            sale.sale_date DESC,
            sale.source,
            sale.source_sale_id
        LIMIT :max_comps
        """
    )


def _decimal_difference(
    comparable_value: Decimal | None,
    subject_value: Decimal | None,
) -> Decimal | None:
    if comparable_value is None or subject_value is None:
        return None
    return abs(comparable_value - subject_value)


def _percent_difference(
    comparable_value: Decimal | None,
    subject_value: Decimal | None,
) -> Decimal | None:
    if (
        comparable_value is None
        or subject_value is None
        or subject_value == 0
    ):
        return None
    return abs(comparable_value - subject_value) / subject_value


def _find_candidates(
    connection: Connection,
    subject: dict[str, Any],
    tier: SelectionTier,
    *,
    as_of_date: date,
    max_comps: int,
) -> tuple[list[dict[str, Any]], int]:
    oldest_sale_date = as_of_date - relativedelta(months=tier.max_sale_age_months)
    rows = connection.execute(
        _candidate_sql(tier.geography),
        {
            "subject_address": subject["address"],
            "subject_city": subject["city"],
            "subject_state": subject["state"],
            "subject_zip": subject["zip"],
            "subject_property_type": subject["property_type"],
            "subject_beds": subject["beds"],
            "subject_baths": subject["baths"],
            "subject_sqft": subject["sqft"],
            "subject_lot": subject["lot_size_acres"],
            "oldest_sale_date": oldest_sale_date,
            "as_of_date": as_of_date,
            "sqft_tolerance": tier.sqft_tolerance,
            "bed_tolerance": tier.bed_tolerance,
            "bath_tolerance": tier.bath_tolerance,
            "lot_tolerance": tier.lot_tolerance,
            "max_comps": max_comps,
        },
    ).mappings()

    candidates = []
    total_candidate_count = 0
    for rank, row in enumerate(rows, start=1):
        candidate = dict(row)
        total_candidate_count = candidate.pop("total_candidate_count")
        candidate["selection_rank"] = rank
        candidate["sale_age_days"] = (as_of_date - candidate["sale_date"]).days
        candidate["sqft_difference_pct"] = _percent_difference(
            candidate["sqft"],
            subject["sqft"],
        )
        candidate["beds_difference"] = _decimal_difference(
            candidate["beds"],
            subject["beds"],
        )
        candidate["baths_difference"] = _decimal_difference(
            candidate["baths"],
            subject["baths"],
        )
        candidate["lot_difference_pct"] = _percent_difference(
            candidate["lot_size_acres"],
            subject["lot_size_acres"],
        )
        candidates.append(candidate)

    return candidates, total_candidate_count


def _tier_contract(tier: SelectionTier) -> dict[str, Any]:
    return asdict(tier)


def select_comparables(
    source: str,
    source_listing_id: str,
    *,
    as_of_date: date | None = None,
    min_comps: int = 3,
    max_comps: int = 5,
    engine: Engine | None = None,
) -> dict[str, Any]:
    source = source.strip()
    source_listing_id = source_listing_id.strip()
    if not source or not source_listing_id:
        raise ValueError("source and source_listing_id cannot be empty")
    if min_comps < 1:
        raise ValueError("min_comps must be at least 1")
    if max_comps < min_comps:
        raise ValueError("max_comps must be greater than or equal to min_comps")

    as_of_date = as_of_date or date.today()
    engine = engine or get_engine()
    with engine.connect() as connection:
        subject_row = connection.execute(
            SUBJECT_SQL,
            {
                "source": source,
                "source_listing_id": source_listing_id,
            },
        ).mappings().one_or_none()
        if subject_row is None:
            raise ValueError(
                f"Listing not found: source={source!r}, "
                f"source_listing_id={source_listing_id!r}"
            )
        subject = dict(subject_row)

        required_fields = ("address", "city", "state", "zip", "property_type")
        missing_required = [field for field in required_fields if not subject[field]]
        if missing_required:
            raise ValueError(
                "Listing is missing required comparable-selection fields: "
                f"{missing_required}"
            )

        criteria_not_applied = [
            field
            for field in ("beds", "baths", "sqft", "lot_size_acres")
            if subject[field] is None
        ]
        criteria_not_applied.append("year_built")

        attempts = []
        selected_candidates: list[dict[str, Any]] = []
        selected_count = 0
        selected_tier = SELECTION_TIERS[-1]
        for tier in SELECTION_TIERS:
            candidates, total_count = _find_candidates(
                connection,
                subject,
                tier,
                as_of_date=as_of_date,
                max_comps=max_comps,
            )
            attempts.append(
                {
                    "tier": tier.key,
                    "candidate_count": total_count,
                }
            )
            selected_candidates = candidates
            selected_count = total_count
            selected_tier = tier
            if total_count >= min_comps:
                break

        for field in ("beds", "baths", "lot_size_acres"):
            if (
                field not in criteria_not_applied
                and selected_candidates
                and all(candidate[field] is None for candidate in selected_candidates)
            ):
                criteria_not_applied.append(field)

    return {
        "selection_version": SELECTION_VERSION,
        "as_of_date": as_of_date,
        "minimum_comps": min_comps,
        "maximum_comps": max_comps,
        "subject": subject,
        "selected_tier": _tier_contract(selected_tier),
        "attempted_tiers": attempts,
        "available_comparable_count": selected_count,
        "returned_comparable_count": len(selected_candidates),
        "meets_minimum": selected_count >= min_comps,
        "criteria_not_applied": criteria_not_applied,
        "comparables": selected_candidates,
    }


def _json_default(value: Any) -> str:
    if isinstance(value, (date, Decimal)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select deterministic comparable sales for a listing."
    )
    parser.add_argument("source", help="Listing source")
    parser.add_argument("source_listing_id", help="Source listing identifier")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--min-comps", type=int, default=3)
    parser.add_argument("--max-comps", type=int, default=5)
    args = parser.parse_args()

    result = select_comparables(
        args.source,
        args.source_listing_id,
        as_of_date=args.as_of_date,
        min_comps=args.min_comps,
        max_comps=args.max_comps,
    )
    print(json.dumps(result, default=_json_default, indent=2))


if __name__ == "__main__":
    main()

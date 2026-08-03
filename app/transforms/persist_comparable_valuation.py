from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Connection, Engine, text

from app.db import get_engine
from app.market.comparable_valuation import (
    MINIMUM_USABLE_COMPS,
    value_listing,
)
from app.market.deal_score_v2 import calculate_comparable_discount_component


VALUATION_INSERT_SQL = text(
    """
    INSERT INTO listing_comparable_valuations (
        source,
        source_listing_id,
        valuation_version,
        selection_version,
        as_of_date,
        input_fingerprint,
        status,
        selected_tier,
        available_comparable_count,
        included_comparable_count,
        weighted_median_price_per_sqft,
        estimated_value,
        estimated_value_low,
        estimated_value_high,
        list_price,
        list_price_discount_amount,
        list_price_discount_pct,
        confidence_score,
        confidence_label,
        calculation_json
    )
    VALUES (
        :source,
        :source_listing_id,
        :valuation_version,
        :selection_version,
        :as_of_date,
        :input_fingerprint,
        :status,
        :selected_tier,
        :available_comparable_count,
        :included_comparable_count,
        :weighted_median_price_per_sqft,
        :estimated_value,
        :estimated_value_low,
        :estimated_value_high,
        :list_price,
        :list_price_discount_amount,
        :list_price_discount_pct,
        :confidence_score,
        :confidence_label,
        CAST(:calculation_json AS JSONB)
    )
    ON CONFLICT (
        source,
        source_listing_id,
        valuation_version,
        as_of_date,
        input_fingerprint
    )
    DO NOTHING
    RETURNING id, created_at
    """
)

VALUATION_SELECT_SQL = text(
    """
    SELECT id, created_at
    FROM listing_comparable_valuations
    WHERE source = :source
      AND source_listing_id = :source_listing_id
      AND valuation_version = :valuation_version
      AND as_of_date = :as_of_date
      AND input_fingerprint = :input_fingerprint
    """
)

COMPONENT_INSERT_SQL = text(
    """
    INSERT INTO deal_score_v2_components (
        valuation_id,
        scoring_version,
        component_key,
        status,
        points,
        max_points,
        reason,
        calculation_json
    )
    VALUES (
        :valuation_id,
        :scoring_version,
        :component_key,
        :status,
        :points,
        :max_points,
        :reason,
        CAST(:calculation_json AS JSONB)
    )
    ON CONFLICT (valuation_id, scoring_version, component_key)
    DO NOTHING
    RETURNING id, created_at
    """
)

COMPONENT_SELECT_SQL = text(
    """
    SELECT id, created_at
    FROM deal_score_v2_components
    WHERE valuation_id = :valuation_id
      AND scoring_version = :scoring_version
      AND component_key = :component_key
    """
)


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        default=_json_default,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _insert_or_find(
    connection: Connection,
    insert_sql: Any,
    select_sql: Any,
    parameters: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    inserted = connection.execute(insert_sql, parameters).mappings().one_or_none()
    if inserted is not None:
        return dict(inserted), True
    existing = connection.execute(select_sql, parameters).mappings().one()
    return dict(existing), False


def persist_comparable_valuation(
    source: str,
    source_listing_id: str,
    *,
    as_of_date: date | None = None,
    min_comps: int = MINIMUM_USABLE_COMPS,
    max_comps: int = 5,
    engine: Engine | None = None,
) -> dict[str, Any]:
    engine = engine or get_engine()
    valuation = value_listing(
        source,
        source_listing_id,
        as_of_date=as_of_date,
        min_comps=min_comps,
        max_comps=max_comps,
        engine=engine,
    )
    calculation_json = _canonical_json(valuation)
    input_fingerprint = hashlib.sha256(
        calculation_json.encode("utf-8")
    ).hexdigest()
    valuation_parameters = {
        "source": valuation["subject"]["source"],
        "source_listing_id": valuation["subject"]["source_listing_id"],
        "valuation_version": valuation["valuation_version"],
        "selection_version": valuation["selection_version"],
        "as_of_date": valuation["as_of_date"],
        "input_fingerprint": input_fingerprint,
        "status": valuation["status"],
        "selected_tier": valuation["selected_tier"],
        "available_comparable_count": valuation["available_comparable_count"],
        "included_comparable_count": valuation["included_comparable_count"],
        "weighted_median_price_per_sqft": valuation[
            "weighted_median_price_per_sqft"
        ],
        "estimated_value": valuation["estimated_value"],
        "estimated_value_low": valuation["estimated_value_low"],
        "estimated_value_high": valuation["estimated_value_high"],
        "list_price": valuation["subject"]["list_price"],
        "list_price_discount_amount": valuation["list_price_discount_amount"],
        "list_price_discount_pct": valuation["list_price_discount_pct"],
        "confidence_score": valuation["confidence_score"],
        "confidence_label": valuation["confidence_label"],
        "calculation_json": calculation_json,
    }
    component = calculate_comparable_discount_component(valuation)
    component_json = _canonical_json(component)

    with engine.begin() as connection:
        valuation_record, valuation_created = _insert_or_find(
            connection,
            VALUATION_INSERT_SQL,
            VALUATION_SELECT_SQL,
            valuation_parameters,
        )
        component_parameters = {
            "valuation_id": valuation_record["id"],
            "scoring_version": component["scoring_version"],
            "component_key": component["component_key"],
            "status": component["status"],
            "points": component["points"],
            "max_points": component["max_points"],
            "reason": component["reason"],
            "calculation_json": component_json,
        }
        component_record, component_created = _insert_or_find(
            connection,
            COMPONENT_INSERT_SQL,
            COMPONENT_SELECT_SQL,
            component_parameters,
        )

    result = {
        "valuation_id": valuation_record["id"],
        "valuation_created": valuation_created,
        "valuation_created_at": valuation_record["created_at"],
        "input_fingerprint": input_fingerprint,
        "component_id": component_record["id"],
        "component_created": component_created,
        "component_created_at": component_record["created_at"],
        "valuation": valuation,
        "deal_score_v2_component": component,
    }
    print(
        "Comparable valuation persistence complete: "
        f"valuation_id={result['valuation_id']} "
        f"({'created' if valuation_created else 'reused'}), "
        f"component_id={result['component_id']} "
        f"({'created' if component_created else 'reused'})."
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Persist a comparable valuation and its Deal Score v2 "
            "discount component."
        )
    )
    parser.add_argument("source", help="Listing source")
    parser.add_argument("source_listing_id", help="Source listing identifier")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--min-comps", type=int, default=MINIMUM_USABLE_COMPS)
    parser.add_argument("--max-comps", type=int, default=5)
    args = parser.parse_args()

    result = persist_comparable_valuation(
        args.source,
        args.source_listing_id,
        as_of_date=args.as_of_date,
        min_comps=args.min_comps,
        max_comps=args.max_comps,
    )
    print(json.dumps(result, default=_json_default, indent=2))


if __name__ == "__main__":
    main()

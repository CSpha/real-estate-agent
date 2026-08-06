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
from app.market.deal_score_v2 import (
    calculate_comparable_discount_component,
    calculate_data_confidence_component,
    calculate_days_on_market_component,
    calculate_deal_score_v2,
    calculate_liquidity_inventory_component,
    calculate_listing_opportunity_component,
    calculate_market_momentum_component,
)


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
        input_fingerprint,
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
        :component_input_fingerprint,
        :status,
        :points,
        :max_points,
        :reason,
        CAST(:calculation_json AS JSONB)
    )
    ON CONFLICT (
        valuation_id,
        scoring_version,
        component_key,
        input_fingerprint
    )
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
      AND input_fingerprint = :component_input_fingerprint
    """
)

PRICE_HISTORY_SQL = text(
    """
    SELECT id, list_price, snapshot_timestamp
    FROM listing_history
    WHERE source = :source
      AND source_listing_id = :source_listing_id
      AND snapshot_timestamp::date <= :as_of_date
    ORDER BY snapshot_timestamp, id
    """
)

LISTING_DOM_SQL = text(
    """
    SELECT days_on_market, snapshot_timestamp
    FROM listing_history
    WHERE source = :source
      AND source_listing_id = :source_listing_id
      AND snapshot_timestamp::date <= :as_of_date
      AND days_on_market IS NOT NULL
    ORDER BY snapshot_timestamp DESC, id DESC
    LIMIT 1
    """
)

COUNTY_DOM_SQL = text(
    """
    SELECT
        lookup.county_name,
        sales.median_days_on_market,
        sales.period_date
    FROM city_county_lookup lookup
    JOIN county_sales sales
      ON LOWER(sales.county_name) = LOWER(lookup.county_name)
     AND UPPER(sales.state) = UPPER(lookup.state)
    WHERE LOWER(lookup.city) = LOWER(:city)
      AND UPPER(lookup.state) = UPPER(:state)
      AND sales.period_date <= :as_of_date
      AND sales.median_days_on_market IS NOT NULL
    ORDER BY sales.period_date DESC, sales.id DESC
    LIMIT 1
    """
)

COUNTY_MARKET_HISTORY_SQL = text(
    """
    SELECT
        lookup.county_name,
        sales.period_date,
        sales.median_sale_price
    FROM city_county_lookup lookup
    JOIN county_sales sales
      ON LOWER(sales.county_name) = LOWER(lookup.county_name)
     AND UPPER(sales.state) = UPPER(lookup.state)
    WHERE LOWER(lookup.city) = LOWER(:city)
      AND UPPER(lookup.state) = UPPER(:state)
      AND sales.period_date <= :as_of_date
      AND sales.median_sale_price > 0
    ORDER BY sales.period_date, sales.id
    """
)

SCORE_INSERT_SQL = text(
    """
    INSERT INTO deal_score_v2_scores (
        valuation_id,
        scoring_version,
        input_fingerprint,
        status,
        total_points,
        available_points,
        available_max_points,
        coverage_pct,
        normalized_available_score,
        reason,
        calculation_json
    )
    VALUES (
        :valuation_id,
        :scoring_version,
        :score_input_fingerprint,
        :status,
        :total_points,
        :available_points,
        :available_max_points,
        :coverage_pct,
        :normalized_available_score,
        :reason,
        CAST(:calculation_json AS JSONB)
    )
    ON CONFLICT (
        valuation_id,
        scoring_version,
        input_fingerprint
    )
    DO NOTHING
    RETURNING id, created_at
    """
)

SCORE_SELECT_SQL = text(
    """
    SELECT id, created_at
    FROM deal_score_v2_scores
    WHERE valuation_id = :valuation_id
      AND scoring_version = :scoring_version
      AND input_fingerprint = :score_input_fingerprint
    """
)

COUNTY_LIQUIDITY_SQL = text(
    """
    SELECT
        lookup.county_name,
        sales.period_date,
        sales.homes_sold,
        sales.new_listings,
        sales.active_listings
    FROM city_county_lookup lookup
    JOIN county_sales sales
      ON LOWER(sales.county_name) = LOWER(lookup.county_name)
     AND UPPER(sales.state) = UPPER(lookup.state)
    WHERE LOWER(lookup.city) = LOWER(:city)
      AND UPPER(lookup.state) = UPPER(:state)
      AND sales.period_date <= :as_of_date
      AND sales.homes_sold > 0
      AND (
          sales.active_listings >= 0
          OR sales.new_listings > 0
      )
    ORDER BY sales.period_date DESC, sales.id DESC
    LIMIT 1
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
    discount_component = calculate_comparable_discount_component(valuation)
    with engine.connect() as connection:
        price_history = [
            dict(row)
            for row in connection.execute(
                PRICE_HISTORY_SQL,
                {
                    "source": valuation["subject"]["source"],
                    "source_listing_id": valuation["subject"][
                        "source_listing_id"
                    ],
                    "as_of_date": valuation["as_of_date"],
                },
            ).mappings()
        ]
        listing_dom = connection.execute(
            LISTING_DOM_SQL,
            {
                "source": valuation["subject"]["source"],
                "source_listing_id": valuation["subject"][
                    "source_listing_id"
                ],
                "as_of_date": valuation["as_of_date"],
            },
        ).mappings().one_or_none()
        county_dom = connection.execute(
            COUNTY_DOM_SQL,
            {
                "city": valuation["subject"]["city"],
                "state": valuation["subject"]["state"],
                "as_of_date": valuation["as_of_date"],
            },
        ).mappings().one_or_none()
        county_market_history = [
            dict(row)
            for row in connection.execute(
                COUNTY_MARKET_HISTORY_SQL,
                {
                    "city": valuation["subject"]["city"],
                    "state": valuation["subject"]["state"],
                    "as_of_date": valuation["as_of_date"],
                },
            ).mappings()
        ]
        county_liquidity = connection.execute(
            COUNTY_LIQUIDITY_SQL,
            {
                "city": valuation["subject"]["city"],
                "state": valuation["subject"]["state"],
                "as_of_date": valuation["as_of_date"],
            },
        ).mappings().one_or_none()
    opportunity_component = calculate_listing_opportunity_component(
        price_history,
        as_of_date=valuation["as_of_date"],
    )
    days_on_market_component = calculate_days_on_market_component(
        subject_days_on_market=(
            listing_dom["days_on_market"] if listing_dom else None
        ),
        listing_snapshot_timestamp=(
            listing_dom["snapshot_timestamp"] if listing_dom else None
        ),
        county_median_days_on_market=(
            county_dom["median_days_on_market"] if county_dom else None
        ),
        county_name=county_dom["county_name"] if county_dom else None,
        market_period_date=county_dom["period_date"] if county_dom else None,
        as_of_date=valuation["as_of_date"],
    )
    market_momentum_component = calculate_market_momentum_component(
        county_market_history,
        as_of_date=valuation["as_of_date"],
    )
    liquidity_inventory_component = calculate_liquidity_inventory_component(
        homes_sold=county_liquidity["homes_sold"] if county_liquidity else None,
        active_listings=(
            county_liquidity["active_listings"] if county_liquidity else None
        ),
        new_listings=(
            county_liquidity["new_listings"] if county_liquidity else None
        ),
        county_name=(
            county_liquidity["county_name"] if county_liquidity else None
        ),
        market_period_date=(
            county_liquidity["period_date"] if county_liquidity else None
        ),
        as_of_date=valuation["as_of_date"],
    )
    substantive_components = [
        discount_component,
        opportunity_component,
        days_on_market_component,
        market_momentum_component,
        liquidity_inventory_component,
    ]
    confidence_component = calculate_data_confidence_component(
        valuation,
        substantive_components,
    )
    components = [*substantive_components, confidence_component]

    with engine.begin() as connection:
        valuation_record, valuation_created = _insert_or_find(
            connection,
            VALUATION_INSERT_SQL,
            VALUATION_SELECT_SQL,
            valuation_parameters,
        )
        persisted_components = []
        for component in components:
            component_json = _canonical_json(component)
            component_input_fingerprint = hashlib.sha256(
                component_json.encode("utf-8")
            ).hexdigest()
            component_parameters = {
                "valuation_id": valuation_record["id"],
                "scoring_version": component["scoring_version"],
                "component_key": component["component_key"],
                "component_input_fingerprint": component_input_fingerprint,
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
            persisted_components.append(
                {
                    "component_id": component_record["id"],
                    "component_created": component_created,
                    "component_created_at": component_record["created_at"],
                    "input_fingerprint": component_input_fingerprint,
                    "component": component,
                }
            )

        deal_score = calculate_deal_score_v2(components)
        deal_score["inputs"]["component_fingerprints"] = {
            item["component"]["component_key"]: item["input_fingerprint"]
            for item in persisted_components
        }
        score_json = _canonical_json(deal_score)
        score_input_fingerprint = hashlib.sha256(
            score_json.encode("utf-8")
        ).hexdigest()
        score_parameters = {
            "valuation_id": valuation_record["id"],
            "scoring_version": deal_score["scoring_version"],
            "score_input_fingerprint": score_input_fingerprint,
            "status": deal_score["status"],
            "total_points": deal_score["total_points"],
            "available_points": deal_score["available_points"],
            "available_max_points": deal_score["available_max_points"],
            "coverage_pct": deal_score["coverage_pct"],
            "normalized_available_score": deal_score[
                "normalized_available_score"
            ],
            "reason": deal_score["reason"],
            "calculation_json": score_json,
        }
        score_record, score_created = _insert_or_find(
            connection,
            SCORE_INSERT_SQL,
            SCORE_SELECT_SQL,
            score_parameters,
        )

    discount_persistence = persisted_components[0]

    result = {
        "valuation_id": valuation_record["id"],
        "valuation_created": valuation_created,
        "valuation_created_at": valuation_record["created_at"],
        "input_fingerprint": input_fingerprint,
        "component_id": discount_persistence["component_id"],
        "component_created": discount_persistence["component_created"],
        "component_created_at": discount_persistence["component_created_at"],
        "valuation": valuation,
        "deal_score_v2_component": discount_component,
        "deal_score_v2_components": persisted_components,
        "deal_score_v2_id": score_record["id"],
        "deal_score_v2_created": score_created,
        "deal_score_v2_created_at": score_record["created_at"],
        "deal_score_v2_input_fingerprint": score_input_fingerprint,
        "deal_score_v2": deal_score,
    }
    print(
        "Comparable valuation persistence complete: "
        f"valuation_id={result['valuation_id']} "
        f"({'created' if valuation_created else 'reused'}), "
        f"{sum(item['component_created'] for item in persisted_components)} "
        f"of {len(persisted_components)} component(s) created; "
        f"aggregate {'created' if score_created else 'reused'}."
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Persist a comparable valuation and its available Deal Score v2 "
            "components."
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

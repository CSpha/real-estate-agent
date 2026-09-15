from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, text

from app.shadow.readiness import assess_shadow_readiness


LISTINGS_SQL = text(
    """
    SELECT
        listing.source, listing.source_listing_id, listing.address, listing.city,
        listing.state, listing.zip, listing.property_type, listing.status,
        listing.list_price, listing.beds, listing.baths, listing.sqft,
        listing.lot_size_acres, listing.days_on_market, listing.price_per_sqft,
        listing.scoring_eligible, listing.alert_eligible,
        market.market_score, market.score_reason AS market_score_reason,
        market.county_median_sale_price, market.pct_below_or_above_median,
        valuation.id AS valuation_id, valuation.as_of_date AS valuation_date,
        valuation.status AS valuation_status, valuation.selected_tier,
        valuation.available_comparable_count, valuation.included_comparable_count,
        valuation.estimated_value, valuation.estimated_value_low,
        valuation.estimated_value_high,
        valuation.list_price_discount_pct AS valuation_discount_pct,
        valuation.confidence_score, valuation.confidence_label,
        score.status AS deal_score_status, score.total_points AS deal_score_total,
        score.available_points, score.available_max_points,
        score.coverage_pct AS deal_score_coverage_pct,
        score.normalized_available_score, score.reason AS deal_score_reason,
        rent.as_of_date AS rent_estimate_date, rent.monthly_rent,
        rent.rent_range_low, rent.rent_range_high,
        rent.comparable_count AS rental_comparable_count,
        rental.analysis_version AS rental_analysis_version,
        rental.gross_yield_pct, rental.monthly_noi, rental.cap_rate_pct,
        rental.monthly_cash_flow, rental.target_dscr,
        rental.underwritten_dscr, rental.interest_rate_pct,
        rental.term_years, rental.max_ltv_pct, rental.max_loan_by_dscr,
        rental.ltv_cap_loan, rental.target_loan_amount,
        rental.target_ltv_pct, rental.down_payment,
        rental.monthly_principal_interest, rental.monthly_pitia,
        rental.minimum_rent_for_target_dscr,
        rental.constraint AS rental_loan_constraint,
        rental.calculation_json AS rental_calculation
    FROM listings_current listing
    LEFT JOIN listing_market_scores market
      ON market.source = listing.source
     AND market.source_listing_id = listing.source_listing_id
    LEFT JOIN LATERAL (
        SELECT valuation.*
        FROM listing_comparable_valuations valuation
        WHERE valuation.source = listing.source
          AND valuation.source_listing_id = listing.source_listing_id
        ORDER BY valuation.as_of_date DESC, valuation.created_at DESC, valuation.id DESC
        LIMIT 1
    ) valuation ON TRUE
    LEFT JOIN LATERAL (
        SELECT aggregate.*
        FROM deal_score_v2_scores aggregate
        WHERE aggregate.valuation_id = valuation.id
        ORDER BY aggregate.created_at DESC, aggregate.id DESC
        LIMIT 1
    ) score ON TRUE
    LEFT JOIN LATERAL (
        SELECT estimate.*
        FROM listing_rent_estimates estimate
        WHERE estimate.source = listing.source
          AND estimate.source_listing_id = listing.source_listing_id
        ORDER BY estimate.as_of_date DESC, estimate.created_at DESC, estimate.id DESC
        LIMIT 1
    ) rent ON TRUE
    LEFT JOIN LATERAL (
        SELECT analysis.*
        FROM rental_underwriting_analyses analysis
        WHERE analysis.rent_estimate_id = rent.id
        ORDER BY analysis.created_at DESC, analysis.id DESC
        LIMIT 1
    ) rental ON TRUE
    WHERE listing.source = :source
    ORDER BY
        COALESCE(score.total_points, score.normalized_available_score) DESC NULLS LAST,
        listing.list_price ASC NULLS LAST,
        listing.source_listing_id
    """
)

COMPONENTS_SQL = text(
    """
    SELECT component.valuation_id, component.component_key, component.status,
           component.points, component.max_points, component.reason
    FROM deal_score_v2_components component
    WHERE component.valuation_id = ANY(:valuation_ids)
    ORDER BY component.valuation_id, component.id
    """
)

RUNS_SQL = text(
    """
    SELECT id, status, started_at, finished_at, listing_count,
           comparable_ready_count, supported_valuation_count, alert_eligible_count
    FROM shadow_pipeline_runs
    WHERE source = :source
    ORDER BY started_at DESC, id DESC
    LIMIT 8
    """
)


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _row_dict(row: Any) -> dict[str, Any]:
    return {key: _serialize(value) for key, value in row.items()}


def _review_flags(listing: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if listing["scoring_eligible"] and listing["sqft"] is None:
        flags.append("Missing square footage")
    if listing["valuation_status"] == "insufficient_comparables":
        flags.append("Insufficient comparables")
    if listing["confidence_label"] in {"low", "very_low"}:
        flags.append("Low comparable confidence")
    if listing["deal_score_status"] in {"partial", "unavailable"}:
        flags.append("Incomplete Deal Score")
    return flags


def summarize(listings: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "listing_count": len(listings),
        "scoring_eligible_count": sum(
            bool(row["scoring_eligible"]) for row in listings
        ),
        "complete_score_count": sum(
            row["deal_score_status"] == "complete" for row in listings
        ),
        "valued_count": sum(row["valuation_status"] == "valued" for row in listings),
        "low_confidence_count": sum(
            row["confidence_label"] in {"low", "very_low"} for row in listings
        ),
        "missing_sqft_count": sum(
            bool(row["scoring_eligible"]) and row["sqft"] is None for row in listings
        ),
        "review_count": sum(bool(row["review_flags"]) for row in listings),
        "alert_eligible_count": sum(bool(row["alert_eligible"]) for row in listings),
        "rent_estimate_count": sum(row["monthly_rent"] is not None for row in listings),
        "positive_cash_flow_count": sum(
            row["monthly_cash_flow"] is not None and row["monthly_cash_flow"] > 0
            for row in listings
        ),
        "dscr_constrained_count": sum(
            row["rental_loan_constraint"] == "dscr" for row in listings
        ),
    }


def build_dashboard_data(*, engine: Engine, source: str = "rentcast") -> dict[str, Any]:
    with engine.connect() as connection:
        listings = [
            _row_dict(row)
            for row in connection.execute(LISTINGS_SQL, {"source": source}).mappings()
        ]
        valuation_ids = [row["valuation_id"] for row in listings if row["valuation_id"]]
        components: dict[int, list[dict[str, Any]]] = {}
        if valuation_ids:
            for row in connection.execute(
                COMPONENTS_SQL, {"valuation_ids": valuation_ids}
            ).mappings():
                item = _row_dict(row)
                components.setdefault(item.pop("valuation_id"), []).append(item)
        runs = [
            _row_dict(row)
            for row in connection.execute(RUNS_SQL, {"source": source}).mappings()
        ]

    for listing in listings:
        listing["components"] = components.get(listing["valuation_id"], [])
        listing["review_flags"] = _review_flags(listing)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "provisional": True,
        "summary": summarize(listings),
        "readiness": asdict(assess_shadow_readiness(source=source, engine=engine)),
        "runs": runs,
        "listings": listings,
    }

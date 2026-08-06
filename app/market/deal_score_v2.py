from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from dateutil.relativedelta import relativedelta


SCORING_VERSION = "deal-score-v2-draft-5"
COMPARABLE_DISCOUNT_COMPONENT = "comparable_discount"
COMPARABLE_DISCOUNT_MAX_POINTS = Decimal("40.00")
FULL_CREDIT_DISCOUNT = Decimal("0.30")
MINIMUM_CONFIDENCE_SCORE = Decimal("40.0")
POINTS_QUANTUM = Decimal("0.01")

LISTING_OPPORTUNITY_COMPONENT = "listing_opportunity"
LISTING_OPPORTUNITY_MAX_POINTS = Decimal("15.00")
CUMULATIVE_REDUCTION_MAX_POINTS = Decimal("9.00")
FULL_CREDIT_CUMULATIVE_REDUCTION = Decimal("0.15")
REDUCTION_FREQUENCY_MAX_POINTS = Decimal("4.00")
FULL_CREDIT_REDUCTION_COUNT = Decimal("3")
RECENT_REDUCTION_MAX_POINTS = Decimal("2.00")

DAYS_ON_MARKET_COMPONENT = "days_on_market"
DAYS_ON_MARKET_MAX_POINTS = Decimal("15.00")
DAYS_ON_MARKET_ZERO_POINT_RATIO = Decimal("0.75")
DAYS_ON_MARKET_FULL_CREDIT_RATIO = Decimal("2.00")
MAX_LISTING_EVIDENCE_AGE_DAYS = 30
MAX_MARKET_CONTEXT_AGE_DAYS = 180

MARKET_MOMENTUM_COMPONENT = "market_momentum"
MARKET_MOMENTUM_MAX_POINTS = Decimal("15.00")
MARKET_MOMENTUM_NEUTRAL_POINTS = Decimal("7.50")
MARKET_MOMENTUM_FLOOR_CHANGE = Decimal("-0.10")
MARKET_MOMENTUM_FULL_CREDIT_CHANGE = Decimal("0.10")
MARKET_MOMENTUM_HORIZONS = (
    {
        "key": "three_month",
        "months": 3,
        "weight": Decimal("0.40"),
        "reference_tolerance_days": 45,
    },
    {
        "key": "twelve_month",
        "months": 12,
        "weight": Decimal("0.60"),
        "reference_tolerance_days": 60,
    },
)

LIQUIDITY_INVENTORY_COMPONENT = "liquidity_inventory"
LIQUIDITY_INVENTORY_MAX_POINTS = Decimal("10.00")
INVENTORY_MONTHS_MAX_POINTS = Decimal("6.00")
FULL_CREDIT_INVENTORY_MONTHS = Decimal("2.00")
ZERO_CREDIT_INVENTORY_MONTHS = Decimal("8.00")
SALES_FLOW_MAX_POINTS = Decimal("4.00")
ZERO_CREDIT_SALES_TO_NEW_LISTINGS = Decimal("0.50")
FULL_CREDIT_SALES_TO_NEW_LISTINGS = Decimal("1.00")

DATA_CONFIDENCE_COMPONENT = "data_confidence"
DATA_CONFIDENCE_MAX_POINTS = Decimal("5.00")
COMPARABLE_CONFIDENCE_MAX_POINTS = Decimal("3.00")
EVIDENCE_COVERAGE_MAX_POINTS = Decimal("2.00")
SUBSTANTIVE_COMPONENT_MAX_POINTS = {
    COMPARABLE_DISCOUNT_COMPONENT: COMPARABLE_DISCOUNT_MAX_POINTS,
    LISTING_OPPORTUNITY_COMPONENT: LISTING_OPPORTUNITY_MAX_POINTS,
    DAYS_ON_MARKET_COMPONENT: DAYS_ON_MARKET_MAX_POINTS,
    MARKET_MOMENTUM_COMPONENT: MARKET_MOMENTUM_MAX_POINTS,
    LIQUIDITY_INVENTORY_COMPONENT: LIQUIDITY_INVENTORY_MAX_POINTS,
}
DEAL_SCORE_COMPONENT_MAX_POINTS = {
    **SUBSTANTIVE_COMPONENT_MAX_POINTS,
    DATA_CONFIDENCE_COMPONENT: DATA_CONFIDENCE_MAX_POINTS,
}
DEAL_SCORE_MAX_POINTS = sum(
    DEAL_SCORE_COMPONENT_MAX_POINTS.values(),
    Decimal("0"),
)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def calculate_comparable_discount_component(
    valuation: dict[str, Any],
) -> dict[str, Any]:
    confidence_score = _decimal(valuation.get("confidence_score"))
    discount_pct = _decimal(valuation.get("list_price_discount_pct"))

    status = "available"
    points = None
    if valuation.get("status") != "valued":
        status = "unavailable"
        reason = "Comparable valuation is unavailable."
    elif confidence_score is None or confidence_score < MINIMUM_CONFIDENCE_SCORE:
        status = "unavailable"
        reason = (
            "Comparable valuation confidence is below the minimum "
            f"{MINIMUM_CONFIDENCE_SCORE} threshold."
        )
    elif discount_pct is None:
        status = "unavailable"
        reason = "A listing price and estimated value are required."
    else:
        qualifying_discount = max(Decimal("0"), discount_pct)
        points = min(
            COMPARABLE_DISCOUNT_MAX_POINTS,
            qualifying_discount
            / FULL_CREDIT_DISCOUNT
            * COMPARABLE_DISCOUNT_MAX_POINTS,
        ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
        if discount_pct <= 0:
            reason = "Listing is at or above the supported comparable value."
        elif points == COMPARABLE_DISCOUNT_MAX_POINTS:
            reason = (
                "Listing is at least 30% below the supported comparable value."
            )
        else:
            reason = (
                f"Listing is {discount_pct * 100:.2f}% below the supported "
                f"comparable value, contributing {points} of "
                f"{COMPARABLE_DISCOUNT_MAX_POINTS} points."
            )

    return {
        "scoring_version": SCORING_VERSION,
        "component_key": COMPARABLE_DISCOUNT_COMPONENT,
        "status": status,
        "points": points,
        "max_points": COMPARABLE_DISCOUNT_MAX_POINTS,
        "reason": reason,
        "inputs": {
            "valuation_version": valuation.get("valuation_version"),
            "valuation_status": valuation.get("status"),
            "estimated_value": valuation.get("estimated_value"),
            "list_price": valuation.get("subject", {}).get("list_price"),
            "list_price_discount_pct": discount_pct,
            "confidence_score": confidence_score,
            "minimum_confidence_score": MINIMUM_CONFIDENCE_SCORE,
            "full_credit_discount_pct": FULL_CREDIT_DISCOUNT,
        },
    }


def _price_observations(
    price_history: list[dict[str, Any]],
    as_of_date: date,
) -> list[dict[str, Any]]:
    observations = []
    for row in sorted(
        price_history,
        key=lambda item: (item["snapshot_timestamp"], item.get("id", 0)),
    ):
        snapshot_timestamp = row["snapshot_timestamp"]
        snapshot_date = (
            snapshot_timestamp.date()
            if isinstance(snapshot_timestamp, datetime)
            else snapshot_timestamp
        )
        if snapshot_date > as_of_date:
            continue
        price = _decimal(row.get("list_price"))
        if price is None or price <= 0:
            continue
        observations.append(
            {
                "history_id": row.get("id"),
                "list_price": price,
                "snapshot_timestamp": snapshot_timestamp,
            }
        )
    return observations


def calculate_listing_opportunity_component(
    price_history: list[dict[str, Any]],
    *,
    as_of_date: date,
) -> dict[str, Any]:
    observations = _price_observations(price_history, as_of_date)
    if not observations:
        return {
            "scoring_version": SCORING_VERSION,
            "component_key": LISTING_OPPORTUNITY_COMPONENT,
            "status": "unavailable",
            "points": None,
            "max_points": LISTING_OPPORTUNITY_MAX_POINTS,
            "reason": "No valid listing price history is available as of the analysis date.",
            "inputs": {
                "as_of_date": as_of_date,
                "price_observations": [],
            },
        }

    reductions = []
    previous = observations[0]
    for observation in observations[1:]:
        if observation["list_price"] < previous["list_price"]:
            amount = previous["list_price"] - observation["list_price"]
            reductions.append(
                {
                    "from_price": previous["list_price"],
                    "to_price": observation["list_price"],
                    "reduction_amount": amount,
                    "reduction_pct": amount / previous["list_price"],
                    "effective_at": observation["snapshot_timestamp"],
                }
            )
        previous = observation

    peak_price = max(item["list_price"] for item in observations)
    latest_price = observations[-1]["list_price"]
    cumulative_reduction_pct = max(
        Decimal("0"),
        (peak_price - latest_price) / peak_price,
    )
    cumulative_points = min(
        CUMULATIVE_REDUCTION_MAX_POINTS,
        cumulative_reduction_pct
        / FULL_CREDIT_CUMULATIVE_REDUCTION
        * CUMULATIVE_REDUCTION_MAX_POINTS,
    ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
    frequency_points = min(
        REDUCTION_FREQUENCY_MAX_POINTS,
        Decimal(len(reductions))
        / FULL_CREDIT_REDUCTION_COUNT
        * REDUCTION_FREQUENCY_MAX_POINTS,
    ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)

    days_since_latest_reduction = None
    recency_points = Decimal("0.00")
    if reductions:
        latest_reduction_at = reductions[-1]["effective_at"]
        if isinstance(latest_reduction_at, datetime):
            latest_reduction_date = latest_reduction_at.date()
        else:
            latest_reduction_date = latest_reduction_at
        days_since_latest_reduction = (as_of_date - latest_reduction_date).days
        if days_since_latest_reduction <= 30:
            recency_points = RECENT_REDUCTION_MAX_POINTS
        elif days_since_latest_reduction <= 90:
            recency_points = Decimal("1.00")

    points = cumulative_points + frequency_points + recency_points
    if reductions:
        reason = (
            f"Listing is {cumulative_reduction_pct * 100:.2f}% below its "
            f"historical peak after {len(reductions)} price reduction(s)."
        )
    else:
        reason = "No price reductions are present in the available listing history."

    return {
        "scoring_version": SCORING_VERSION,
        "component_key": LISTING_OPPORTUNITY_COMPONENT,
        "status": "available",
        "points": points,
        "max_points": LISTING_OPPORTUNITY_MAX_POINTS,
        "reason": reason,
        "inputs": {
            "as_of_date": as_of_date,
            "initial_price": observations[0]["list_price"],
            "peak_price": peak_price,
            "latest_price": latest_price,
            "cumulative_reduction_pct": cumulative_reduction_pct,
            "reduction_count": len(reductions),
            "days_since_latest_reduction": days_since_latest_reduction,
            "price_observations": observations,
            "reductions": reductions,
            "point_contributions": {
                "cumulative_reduction": cumulative_points,
                "reduction_frequency": frequency_points,
                "reduction_recency": recency_points,
            },
            "thresholds": {
                "full_credit_cumulative_reduction_pct": (
                    FULL_CREDIT_CUMULATIVE_REDUCTION
                ),
                "full_credit_reduction_count": FULL_CREDIT_REDUCTION_COUNT,
                "recent_reduction_full_credit_days": 30,
                "recent_reduction_partial_credit_days": 90,
            },
        },
    }


def calculate_days_on_market_component(
    *,
    subject_days_on_market: Any,
    listing_snapshot_timestamp: datetime | None,
    county_median_days_on_market: Any,
    county_name: str | None,
    market_period_date: date | None,
    as_of_date: date,
) -> dict[str, Any]:
    subject_days = _decimal(subject_days_on_market)
    county_median = _decimal(county_median_days_on_market)
    listing_evidence_age = (
        (as_of_date - listing_snapshot_timestamp.date()).days
        if listing_snapshot_timestamp is not None
        else None
    )
    market_context_age = (
        (as_of_date - market_period_date).days
        if market_period_date is not None
        else None
    )
    inputs = {
        "as_of_date": as_of_date,
        "subject_days_on_market": subject_days,
        "listing_snapshot_timestamp": listing_snapshot_timestamp,
        "listing_evidence_age_days": listing_evidence_age,
        "county_name": county_name,
        "county_median_days_on_market": county_median,
        "market_period_date": market_period_date,
        "market_context_age_days": market_context_age,
        "thresholds": {
            "zero_point_ratio": DAYS_ON_MARKET_ZERO_POINT_RATIO,
            "full_credit_ratio": DAYS_ON_MARKET_FULL_CREDIT_RATIO,
            "max_listing_evidence_age_days": MAX_LISTING_EVIDENCE_AGE_DAYS,
            "max_market_context_age_days": MAX_MARKET_CONTEXT_AGE_DAYS,
        },
    }

    unavailable_reason = None
    if subject_days is None or subject_days < 0:
        unavailable_reason = "Valid listing days-on-market history is unavailable."
    elif (
        listing_evidence_age is None
        or not 0 <= listing_evidence_age <= MAX_LISTING_EVIDENCE_AGE_DAYS
    ):
        unavailable_reason = "Listing days-on-market evidence is missing or stale."
    elif county_median is None or county_median <= 0:
        unavailable_reason = "A valid county median days on market is unavailable."
    elif (
        market_context_age is None
        or not 0 <= market_context_age <= MAX_MARKET_CONTEXT_AGE_DAYS
    ):
        unavailable_reason = "County days-on-market context is missing or stale."

    if unavailable_reason is not None:
        return {
            "scoring_version": SCORING_VERSION,
            "component_key": DAYS_ON_MARKET_COMPONENT,
            "status": "unavailable",
            "points": None,
            "max_points": DAYS_ON_MARKET_MAX_POINTS,
            "reason": unavailable_reason,
            "inputs": inputs,
        }

    days_on_market_ratio = subject_days / county_median
    qualifying_ratio = max(
        Decimal("0"),
        days_on_market_ratio - DAYS_ON_MARKET_ZERO_POINT_RATIO,
    )
    scoring_range = (
        DAYS_ON_MARKET_FULL_CREDIT_RATIO
        - DAYS_ON_MARKET_ZERO_POINT_RATIO
    )
    points = min(
        DAYS_ON_MARKET_MAX_POINTS,
        qualifying_ratio / scoring_range * DAYS_ON_MARKET_MAX_POINTS,
    ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
    inputs["days_on_market_ratio"] = days_on_market_ratio

    if points == 0:
        reason = (
            "Listing days on market are no more than 75% of the county median."
        )
    elif points == DAYS_ON_MARKET_MAX_POINTS:
        reason = (
            "Listing days on market are at least twice the county median, "
            "indicating maximum time-on-market negotiation leverage."
        )
    else:
        reason = (
            f"Listing days on market are {days_on_market_ratio:.2f} times the "
            f"{county_name} County median."
        )

    return {
        "scoring_version": SCORING_VERSION,
        "component_key": DAYS_ON_MARKET_COMPONENT,
        "status": "available",
        "points": points,
        "max_points": DAYS_ON_MARKET_MAX_POINTS,
        "reason": reason,
        "inputs": inputs,
    }


def _market_price_observations(
    market_history: list[dict[str, Any]],
    as_of_date: date,
) -> list[dict[str, Any]]:
    observations = []
    for row in sorted(
        market_history,
        key=lambda item: (item["period_date"], item.get("id", 0)),
    ):
        period = row["period_date"]
        period_date = period.date() if isinstance(period, datetime) else period
        price = _decimal(row.get("median_sale_price"))
        if period_date > as_of_date or price is None or price <= 0:
            continue
        observations.append(
            {
                "county_name": row.get("county_name"),
                "period_date": period_date,
                "median_sale_price": price,
            }
        )
    return observations


def _momentum_horizon_points(change: Decimal) -> Decimal:
    qualifying_change = min(
        MARKET_MOMENTUM_FULL_CREDIT_CHANGE,
        max(MARKET_MOMENTUM_FLOOR_CHANGE, change),
    )
    scoring_range = MARKET_MOMENTUM_FULL_CREDIT_CHANGE - MARKET_MOMENTUM_FLOOR_CHANGE
    return (
        (qualifying_change - MARKET_MOMENTUM_FLOOR_CHANGE)
        / scoring_range
        * MARKET_MOMENTUM_MAX_POINTS
    )


def calculate_market_momentum_component(
    market_history: list[dict[str, Any]],
    *,
    as_of_date: date,
) -> dict[str, Any]:
    observations = _market_price_observations(market_history, as_of_date)
    latest = observations[-1] if observations else None
    latest_age_days = (as_of_date - latest["period_date"]).days if latest else None
    inputs = {
        "as_of_date": as_of_date,
        "county_name": latest["county_name"] if latest else None,
        "latest_observation": latest,
        "latest_observation_age_days": latest_age_days,
        "price_observations": observations,
        "horizons": {},
        "thresholds": {
            "floor_change_pct": MARKET_MOMENTUM_FLOOR_CHANGE,
            "full_credit_change_pct": MARKET_MOMENTUM_FULL_CREDIT_CHANGE,
            "neutral_points": MARKET_MOMENTUM_NEUTRAL_POINTS,
            "max_market_context_age_days": MAX_MARKET_CONTEXT_AGE_DAYS,
        },
    }

    if latest is None:
        unavailable_reason = "No valid county median sale-price history is available."
    elif latest_age_days is None or not (
        0 <= latest_age_days <= MAX_MARKET_CONTEXT_AGE_DAYS
    ):
        unavailable_reason = "County sale-price history is missing or stale."
    else:
        unavailable_reason = None

    if unavailable_reason is not None:
        return {
            "scoring_version": SCORING_VERSION,
            "component_key": MARKET_MOMENTUM_COMPONENT,
            "status": "unavailable",
            "points": None,
            "max_points": MARKET_MOMENTUM_MAX_POINTS,
            "reason": unavailable_reason,
            "inputs": inputs,
        }

    weighted_points = Decimal("0")
    available_weight = Decimal("0")
    for definition in MARKET_MOMENTUM_HORIZONS:
        target_date = latest["period_date"] - relativedelta(months=definition["months"])
        earliest_reference_date = target_date - timedelta(
            days=definition["reference_tolerance_days"]
        )
        references = [
            observation
            for observation in observations[:-1]
            if earliest_reference_date <= observation["period_date"] <= target_date
        ]
        reference = references[-1] if references else None
        horizon = {
            "months": definition["months"],
            "weight": definition["weight"],
            "target_date": target_date,
            "earliest_reference_date": earliest_reference_date,
            "reference_observation": reference,
            "status": "available" if reference else "unavailable",
        }
        if reference is not None:
            change = latest["median_sale_price"] / reference[
                "median_sale_price"
            ] - Decimal("1")
            horizon_points = _momentum_horizon_points(change)
            horizon["change_pct"] = change
            horizon["points_before_weight"] = horizon_points.quantize(
                POINTS_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
            weighted_points += definition["weight"] * horizon_points
            available_weight += definition["weight"]
        inputs["horizons"][definition["key"]] = horizon

    if available_weight == 0:
        return {
            "scoring_version": SCORING_VERSION,
            "component_key": MARKET_MOMENTUM_COMPONENT,
            "status": "unavailable",
            "points": None,
            "max_points": MARKET_MOMENTUM_MAX_POINTS,
            "reason": "County sale-price history has no usable 3- or 12-month reference.",
            "inputs": inputs,
        }

    neutral_weight = Decimal("1") - available_weight
    points = (
        weighted_points + neutral_weight * MARKET_MOMENTUM_NEUTRAL_POINTS
    ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
    inputs["available_weight"] = available_weight
    inputs["neutral_weight"] = neutral_weight
    inputs["point_contributions"] = {
        "available_horizons": weighted_points,
        "missing_horizon_neutral_fill": (
            neutral_weight * MARKET_MOMENTUM_NEUTRAL_POINTS
        ),
    }

    available_horizon_count = sum(
        horizon["status"] == "available" for horizon in inputs["horizons"].values()
    )
    reason = (
        f"County median sale-price momentum contributes {points} of "
        f"{MARKET_MOMENTUM_MAX_POINTS} points using "
        f"{available_horizon_count} of 2 trend horizons."
    )
    return {
        "scoring_version": SCORING_VERSION,
        "component_key": MARKET_MOMENTUM_COMPONENT,
        "status": "available",
        "points": points,
        "max_points": MARKET_MOMENTUM_MAX_POINTS,
        "reason": reason,
        "inputs": inputs,
    }


def calculate_liquidity_inventory_component(
    *,
    homes_sold: Any,
    active_listings: Any,
    new_listings: Any,
    county_name: str | None,
    market_period_date: date | None,
    as_of_date: date,
) -> dict[str, Any]:
    sold = _decimal(homes_sold)
    active = _decimal(active_listings)
    new = _decimal(new_listings)
    market_context_age = (
        (as_of_date - market_period_date).days
        if market_period_date is not None
        else None
    )
    inputs = {
        "as_of_date": as_of_date,
        "county_name": county_name,
        "market_period_date": market_period_date,
        "market_context_age_days": market_context_age,
        "homes_sold": sold,
        "active_listings": active,
        "new_listings": new,
        "thresholds": {
            "full_credit_inventory_months": FULL_CREDIT_INVENTORY_MONTHS,
            "zero_credit_inventory_months": ZERO_CREDIT_INVENTORY_MONTHS,
            "zero_credit_sales_to_new_listings": (
                ZERO_CREDIT_SALES_TO_NEW_LISTINGS
            ),
            "full_credit_sales_to_new_listings": (
                FULL_CREDIT_SALES_TO_NEW_LISTINGS
            ),
            "max_market_context_age_days": MAX_MARKET_CONTEXT_AGE_DAYS,
        },
    }

    unavailable_reason = None
    if sold is None or sold <= 0:
        unavailable_reason = "A positive county homes-sold count is unavailable."
    elif (
        market_context_age is None
        or not 0 <= market_context_age <= MAX_MARKET_CONTEXT_AGE_DAYS
    ):
        unavailable_reason = "County liquidity and inventory context is missing or stale."
    elif (active is None or active < 0) and (new is None or new <= 0):
        unavailable_reason = (
            "County active- or new-listing inventory is unavailable."
        )

    if unavailable_reason is not None:
        return {
            "scoring_version": SCORING_VERSION,
            "component_key": LIQUIDITY_INVENTORY_COMPONENT,
            "status": "unavailable",
            "points": None,
            "max_points": LIQUIDITY_INVENTORY_MAX_POINTS,
            "reason": unavailable_reason,
            "inputs": inputs,
        }

    inventory_months = None
    inventory_points = None
    if active is not None and active >= 0:
        inventory_months = active / sold
        qualifying_inventory = min(
            ZERO_CREDIT_INVENTORY_MONTHS,
            max(FULL_CREDIT_INVENTORY_MONTHS, inventory_months),
        )
        inventory_points = (
            (ZERO_CREDIT_INVENTORY_MONTHS - qualifying_inventory)
            / (ZERO_CREDIT_INVENTORY_MONTHS - FULL_CREDIT_INVENTORY_MONTHS)
            * INVENTORY_MONTHS_MAX_POINTS
        )

    sales_to_new_listings = None
    sales_flow_points = None
    if new is not None and new > 0:
        sales_to_new_listings = sold / new
        qualifying_sales_flow = min(
            FULL_CREDIT_SALES_TO_NEW_LISTINGS,
            max(ZERO_CREDIT_SALES_TO_NEW_LISTINGS, sales_to_new_listings),
        )
        sales_flow_points = (
            (qualifying_sales_flow - ZERO_CREDIT_SALES_TO_NEW_LISTINGS)
            / (
                FULL_CREDIT_SALES_TO_NEW_LISTINGS
                - ZERO_CREDIT_SALES_TO_NEW_LISTINGS
            )
            * SALES_FLOW_MAX_POINTS
        )

    neutral_fill = Decimal("0")
    if inventory_points is None:
        neutral_fill += INVENTORY_MONTHS_MAX_POINTS / Decimal("2")
    if sales_flow_points is None:
        neutral_fill += SALES_FLOW_MAX_POINTS / Decimal("2")
    points = (
        (inventory_points or Decimal("0"))
        + (sales_flow_points or Decimal("0"))
        + neutral_fill
    ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)

    inputs["inventory_months"] = inventory_months
    inputs["sales_to_new_listings"] = sales_to_new_listings
    inputs["point_contributions"] = {
        "inventory_months": (
            inventory_points.quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
            if inventory_points is not None
            else None
        ),
        "sales_flow": (
            sales_flow_points.quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
            if sales_flow_points is not None
            else None
        ),
        "missing_metric_neutral_fill": neutral_fill,
    }
    available_metric_count = sum(
        metric is not None for metric in (inventory_points, sales_flow_points)
    )
    reason = (
        f"{county_name} County liquidity and inventory contribute {points} of "
        f"{LIQUIDITY_INVENTORY_MAX_POINTS} points using "
        f"{available_metric_count} of 2 market metrics."
    )
    return {
        "scoring_version": SCORING_VERSION,
        "component_key": LIQUIDITY_INVENTORY_COMPONENT,
        "status": "available",
        "points": points,
        "max_points": LIQUIDITY_INVENTORY_MAX_POINTS,
        "reason": reason,
        "inputs": inputs,
    }


def calculate_data_confidence_component(
    valuation: dict[str, Any],
    substantive_components: list[dict[str, Any]],
) -> dict[str, Any]:
    comparable_confidence = _decimal(valuation.get("confidence_score"))
    components_by_key = {
        component["component_key"]: component
        for component in substantive_components
        if component.get("component_key") in SUBSTANTIVE_COMPONENT_MAX_POINTS
    }
    available_max_points = sum(
        (
            max_points
            for key, max_points in SUBSTANTIVE_COMPONENT_MAX_POINTS.items()
            if components_by_key.get(key, {}).get("status") == "available"
            and components_by_key[key].get("points") is not None
        ),
        Decimal("0"),
    )
    substantive_max_points = sum(
        SUBSTANTIVE_COMPONENT_MAX_POINTS.values(),
        Decimal("0"),
    )
    coverage_ratio = available_max_points / substantive_max_points
    inputs = {
        "valuation_confidence_score": comparable_confidence,
        "valuation_confidence_label": valuation.get("confidence_label"),
        "valuation_confidence_components": valuation.get(
            "confidence_components"
        ),
        "available_substantive_max_points": available_max_points,
        "substantive_max_points": substantive_max_points,
        "coverage_ratio": coverage_ratio,
        "component_statuses": {
            key: components_by_key.get(key, {}).get("status", "missing")
            for key in SUBSTANTIVE_COMPONENT_MAX_POINTS
        },
        "weights": {
            "comparable_quality_max_points": (
                COMPARABLE_CONFIDENCE_MAX_POINTS
            ),
            "evidence_coverage_max_points": EVIDENCE_COVERAGE_MAX_POINTS,
        },
    }
    if (
        comparable_confidence is None
        or comparable_confidence < 0
        or comparable_confidence > 100
    ):
        return {
            "scoring_version": SCORING_VERSION,
            "component_key": DATA_CONFIDENCE_COMPONENT,
            "status": "unavailable",
            "points": None,
            "max_points": DATA_CONFIDENCE_MAX_POINTS,
            "reason": "A valid comparable-valuation confidence score is unavailable.",
            "inputs": inputs,
        }

    comparable_quality_points = (
        comparable_confidence
        / Decimal("100")
        * COMPARABLE_CONFIDENCE_MAX_POINTS
    )
    evidence_coverage_points = coverage_ratio * EVIDENCE_COVERAGE_MAX_POINTS
    points = (comparable_quality_points + evidence_coverage_points).quantize(
        POINTS_QUANTUM,
        rounding=ROUND_HALF_UP,
    )
    inputs["point_contributions"] = {
        "comparable_quality": comparable_quality_points.quantize(
            POINTS_QUANTUM,
            rounding=ROUND_HALF_UP,
        ),
        "evidence_coverage": evidence_coverage_points.quantize(
            POINTS_QUANTUM,
            rounding=ROUND_HALF_UP,
        ),
    }
    return {
        "scoring_version": SCORING_VERSION,
        "component_key": DATA_CONFIDENCE_COMPONENT,
        "status": "available",
        "points": points,
        "max_points": DATA_CONFIDENCE_MAX_POINTS,
        "reason": (
            f"Data confidence contributes {points} of "
            f"{DATA_CONFIDENCE_MAX_POINTS} points from comparable quality "
            f"and {coverage_ratio * 100:.2f}% evidence coverage."
        ),
        "inputs": inputs,
    }


def calculate_deal_score_v2(
    components: list[dict[str, Any]],
) -> dict[str, Any]:
    components_by_key: dict[str, dict[str, Any]] = {}
    for component in components:
        key = component.get("component_key")
        if key not in DEAL_SCORE_COMPONENT_MAX_POINTS:
            raise ValueError(f"Unexpected Deal Score v2 component: {key}")
        if key in components_by_key:
            raise ValueError(f"Duplicate Deal Score v2 component: {key}")
        if component.get("scoring_version") != SCORING_VERSION:
            raise ValueError(f"Component {key} uses a different scoring version")
        if _decimal(component.get("max_points")) != (
            DEAL_SCORE_COMPONENT_MAX_POINTS[key]
        ):
            raise ValueError(f"Component {key} has an unexpected maximum")
        components_by_key[key] = component

    component_summaries = {}
    available_points = Decimal("0")
    available_max_points = Decimal("0")
    missing_components = []
    for key, max_points in DEAL_SCORE_COMPONENT_MAX_POINTS.items():
        component = components_by_key.get(key)
        status = component.get("status") if component else "missing"
        points = _decimal(component.get("points")) if component else None
        if status == "available" and points is not None:
            if not 0 <= points <= max_points:
                raise ValueError(f"Component {key} points are outside its range")
            available_points += points
            available_max_points += max_points
        else:
            missing_components.append(key)
        component_summaries[key] = {
            "status": status,
            "points": points,
            "max_points": max_points,
            "reason": component.get("reason") if component else "Component is missing.",
        }

    coverage_pct = (
        available_max_points / DEAL_SCORE_MAX_POINTS * Decimal("100")
    ).quantize(POINTS_QUANTUM, rounding=ROUND_HALF_UP)
    normalized_available_score = (
        (available_points / available_max_points * Decimal("100")).quantize(
            POINTS_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
        if available_max_points > 0
        else None
    )
    core_component = components_by_key.get(COMPARABLE_DISCOUNT_COMPONENT)
    core_available = (
        core_component is not None
        and core_component.get("status") == "available"
        and core_component.get("points") is not None
    )
    if not core_available:
        status = "unavailable"
        total_points = None
        reason = "Deal Score v2 is unavailable without comparable-discount evidence."
    elif missing_components:
        status = "partial"
        total_points = None
        reason = (
            "Deal Score v2 is partial because these components are unavailable: "
            + ", ".join(missing_components)
            + "."
        )
    else:
        status = "complete"
        total_points = available_points.quantize(
            POINTS_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
        reason = f"Complete Deal Score v2 is {total_points} of {DEAL_SCORE_MAX_POINTS}."

    return {
        "scoring_version": SCORING_VERSION,
        "status": status,
        "total_points": total_points,
        "max_points": DEAL_SCORE_MAX_POINTS,
        "available_points": available_points.quantize(
            POINTS_QUANTUM,
            rounding=ROUND_HALF_UP,
        ),
        "available_max_points": available_max_points,
        "coverage_pct": coverage_pct,
        "normalized_available_score": normalized_available_score,
        "missing_components": missing_components,
        "reason": reason,
        "inputs": {
            "components": component_summaries,
        },
    }

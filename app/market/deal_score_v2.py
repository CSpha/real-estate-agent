from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


SCORING_VERSION = "deal-score-v2-draft-1"
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

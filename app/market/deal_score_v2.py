from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


SCORING_VERSION = "deal-score-v2-draft-1"
COMPARABLE_DISCOUNT_COMPONENT = "comparable_discount"
COMPARABLE_DISCOUNT_MAX_POINTS = Decimal("40.00")
FULL_CREDIT_DISCOUNT = Decimal("0.30")
MINIMUM_CONFIDENCE_SCORE = Decimal("40.0")
POINTS_QUANTUM = Decimal("0.01")


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

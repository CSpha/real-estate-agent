from __future__ import annotations

import argparse
import json
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from statistics import median
from typing import Any

from sqlalchemy import Engine

from app.market.comparable_selection import select_comparables


VALUATION_VERSION = "comparable-valuation-v1"
OUTLIER_PPSF_DEVIATION = Decimal("0.35")
MINIMUM_USABLE_COMPS = 3
MONEY_QUANTUM = Decimal("0.01")
PERCENT_QUANTUM = Decimal("0.0001")
SCORE_QUANTUM = Decimal("0.1")

TIER_CONFIDENCE_POINTS = {
    "strict_zip": Decimal("20"),
    "broad_zip": Decimal("14"),
    "city_fallback": Decimal("8"),
}


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _round_score(value: Decimal) -> Decimal:
    return value.quantize(SCORE_QUANTUM, rounding=ROUND_HALF_UP)


def _clamp(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return min(maximum, max(minimum, value))


def _weighted_quantile(
    observations: list[dict[str, Any]],
    quantile: Decimal,
) -> Decimal:
    ordered = sorted(
        observations,
        key=lambda item: (
            item["price_per_sqft"],
            item["source"],
            item["source_sale_id"],
        ),
    )
    total_weight = sum((item["weight"] for item in ordered), Decimal("0"))
    threshold = total_weight * quantile
    cumulative = Decimal("0")
    for item in ordered:
        cumulative += item["weight"]
        if cumulative >= threshold:
            return item["price_per_sqft"]
    return ordered[-1]["price_per_sqft"]


def _similarity_score(
    comparable: dict[str, Any],
    tier: dict[str, Any],
) -> Decimal:
    comparisons = (
        ("sqft_difference_pct", _decimal(tier["sqft_tolerance"])),
        ("beds_difference", _decimal(tier["bed_tolerance"])),
        ("baths_difference", _decimal(tier["bath_tolerance"])),
        ("lot_difference_pct", _decimal(tier["lot_tolerance"])),
    )
    component_scores = []
    for field, tolerance in comparisons:
        difference = _decimal(comparable.get(field))
        if difference is None or tolerance in (None, 0):
            continue
        component_scores.append(
            _clamp(
                Decimal("1") - (difference / tolerance),
                Decimal("0"),
                Decimal("1"),
            )
        )
    if not component_scores:
        return Decimal("0.5")
    return sum(component_scores, Decimal("0")) / len(component_scores)


def _recency_score(
    comparable: dict[str, Any],
    tier: dict[str, Any],
) -> Decimal:
    maximum_days = Decimal(str(tier["max_sale_age_months"])) * Decimal("30.4375")
    sale_age_days = Decimal(str(comparable["sale_age_days"]))
    return _clamp(
        Decimal("1") - (sale_age_days / maximum_days),
        Decimal("0"),
        Decimal("1"),
    )


def _prepare_observations(
    selection: dict[str, Any],
) -> list[dict[str, Any]]:
    subject_sqft = _decimal(selection["subject"].get("sqft"))
    if subject_sqft is None or subject_sqft <= 0:
        return []

    observations = []
    tier = selection["selected_tier"]
    for comparable in selection["comparables"]:
        sale_price = _decimal(comparable.get("sale_price"))
        comparable_sqft = _decimal(comparable.get("sqft"))
        if (
            sale_price is None
            or sale_price <= 0
            or comparable_sqft is None
            or comparable_sqft <= 0
        ):
            continue

        price_per_sqft = sale_price / comparable_sqft
        similarity_score = _similarity_score(comparable, tier)
        recency_score = _recency_score(comparable, tier)
        weight = (
            Decimal("0.25") + Decimal("0.75") * similarity_score
        ) * (
            Decimal("0.50") + Decimal("0.50") * recency_score
        )
        observations.append(
            {
                **comparable,
                "price_per_sqft": price_per_sqft,
                "implied_subject_value": price_per_sqft * subject_sqft,
                "similarity_score": similarity_score,
                "recency_score": recency_score,
                "weight": weight,
            }
        )
    return observations


def _apply_outlier_policy(
    observations: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    center = median(item["price_per_sqft"] for item in observations)
    for item in observations:
        item["outlier"] = (
            abs(item["price_per_sqft"] - center) / center
            > OUTLIER_PPSF_DEVIATION
        )

    ordinary = [item for item in observations if not item["outlier"]]
    filter_applied = (
        len(ordinary) < len(observations)
        and len(ordinary) >= MINIMUM_USABLE_COMPS
    )
    included = ordinary if filter_applied else observations
    included_ids = {
        (item["source"], item["source_sale_id"]) for item in included
    }
    total_weight = sum((item["weight"] for item in included), Decimal("0"))
    for item in observations:
        item["included_in_valuation"] = (
            item["source"],
            item["source_sale_id"],
        ) in included_ids
        item["normalized_weight"] = (
            item["weight"] / total_weight
            if item["included_in_valuation"] and total_weight
            else Decimal("0")
        )
    return included, filter_applied


def _confidence(
    selection: dict[str, Any],
    included: list[dict[str, Any]],
    median_ppsf: Decimal | None,
    lower_ppsf: Decimal | None,
    upper_ppsf: Decimal | None,
) -> tuple[Decimal, str, dict[str, Decimal]]:
    count_points = min(
        Decimal("30"),
        Decimal(len(included)) / Decimal("5") * Decimal("30"),
    )
    tier_points = TIER_CONFIDENCE_POINTS[selection["selected_tier"]["key"]]

    if included:
        recency_points = (
            sum((item["recency_score"] for item in included), Decimal("0"))
            / len(included)
            * Decimal("20")
        )
        similarity_points = (
            sum((item["similarity_score"] for item in included), Decimal("0"))
            / len(included)
            * Decimal("20")
        )
    else:
        recency_points = Decimal("0")
        similarity_points = Decimal("0")

    tracked_criteria = {"beds", "baths", "sqft", "lot_size_acres", "year_built"}
    unavailable = tracked_criteria.intersection(selection["criteria_not_applied"])
    completeness_points = (
        Decimal(len(tracked_criteria) - len(unavailable))
        / len(tracked_criteria)
        * Decimal("10")
    )

    dispersion_penalty = Decimal("0")
    if (
        median_ppsf is not None
        and median_ppsf > 0
        and lower_ppsf is not None
        and upper_ppsf is not None
    ):
        relative_spread = (upper_ppsf - lower_ppsf) / median_ppsf
        dispersion_penalty = min(
            Decimal("15"),
            relative_spread * Decimal("30"),
        )

    components = {
        "sample_size_points": _round_score(count_points),
        "geography_points": _round_score(tier_points),
        "recency_points": _round_score(recency_points),
        "similarity_points": _round_score(similarity_points),
        "completeness_points": _round_score(completeness_points),
        "dispersion_penalty": _round_score(dispersion_penalty),
    }
    score = _clamp(
        sum(
            (
                count_points,
                tier_points,
                recency_points,
                similarity_points,
                completeness_points,
                -dispersion_penalty,
            ),
            Decimal("0"),
        ),
        Decimal("0"),
        Decimal("100"),
    )
    if len(included) < MINIMUM_USABLE_COMPS:
        score = min(score, Decimal("39.9"))
    score = _round_score(score)
    if score >= 80:
        label = "high"
    elif score >= 60:
        label = "medium"
    elif score >= 40:
        label = "low"
    else:
        label = "very_low"
    return score, label, components


def calculate_comparable_valuation(
    selection: dict[str, Any],
) -> dict[str, Any]:
    subject = selection["subject"]
    subject_sqft = _decimal(subject.get("sqft"))
    observations = _prepare_observations(selection)

    if subject_sqft is None or subject_sqft <= 0:
        status = "insufficient_subject_data"
        reason = "Subject square footage is required for price-per-square-foot valuation."
        included = []
        outlier_filter_applied = False
    elif len(observations) < MINIMUM_USABLE_COMPS:
        status = "insufficient_comparables"
        reason = (
            f"At least {MINIMUM_USABLE_COMPS} comparable sales with price and "
            "square footage are required."
        )
        included = observations
        outlier_filter_applied = False
    else:
        included, outlier_filter_applied = _apply_outlier_policy(observations)
        status = "valued"
        reason = (
            "Estimated from the weighted median price per square foot of "
            f"{len(included)} included comparable sales."
        )

    median_ppsf = None
    lower_ppsf = None
    upper_ppsf = None
    estimated_value = None
    estimated_value_low = None
    estimated_value_high = None
    list_price_discount_amount = None
    list_price_discount_pct = None
    if status == "valued":
        median_ppsf = _weighted_quantile(included, Decimal("0.50"))
        lower_ppsf = _weighted_quantile(included, Decimal("0.25"))
        upper_ppsf = _weighted_quantile(included, Decimal("0.75"))
        estimated_value = _round_money(median_ppsf * subject_sqft)
        estimated_value_low = _round_money(lower_ppsf * subject_sqft)
        estimated_value_high = _round_money(upper_ppsf * subject_sqft)
        list_price = _decimal(subject.get("list_price"))
        if list_price is not None:
            list_price_discount_amount = _round_money(estimated_value - list_price)
            list_price_discount_pct = (
                (list_price_discount_amount / estimated_value).quantize(
                    PERCENT_QUANTUM,
                    rounding=ROUND_HALF_UP,
                )
                if estimated_value
                else None
            )

    confidence_score, confidence_label, confidence_components = _confidence(
        selection,
        included,
        median_ppsf,
        lower_ppsf,
        upper_ppsf,
    )

    comparable_details = []
    for item in observations:
        comparable_details.append(
            {
                "source": item["source"],
                "source_sale_id": item["source_sale_id"],
                "sale_date": item["sale_date"],
                "sale_price": item["sale_price"],
                "sqft": item["sqft"],
                "price_per_sqft": _round_money(item["price_per_sqft"]),
                "implied_subject_value": _round_money(
                    item["implied_subject_value"]
                ),
                "similarity_score": _round_score(item["similarity_score"] * 100),
                "recency_score": _round_score(item["recency_score"] * 100),
                "outlier": item.get("outlier", False),
                "included_in_valuation": item.get(
                    "included_in_valuation",
                    status == "valued",
                ),
                "normalized_weight": _round_score(
                    item.get("normalized_weight", Decimal("0")) * 100
                ),
            }
        )

    return {
        "valuation_version": VALUATION_VERSION,
        "selection_version": selection["selection_version"],
        "as_of_date": selection["as_of_date"],
        "status": status,
        "reason": reason,
        "subject": subject,
        "selected_tier": selection["selected_tier"]["key"],
        "available_comparable_count": selection["available_comparable_count"],
        "included_comparable_count": len(included),
        "outlier_filter_applied": outlier_filter_applied,
        "outlier_threshold_pct": OUTLIER_PPSF_DEVIATION * 100,
        "weighted_median_price_per_sqft": (
            _round_money(median_ppsf) if median_ppsf is not None else None
        ),
        "estimated_value": estimated_value,
        "estimated_value_low": estimated_value_low,
        "estimated_value_high": estimated_value_high,
        "list_price_discount_amount": list_price_discount_amount,
        "list_price_discount_pct": list_price_discount_pct,
        "confidence_score": confidence_score,
        "confidence_label": confidence_label,
        "confidence_components": confidence_components,
        "criteria_not_applied": selection["criteria_not_applied"],
        "comparables": comparable_details,
    }


def value_listing(
    source: str,
    source_listing_id: str,
    *,
    as_of_date: date | None = None,
    min_comps: int = MINIMUM_USABLE_COMPS,
    max_comps: int = 5,
    engine: Engine | None = None,
) -> dict[str, Any]:
    selection = select_comparables(
        source,
        source_listing_id,
        as_of_date=as_of_date,
        min_comps=min_comps,
        max_comps=max_comps,
        engine=engine,
    )
    return calculate_comparable_valuation(selection)


def _json_default(value: Any) -> str:
    if isinstance(value, (date, Decimal)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Estimate a listing value from selected comparable sales."
    )
    parser.add_argument("source", help="Listing source")
    parser.add_argument("source_listing_id", help="Source listing identifier")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--min-comps", type=int, default=MINIMUM_USABLE_COMPS)
    parser.add_argument("--max-comps", type=int, default=5)
    args = parser.parse_args()

    result = value_listing(
        args.source,
        args.source_listing_id,
        as_of_date=args.as_of_date,
        min_comps=args.min_comps,
        max_comps=args.max_comps,
    )
    print(json.dumps(result, default=_json_default, indent=2))


if __name__ == "__main__":
    main()

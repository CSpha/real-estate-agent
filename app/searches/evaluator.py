from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.searches.models import (
    CriteriaEvaluation,
    EvaluationReason,
    ListingCandidate,
    NumericRange,
    SearchCriteria,
)


def evaluate_listing(
    criteria: SearchCriteria,
    listing: ListingCandidate,
) -> CriteriaEvaluation:
    passed: list[EvaluationReason] = []
    failed: list[EvaluationReason] = []

    if criteria.locations:
        _evaluate_locations(criteria, listing, passed, failed)

    for criterion_name, label, bounds, value in (
        ("price", "price", criteria.price, listing.list_price),
        ("beds", "beds", criteria.beds, listing.beds),
        ("baths", "baths", criteria.baths, listing.baths),
        ("sqft", "square feet", criteria.sqft, listing.sqft),
        (
            "lot_size_acres",
            "lot size in acres",
            criteria.lot_size_acres,
            listing.lot_size_acres,
        ),
    ):
        if bounds is not None:
            _evaluate_range(
                criterion_name,
                label,
                bounds,
                value,
                passed,
                failed,
            )

    if criteria.property_types:
        _evaluate_membership(
            "property_types",
            "property type",
            listing.property_type,
            criteria.property_types,
            passed,
            failed,
        )

    if criteria.statuses:
        _evaluate_membership(
            "statuses",
            "listing status",
            listing.status,
            criteria.statuses,
            passed,
            failed,
        )

    if criteria.max_days_on_market is not None:
        expected = {"max": criteria.max_days_on_market}
        if listing.days_on_market is None:
            failed.append(
                _reason(
                    "max_days_on_market",
                    None,
                    expected,
                    "Days on market is missing.",
                )
            )
        elif listing.days_on_market <= criteria.max_days_on_market:
            passed.append(
                _reason(
                    "max_days_on_market",
                    listing.days_on_market,
                    expected,
                    "Days on market is within the maximum.",
                )
            )
        else:
            failed.append(
                _reason(
                    "max_days_on_market",
                    listing.days_on_market,
                    expected,
                    "Days on market exceeds the maximum.",
                )
            )

    if criteria.price_drop is not None:
        _evaluate_price_drop(criteria, listing, passed, failed)

    if criteria.deal_score is not None:
        expected = {"min": criteria.deal_score.min}
        if listing.market_score is None:
            failed.append(
                _reason(
                    "deal_score",
                    None,
                    expected,
                    "Market score is unavailable.",
                )
            )
        elif listing.market_score >= criteria.deal_score.min:
            passed.append(
                _reason(
                    "deal_score",
                    listing.market_score,
                    expected,
                    "Market score meets the minimum.",
                )
            )
        else:
            failed.append(
                _reason(
                    "deal_score",
                    listing.market_score,
                    expected,
                    "Market score is below the minimum.",
                )
            )

    return CriteriaEvaluation(
        is_match=not failed,
        match_reasons=passed,
        failed_reasons=failed,
    )


def _evaluate_locations(
    criteria: SearchCriteria,
    listing: ListingCandidate,
    passed: list[EvaluationReason],
    failed: list[EvaluationReason],
) -> None:
    actual = {
        "state": listing.state,
        "county": listing.county_name,
        "city": listing.city,
        "zip": listing.zip,
    }
    expected = [
        location.model_dump(mode="json", exclude_defaults=True)
        for location in criteria.locations
    ]
    if any(_location_matches(location, listing) for location in criteria.locations):
        passed.append(
            _reason(
                "locations",
                actual,
                expected,
                "Listing is in an allowed location.",
            )
        )
    else:
        failed.append(
            _reason(
                "locations",
                actual,
                expected,
                "Listing is outside the allowed locations.",
            )
        )


def _location_matches(location: Any, listing: ListingCandidate) -> bool:
    if _casefold(listing.state) != location.state.casefold():
        return False
    if location.counties and _casefold(listing.county_name) not in {
        value.casefold() for value in location.counties
    }:
        return False
    if location.cities and _casefold(listing.city) not in {
        value.casefold() for value in location.cities
    }:
        return False
    if location.zips and listing.zip not in location.zips:
        return False
    return True


def _evaluate_range(
    criterion_name: str,
    label: str,
    bounds: NumericRange,
    actual: Decimal | None,
    passed: list[EvaluationReason],
    failed: list[EvaluationReason],
) -> None:
    expected = bounds.model_dump(mode="json", exclude_none=True)
    if actual is None:
        failed.append(
            _reason(
                criterion_name,
                None,
                expected,
                f"Listing {label} is missing.",
            )
        )
        return

    meets_min = bounds.min is None or actual >= bounds.min
    meets_max = bounds.max is None or actual <= bounds.max
    if meets_min and meets_max:
        passed.append(
            _reason(
                criterion_name,
                actual,
                expected,
                f"Listing {label} is within range.",
            )
        )
    else:
        failed.append(
            _reason(
                criterion_name,
                actual,
                expected,
                f"Listing {label} is outside the allowed range.",
            )
        )


def _evaluate_membership(
    criterion_name: str,
    label: str,
    actual: str | None,
    allowed: list[str],
    passed: list[EvaluationReason],
    failed: list[EvaluationReason],
) -> None:
    if actual is not None and actual.casefold() in {
        item.casefold() for item in allowed
    }:
        passed.append(
            _reason(
                criterion_name,
                actual,
                allowed,
                f"Listing {label} is allowed.",
            )
        )
    else:
        failed.append(
            _reason(
                criterion_name,
                actual,
                allowed,
                f"Listing {label} is not allowed.",
            )
        )


def _evaluate_price_drop(
    criteria: SearchCriteria,
    listing: ListingCandidate,
    passed: list[EvaluationReason],
    failed: list[EvaluationReason],
) -> None:
    threshold = criteria.price_drop
    if threshold is None:
        return

    expected = threshold.model_dump(mode="json", exclude_none=True)
    if (
        listing.previous_price is None
        or listing.list_price is None
        or listing.previous_price <= 0
    ):
        failed.append(
            _reason(
                "price_drop",
                {
                    "previous_price": listing.previous_price,
                    "current_price": listing.list_price,
                },
                expected,
                "A valid previous and current price are required.",
            )
        )
        return

    drop_amount = listing.previous_price - listing.list_price
    drop_percent = (drop_amount / listing.previous_price) * Decimal("100")
    actual = {
        "amount": drop_amount,
        "percent": drop_percent.quantize(Decimal("0.01")),
    }
    meets_amount = threshold.min_amount is None or drop_amount >= threshold.min_amount
    meets_percent = (
        threshold.min_percent is None or drop_percent >= threshold.min_percent
    )
    if drop_amount > 0 and meets_amount and meets_percent:
        passed.append(
            _reason(
                "price_drop",
                actual,
                expected,
                "Price drop meets all configured thresholds.",
            )
        )
    else:
        failed.append(
            _reason(
                "price_drop",
                actual,
                expected,
                "Price drop does not meet all configured thresholds.",
            )
        )


def _reason(
    criterion: str,
    actual: Any,
    expected: Any,
    message: str,
) -> EvaluationReason:
    return EvaluationReason(
        criterion=criterion,
        actual=actual,
        expected=expected,
        message=message,
    )


def _casefold(value: str | None) -> str | None:
    return value.casefold() if value is not None else None

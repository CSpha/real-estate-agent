from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from app.market.deal_score_v2 import (
    calculate_comparable_discount_component,
    calculate_listing_opportunity_component,
)


def _valuation(
    discount_pct,
    *,
    status="valued",
    confidence_score=Decimal("63.8"),
):
    return {
        "valuation_version": "comparable-valuation-v1",
        "status": status,
        "estimated_value": Decimal("100000"),
        "list_price_discount_pct": discount_pct,
        "confidence_score": confidence_score,
        "subject": {"list_price": Decimal("90000")},
    }


@pytest.mark.parametrize(
    ("discount_pct", "expected_points"),
    [
        (Decimal("-0.10"), Decimal("0.00")),
        (Decimal("0"), Decimal("0.00")),
        (Decimal("0.0451"), Decimal("6.01")),
        (Decimal("0.15"), Decimal("20.00")),
        (Decimal("0.30"), Decimal("40.00")),
        (Decimal("0.50"), Decimal("40.00")),
    ],
)
def test_comparable_discount_component_scales_to_thirty_percent(
    discount_pct,
    expected_points,
):
    component = calculate_comparable_discount_component(
        _valuation(discount_pct)
    )

    assert component["status"] == "available"
    assert component["points"] == expected_points
    assert component["max_points"] == Decimal("40.00")


@pytest.mark.parametrize(
    "valuation",
    [
        _valuation(None, status="insufficient_comparables"),
        _valuation(Decimal("0.20"), confidence_score=Decimal("39.9")),
        _valuation(None),
    ],
)
def test_comparable_discount_component_preserves_unavailable_state(valuation):
    component = calculate_comparable_discount_component(valuation)

    assert component["status"] == "unavailable"
    assert component["points"] is None


def _history(*prices_and_dates):
    return [
        {
            "id": index,
            "list_price": Decimal(str(price)),
            "snapshot_timestamp": datetime.fromisoformat(timestamp).replace(
                tzinfo=timezone.utc
            ),
        }
        for index, (price, timestamp) in enumerate(prices_and_dates, start=1)
    ]


def test_listing_opportunity_scores_reduction_depth_frequency_and_recency():
    component = calculate_listing_opportunity_component(
        _history(
            (90000, "2026-06-01T12:00:00"),
            (85000, "2026-06-20T12:00:00"),
            (78500, "2026-07-15T12:00:00"),
        ),
        as_of_date=date(2026, 7, 27),
    )

    assert component["status"] == "available"
    assert component["points"] == Decimal("12.34")
    assert component["inputs"]["cumulative_reduction_pct"] == (
        Decimal("11500") / Decimal("90000")
    )
    assert component["inputs"]["reduction_count"] == 2
    assert component["inputs"]["days_since_latest_reduction"] == 12
    assert component["inputs"]["point_contributions"] == {
        "cumulative_reduction": Decimal("7.67"),
        "reduction_frequency": Decimal("2.67"),
        "reduction_recency": Decimal("2.00"),
    }


def test_listing_opportunity_does_not_treat_price_increase_as_discount():
    component = calculate_listing_opportunity_component(
        _history(
            (100000, "2026-01-01T12:00:00"),
            (90000, "2026-02-01T12:00:00"),
            (105000, "2026-03-01T12:00:00"),
        ),
        as_of_date=date(2026, 7, 1),
    )

    assert component["inputs"]["cumulative_reduction_pct"] == Decimal("0")
    assert component["inputs"]["reduction_count"] == 1
    assert component["inputs"]["point_contributions"][
        "cumulative_reduction"
    ] == Decimal("0.00")


def test_listing_opportunity_with_no_reduction_scores_zero():
    component = calculate_listing_opportunity_component(
        _history((100000, "2026-06-01T12:00:00")),
        as_of_date=date(2026, 7, 1),
    )

    assert component["status"] == "available"
    assert component["points"] == Decimal("0.00")


def test_listing_opportunity_is_unavailable_without_price_history():
    component = calculate_listing_opportunity_component(
        [],
        as_of_date=date(2026, 7, 1),
    )

    assert component["status"] == "unavailable"
    assert component["points"] is None


def test_listing_opportunity_excludes_history_after_analysis_date():
    component = calculate_listing_opportunity_component(
        _history(
            (100000, "2026-06-01T12:00:00"),
            (80000, "2026-08-01T12:00:00"),
        ),
        as_of_date=date(2026, 7, 1),
    )

    assert component["points"] == Decimal("0.00")
    assert component["inputs"]["reduction_count"] == 0
    assert len(component["inputs"]["price_observations"]) == 1

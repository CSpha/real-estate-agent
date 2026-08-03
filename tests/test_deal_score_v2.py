from decimal import Decimal

import pytest

from app.market.deal_score_v2 import calculate_comparable_discount_component


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

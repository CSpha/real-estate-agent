from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.market.mortgage_rate_outlook import (
    MODEL_VERSION,
    generate_outlook,
)


def observations(*, weekly_change: Decimal, count: int = 53):
    latest = date(2026, 7, 23)
    return [
        {
            "product_type": "30_year_fixed",
            "observation_date": latest - timedelta(weeks=week),
            "rate_percent": Decimal("6.50") - weekly_change * week,
        }
        for week in range(count)
    ]


@pytest.mark.parametrize("horizon", [1, 3])
def test_outlook_probabilities_are_reproducible_and_sum_to_one(horizon):
    outlook = generate_outlook(
        observations(weekly_change=Decimal("0.01")),
        product_type="30_year_fixed",
        horizon_months=horizon,
    )

    assert outlook.model_version == MODEL_VERSION
    assert (
        outlook.probability_lower
        + outlook.probability_stable
        + outlook.probability_higher
        == Decimal("1")
    )
    assert outlook.probability_higher > outlook.probability_lower
    assert outlook.confidence == "medium"
    assert outlook.input_snapshot["observation_count"] == 53
    assert any("not yet included" in driver for driver in outlook.drivers)


def test_downward_momentum_increases_probability_of_lower_rates():
    outlook = generate_outlook(
        observations(weekly_change=Decimal("-0.08")),
        product_type="30_year_fixed",
        horizon_months=1,
    )

    assert outlook.probability_lower > outlook.probability_higher
    assert outlook.most_likely_direction == "lower"


def test_sparse_history_is_rejected():
    with pytest.raises(ValueError, match="at least 27"):
        generate_outlook(
            observations(weekly_change=Decimal("0.01"), count=20),
            product_type="30_year_fixed",
            horizon_months=3,
        )


def test_unsupported_horizon_is_rejected():
    with pytest.raises(ValueError, match="must be 1 or 3"):
        generate_outlook(
            observations(weekly_change=Decimal("0.01")),
            product_type="30_year_fixed",
            horizon_months=2,
        )

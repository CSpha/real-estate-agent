from decimal import Decimal

from app.market.backtest_mortgage_rate_outlook import (
    BASELINE_PROBABILITIES,
    STABLE_BAND,
    _actual_direction,
    _brier,
)


def test_actual_direction_uses_inclusive_stable_band():
    current = Decimal("6.50")

    assert _actual_direction(current, Decimal("6.75")) == "roughly_stable"
    assert _actual_direction(current, Decimal("6.76")) == "higher"
    assert _actual_direction(current, Decimal("6.25")) == "roughly_stable"
    assert _actual_direction(current, Decimal("6.24")) == "lower"
    assert STABLE_BAND == Decimal("0.25")


def test_brier_score_rewards_correct_probability():
    correct = _brier(
        (Decimal("0.05"), Decimal("0.90"), Decimal("0.05")),
        "roughly_stable",
    )
    baseline = _brier(BASELINE_PROBABILITIES, "roughly_stable")

    assert correct < baseline

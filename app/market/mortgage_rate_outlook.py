from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from statistics import pstdev
from typing import Any

from dateutil.relativedelta import relativedelta
from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.mortgage_rates import PMMS_SOURCE


MODEL_VERSION = "pmms_momentum_v1"
STABLE_BAND = Decimal("0.25")
PROBABILITY_QUANTUM = Decimal("0.00001")


@dataclass(frozen=True)
class MortgageRateOutlook:
    product_type: str
    as_of_date: date
    horizon_months: int
    target_date: date
    model_version: str
    probability_lower: Decimal
    probability_stable: Decimal
    probability_higher: Decimal
    confidence: str
    input_snapshot: dict[str, Any]
    drivers: list[str]

    @property
    def most_likely_direction(self) -> str:
        probabilities = {
            "lower": self.probability_lower,
            "roughly_stable": self.probability_stable,
            "higher": self.probability_higher,
        }
        return max(probabilities, key=probabilities.get)


def generate_outlook(
    observations: Iterable[Mapping[str, Any]],
    *,
    product_type: str,
    horizon_months: int,
) -> MortgageRateOutlook:
    if horizon_months not in (1, 3):
        raise ValueError("horizon_months must be 1 or 3")

    ordered = sorted(
        (
            (
                item["observation_date"],
                Decimal(str(item["rate_percent"])),
            )
            for item in observations
            if item["product_type"] == product_type
        ),
        reverse=True,
    )
    required = 14 if horizon_months == 1 else 27
    if len(ordered) < required:
        raise ValueError(
            f"{product_type} needs at least {required} weekly observations "
            f"for a {horizon_months}-month outlook"
        )

    as_of_date, current_rate = ordered[0]
    change_4 = current_rate - ordered[4][1]
    change_13 = current_rate - ordered[13][1]
    change_26 = current_rate - ordered[26][1] if len(ordered) > 26 else None

    if horizon_months == 1:
        momentum = (
            Decimal("0.65") * change_4
            + Decimal("0.35") * (change_13 / Decimal("3.25"))
        )
    else:
        if change_26 is None:
            raise ValueError("A 3-month outlook requires 27 observations")
        momentum = (
            Decimal("0.60") * change_13
            + Decimal("0.40") * (change_26 / Decimal("2"))
        )

    weekly_changes = [
        float(ordered[index][1] - ordered[index + 1][1])
        for index in range(min(12, len(ordered) - 1))
    ]
    recent_volatility = Decimal(str(pstdev(weekly_changes)))
    strength = _clamp(momentum / Decimal("0.50"), Decimal("-1"), Decimal("1"))
    stable_probability = max(
        Decimal("0.25"),
        Decimal("0.55") - abs(strength) * Decimal("0.30"),
    )
    directional_probability = Decimal("1") - stable_probability
    higher_share = (Decimal("1") + strength) / Decimal("2")
    higher_probability = directional_probability * higher_share
    lower_probability = directional_probability - higher_probability
    lower_probability, stable_probability, higher_probability = (
        _rounded_probabilities(
            lower_probability,
            stable_probability,
            higher_probability,
        )
    )

    confidence = "medium"
    if len(ordered) < 52 or recent_volatility > Decimal("0.20"):
        confidence = "low"

    drivers = [
        _driver("4-week mortgage-rate momentum", change_4),
        _driver("13-week mortgage-rate momentum", change_13),
        (
            _driver("26-week mortgage-rate momentum", change_26)
            if change_26 is not None
            else "26-week mortgage-rate momentum unavailable"
        ),
        f"Recent weekly volatility is {recent_volatility:.3f} percentage points",
        (
            "Only mortgage-rate history is available in model v1; "
            "Treasury, inflation, and policy-expectation signals are not yet included"
        ),
    ]
    input_snapshot = {
        "source": PMMS_SOURCE,
        "current_rate": str(current_rate),
        "change_4_weeks": str(change_4),
        "change_13_weeks": str(change_13),
        "change_26_weeks": str(change_26) if change_26 is not None else None,
        "recent_weekly_volatility": str(recent_volatility),
        "momentum": str(momentum),
        "stable_band_percentage_points": str(STABLE_BAND),
        "observation_count": len(ordered),
    }

    return MortgageRateOutlook(
        product_type=product_type,
        as_of_date=as_of_date,
        horizon_months=horizon_months,
        target_date=as_of_date + relativedelta(months=horizon_months),
        model_version=MODEL_VERSION,
        probability_lower=lower_probability,
        probability_stable=stable_probability,
        probability_higher=higher_probability,
        confidence=confidence,
        input_snapshot=input_snapshot,
        drivers=drivers,
    )


def generate_and_store_outlooks(
    *,
    product_type: str = "30_year_fixed",
    engine: Engine | None = None,
) -> list[MortgageRateOutlook]:
    engine = engine or get_engine()
    with engine.connect() as conn:
        observations = conn.execute(
            text(
                """
                SELECT product_type, observation_date, rate_percent
                FROM mortgage_rate_observations
                WHERE source = :source
                  AND product_type = :product_type
                ORDER BY observation_date DESC
                LIMIT 53
                """
            ),
            {"source": PMMS_SOURCE, "product_type": product_type},
        ).mappings().all()

    outlooks = [
        generate_outlook(
            observations,
            product_type=product_type,
            horizon_months=horizon,
        )
        for horizon in (1, 3)
    ]
    with engine.begin() as conn:
        for outlook in outlooks:
            conn.execute(
                text(
                    """
                    INSERT INTO mortgage_rate_outlooks (
                        product_type,
                        as_of_date,
                        horizon_months,
                        target_date,
                        model_version,
                        probability_lower,
                        probability_stable,
                        probability_higher,
                        confidence,
                        input_snapshot,
                        drivers
                    )
                    VALUES (
                        :product_type,
                        :as_of_date,
                        :horizon_months,
                        :target_date,
                        :model_version,
                        :probability_lower,
                        :probability_stable,
                        :probability_higher,
                        :confidence,
                        CAST(:input_snapshot AS JSONB),
                        CAST(:drivers AS JSONB)
                    )
                    ON CONFLICT (
                        product_type,
                        as_of_date,
                        horizon_months,
                        model_version
                    )
                    DO NOTHING
                    """
                ),
                {
                    **asdict(outlook),
                    "input_snapshot": json.dumps(outlook.input_snapshot),
                    "drivers": json.dumps(outlook.drivers),
                },
            )
    return outlooks


def _rounded_probabilities(
    lower: Decimal,
    stable: Decimal,
    higher: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    lower = lower.quantize(PROBABILITY_QUANTUM, rounding=ROUND_HALF_UP)
    stable = stable.quantize(PROBABILITY_QUANTUM, rounding=ROUND_HALF_UP)
    higher = Decimal("1") - lower - stable
    return lower, stable, higher


def _clamp(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return max(minimum, min(value, maximum))


def _driver(label: str, change: Decimal) -> str:
    if change > 0:
        direction = "upward"
    elif change < 0:
        direction = "downward"
    else:
        direction = "neutral"
    return f"{label} is {direction} ({change:+.3f} percentage points)"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate versioned 1- and 3-month mortgage-rate outlooks.",
    )
    parser.add_argument(
        "--product",
        default="30_year_fixed",
        choices=("30_year_fixed", "15_year_fixed"),
    )
    args = parser.parse_args()
    outlooks = generate_and_store_outlooks(product_type=args.product)
    for outlook in outlooks:
        print(
            f"{outlook.horizon_months}-month {outlook.product_type} outlook "
            f"as of {outlook.as_of_date}: "
            f"lower {outlook.probability_lower:.1%}, "
            f"stable {outlook.probability_stable:.1%}, "
            f"higher {outlook.probability_higher:.1%}; "
            f"most likely {outlook.most_likely_direction}; "
            f"confidence {outlook.confidence}."
        )


if __name__ == "__main__":
    main()

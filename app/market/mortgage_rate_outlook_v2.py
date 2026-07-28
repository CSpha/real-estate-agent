from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from dateutil.relativedelta import relativedelta
from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.mortgage_rate_outlook import (
    MortgageRateOutlook,
    generate_outlook,
)
from app.market.mortgage_rates import PMMS_SOURCE


MODEL_VERSION = "multi_signal_v2"
PROBABILITY_QUANTUM = Decimal("0.00001")


def generate_multi_signal_outlooks(
    *,
    product_type: str = "30_year_fixed",
    engine: Engine | None = None,
) -> list[MortgageRateOutlook]:
    engine = engine or get_engine()
    with engine.connect() as conn:
        as_of_date = conn.scalar(
            text(
                """
                SELECT MAX(observation_date)
                FROM mortgage_rate_observations
                WHERE source = :source
                  AND product_type = :product_type
                """
            ),
            {"source": PMMS_SOURCE, "product_type": product_type},
        )
    if as_of_date is None:
        raise ValueError(f"No mortgage-rate observations for {product_type}")
    return generate_multi_signal_outlooks_as_of(
        as_of_date,
        product_type=product_type,
        engine=engine,
        persist=True,
    )


def generate_multi_signal_outlooks_as_of(
    as_of_date: date,
    *,
    product_type: str = "30_year_fixed",
    engine: Engine | None = None,
    persist: bool = False,
) -> list[MortgageRateOutlook]:
    engine = engine or get_engine()
    with engine.connect() as conn:
        mortgage_rows = conn.execute(
            text(
                """
                SELECT product_type, observation_date, rate_percent
                FROM mortgage_rate_observations
                WHERE source = :source
                  AND product_type = :product_type
                  AND observation_date <= :as_of_date
                ORDER BY observation_date DESC
                LIMIT 53
                """
            ),
            {
                "source": PMMS_SOURCE,
                "product_type": product_type,
                "as_of_date": as_of_date,
            },
        ).mappings().all()
        if not mortgage_rows:
            raise ValueError(f"No mortgage-rate observations for {product_type}")
        actual_as_of_date = mortgage_rows[0]["observation_date"]
        signals = _load_signal_snapshot(conn, actual_as_of_date)
    outlooks = [
        _combine_signals(
            generate_outlook(
                mortgage_rows,
                product_type=product_type,
                horizon_months=horizon,
            ),
            signals,
        )
        for horizon in (1, 3)
    ]
    if persist:
        _store_outlooks(outlooks, engine)
    return outlooks


def _load_signal_snapshot(conn: Any, as_of_date: date) -> dict[str, Any]:
    series = {}
    for series_id, limit in (("DGS2", 65), ("DGS10", 65), ("DFF", 5), ("CPIAUCSL", 16)):
        series[series_id] = conn.execute(
            text(
                """
                SELECT observation_date, value
                FROM macro_rate_observations
                WHERE series_id = :series_id
                  AND observation_date <= :as_of_date
                ORDER BY observation_date DESC
                LIMIT :limit
                """
            ),
            {
                "series_id": series_id,
                "as_of_date": as_of_date,
                "limit": limit,
            },
        ).mappings().all()

    latest_release = conn.scalar(
        text(
            """
            SELECT MAX(release_date)
            FROM fed_expectation_observations
            WHERE subject = 'fed_funds_target_range'
              AND aggregation = 'pctl50'
              AND release_date <= :as_of_date
            """
        ),
        {"as_of_date": as_of_date},
    )
    expectations = []
    if latest_release is not None:
        expectations = conn.execute(
            text(
                """
                SELECT
                    release_date,
                    horizon_date,
                    value_percent,
                    respondent_count,
                    value_tag
                FROM fed_expectation_observations
                WHERE subject = 'fed_funds_target_range'
                  AND aggregation = 'pctl50'
                  AND release_date = :release_date
                  AND horizon_date >= :as_of_date
                ORDER BY horizon_date
                """
            ),
            {"release_date": latest_release, "as_of_date": as_of_date},
        ).mappings().all()
    return {
        "as_of_date": as_of_date,
        "series": series,
        "expectations": expectations,
    }


def _combine_signals(
    baseline: MortgageRateOutlook,
    signals: dict[str, Any],
) -> MortgageRateOutlook:
    horizon = baseline.horizon_months
    base_strength = _clamp(
        Decimal(baseline.input_snapshot["momentum"]) / Decimal("0.50")
    )
    treasury_rows = signals["series"]["DGS10"]
    treasury_offset = 20 if horizon == 1 else 60
    treasury_change = _series_change(treasury_rows, treasury_offset)
    treasury_strength = (
        _clamp(treasury_change / Decimal("0.50"))
        if treasury_change is not None
        else Decimal("0")
    )

    cpi_rows = signals["series"]["CPIAUCSL"]
    inflation_change = _inflation_trend(cpi_rows)
    inflation_strength = (
        _clamp(inflation_change / Decimal("1.00"))
        if inflation_change is not None
        else Decimal("0")
    )

    dff_rows = signals["series"]["DFF"]
    current_fed_funds = (
        Decimal(str(dff_rows[0]["value"])) if dff_rows else None
    )
    target_date = baseline.as_of_date + relativedelta(months=horizon)
    expectation = _nearest_expectation(signals["expectations"], target_date)
    policy_delta = (
        Decimal(str(expectation["value_percent"])) - current_fed_funds
        if expectation is not None and current_fed_funds is not None
        else None
    )
    policy_strength = (
        _clamp(policy_delta / Decimal("1.00"))
        if policy_delta is not None
        else Decimal("0")
    )

    combined_strength = _clamp(
        Decimal("0.40") * base_strength
        + Decimal("0.30") * treasury_strength
        + Decimal("0.15") * inflation_strength
        + Decimal("0.15") * policy_strength
    )
    lower, stable, higher = _probabilities(combined_strength)
    missing = [
        label
        for label, value in (
            ("10-year Treasury momentum", treasury_change),
            ("CPI trend", inflation_change),
            ("Fed policy expectation", policy_delta),
        )
        if value is None
    ]
    confidence = "medium" if not missing else "low"
    drivers = [
        *baseline.drivers[:3],
        _driver("10-year Treasury momentum", treasury_change),
        _driver("three-month change in CPI inflation", inflation_change),
        _driver("expected fed-funds change", policy_delta),
    ]
    if missing:
        drivers.append("Missing signals lowered confidence: " + ", ".join(missing))
    else:
        drivers.append(
            "All v2 signal families were available; confidence remains capped "
            "at medium until forecast calibration is established"
        )

    input_snapshot = {
        **baseline.input_snapshot,
        "baseline_model_version": baseline.model_version,
        "baseline_strength": str(base_strength),
        "treasury_10y_change": (
            str(treasury_change) if treasury_change is not None else None
        ),
        "treasury_strength": str(treasury_strength),
        "inflation_yoy_change_3_months": (
            str(inflation_change) if inflation_change is not None else None
        ),
        "inflation_strength": str(inflation_strength),
        "effective_fed_funds_rate": (
            str(current_fed_funds) if current_fed_funds is not None else None
        ),
        "expected_fed_funds_rate": (
            str(expectation["value_percent"]) if expectation is not None else None
        ),
        "expectation_release_date": (
            expectation["release_date"].isoformat()
            if expectation is not None
            else None
        ),
        "expectation_horizon_date": (
            expectation["horizon_date"].isoformat()
            if expectation is not None
            else None
        ),
        "policy_strength": str(policy_strength),
        "combined_strength": str(combined_strength),
        "weights": {
            "mortgage_momentum": "0.40",
            "treasury_10y_momentum": "0.30",
            "inflation_trend": "0.15",
            "fed_expectations": "0.15",
        },
    }
    return replace(
        baseline,
        model_version=MODEL_VERSION,
        probability_lower=lower,
        probability_stable=stable,
        probability_higher=higher,
        confidence=confidence,
        input_snapshot=input_snapshot,
        drivers=drivers,
    )


def _store_outlooks(
    outlooks: list[MortgageRateOutlook],
    engine: Engine,
) -> None:
    with engine.begin() as conn:
        for outlook in outlooks:
            values = asdict(outlook)
            values["input_snapshot"] = json.dumps(outlook.input_snapshot)
            values["drivers"] = json.dumps(outlook.drivers)
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
                values,
            )


def _series_change(
    rows: list[Mapping[str, Any]],
    offset: int,
) -> Decimal | None:
    if len(rows) <= offset:
        return None
    return Decimal(str(rows[0]["value"])) - Decimal(str(rows[offset]["value"]))


def _inflation_trend(rows: list[Mapping[str, Any]]) -> Decimal | None:
    if len(rows) < 16:
        return None
    current_yoy = (
        Decimal(str(rows[0]["value"])) / Decimal(str(rows[12]["value"]))
        - Decimal("1")
    ) * Decimal("100")
    prior_yoy = (
        Decimal(str(rows[3]["value"])) / Decimal(str(rows[15]["value"]))
        - Decimal("1")
    ) * Decimal("100")
    return current_yoy - prior_yoy


def _nearest_expectation(
    expectations: list[Mapping[str, Any]],
    target_date: date,
) -> Mapping[str, Any] | None:
    if not expectations:
        return None
    return min(
        expectations,
        key=lambda item: abs((item["horizon_date"] - target_date).days),
    )


def _probabilities(
    strength: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    stable = max(
        Decimal("0.25"),
        Decimal("0.55") - abs(strength) * Decimal("0.30"),
    )
    directional = Decimal("1") - stable
    higher = directional * (Decimal("1") + strength) / Decimal("2")
    lower = directional - higher
    lower = lower.quantize(PROBABILITY_QUANTUM, rounding=ROUND_HALF_UP)
    stable = stable.quantize(PROBABILITY_QUANTUM, rounding=ROUND_HALF_UP)
    return lower, stable, Decimal("1") - lower - stable


def _clamp(value: Decimal) -> Decimal:
    return max(Decimal("-1"), min(value, Decimal("1")))


def _driver(label: str, value: Decimal | None) -> str:
    if value is None:
        return f"{label} is unavailable"
    direction = "upward" if value > 0 else "downward" if value < 0 else "neutral"
    return f"{label} is {direction} ({value:+.3f} percentage points)"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate multi-signal mortgage-rate outlooks.",
    )
    parser.add_argument(
        "--product",
        default="30_year_fixed",
        choices=("30_year_fixed", "15_year_fixed"),
    )
    args = parser.parse_args()
    outlooks = generate_multi_signal_outlooks(product_type=args.product)
    for outlook in outlooks:
        print(
            f"{outlook.horizon_months}-month {outlook.product_type} "
            f"{outlook.model_version} outlook: "
            f"lower {outlook.probability_lower:.1%}, "
            f"stable {outlook.probability_stable:.1%}, "
            f"higher {outlook.probability_higher:.1%}; "
            f"most likely {outlook.most_likely_direction}; "
            f"confidence {outlook.confidence}."
        )


if __name__ == "__main__":
    main()

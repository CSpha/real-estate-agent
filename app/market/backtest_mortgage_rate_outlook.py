from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from dateutil.relativedelta import relativedelta
from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.mortgage_rate_outlook_v2 import (
    MODEL_VERSION,
    _probabilities,
    generate_multi_signal_outlooks_as_of,
)
from app.market.mortgage_rates import PMMS_SOURCE


CALIBRATION_VERSION = "strength_scale_v1"
STABLE_BAND = Decimal("0.25")
TRAINING_FRACTION = Decimal("0.70")
ORIGIN_SPACING_WEEKS = 4
SCALE_CANDIDATES = [
    Decimal(value) / Decimal("10")
    for value in range(5, 21)
]
BASELINE_PROBABILITIES = (
    Decimal("0.225"),
    Decimal("0.550"),
    Decimal("0.225"),
)


@dataclass
class BacktestPrediction:
    horizon_months: int
    split: str
    origin_date: date
    target_date: date
    current_rate: Decimal
    actual_rate: Decimal
    actual_direction: str
    probability_lower: Decimal
    probability_stable: Decimal
    probability_higher: Decimal
    combined_strength: Decimal
    raw_brier_score: Decimal
    input_snapshot: dict[str, Any]
    calibrated_brier_score: Decimal | None = None


@dataclass(frozen=True)
class CalibrationResult:
    horizon_months: int
    strength_scale: Decimal
    training_count: int
    holdout_count: int
    training_brier_score: Decimal
    holdout_raw_brier_score: Decimal
    holdout_calibrated_brier_score: Decimal
    holdout_baseline_brier_score: Decimal
    approved: bool
    approval_reason: str


def run_backtest(
    *,
    product_type: str = "30_year_fixed",
    engine: Engine | None = None,
    persist: bool = True,
) -> tuple[list[BacktestPrediction], list[CalibrationResult]]:
    engine = engine or get_engine()
    with engine.connect() as conn:
        rate_rows = conn.execute(
            text(
                """
                SELECT observation_date, rate_percent
                FROM mortgage_rate_observations
                WHERE source = :source
                  AND product_type = :product_type
                ORDER BY observation_date
                """
            ),
            {"source": PMMS_SOURCE, "product_type": product_type},
        ).mappings().all()
        macro_start = conn.scalar(
            text(
                """
                SELECT MAX(first_date)
                FROM (
                    SELECT MIN(observation_date) AS first_date
                    FROM macro_rate_observations
                    WHERE series_id IN ('DGS10', 'DFF')
                    GROUP BY series_id
                ) starts
                """
            )
        )
        expectation_start = conn.scalar(
            text("SELECT MIN(release_date) FROM fed_expectation_observations")
        )

    if not rate_rows or macro_start is None or expectation_start is None:
        raise ValueError("Mortgage, macro, and Fed-expectation history are required")

    latest_rate_date = rate_rows[-1]["observation_date"]
    start_date = max(
        max(macro_start, expectation_start) + relativedelta(months=3),
        rate_rows[0]["observation_date"] + relativedelta(weeks=27),
    )
    end_date = latest_rate_date - relativedelta(months=3)
    if start_date >= end_date:
        raise ValueError("Not enough completed history for a 3-month backtest")

    candidate_origins = [
        row["observation_date"]
        for row in rate_rows
        if start_date <= row["observation_date"] <= end_date
    ][::ORIGIN_SPACING_WEEKS]
    rate_lookup = [
        (row["observation_date"], Decimal(str(row["rate_percent"])))
        for row in rate_rows
    ]
    predictions: list[BacktestPrediction] = []
    for origin_date in candidate_origins:
        outlooks = generate_multi_signal_outlooks_as_of(
            origin_date,
            product_type=product_type,
            engine=engine,
            persist=False,
        )
        for outlook in outlooks:
            if outlook.confidence != "medium":
                continue
            actual_date, actual_rate = _nearest_rate(
                rate_lookup,
                outlook.target_date,
            )
            if abs((actual_date - outlook.target_date).days) > 10:
                continue
            current_rate = Decimal(outlook.input_snapshot["current_rate"])
            actual_direction = _actual_direction(
                current_rate,
                actual_rate,
            )
            probabilities = (
                outlook.probability_lower,
                outlook.probability_stable,
                outlook.probability_higher,
            )
            predictions.append(
                BacktestPrediction(
                    horizon_months=outlook.horizon_months,
                    split="",
                    origin_date=outlook.as_of_date,
                    target_date=outlook.target_date,
                    current_rate=current_rate,
                    actual_rate=actual_rate,
                    actual_direction=actual_direction,
                    probability_lower=probabilities[0],
                    probability_stable=probabilities[1],
                    probability_higher=probabilities[2],
                    combined_strength=Decimal(
                        outlook.input_snapshot["combined_strength"]
                    ),
                    raw_brier_score=_brier(
                        probabilities,
                        actual_direction,
                    ),
                    input_snapshot=outlook.input_snapshot,
                )
            )

    calibrations = []
    for horizon in (1, 3):
        horizon_rows = sorted(
            (
                row
                for row in predictions
                if row.horizon_months == horizon
            ),
            key=lambda row: row.origin_date,
        )
        if len(horizon_rows) < 10:
            raise ValueError(
                f"Only {len(horizon_rows)} completed {horizon}-month forecasts"
            )
        split_index = max(
            1,
            int(len(horizon_rows) * float(TRAINING_FRACTION)),
        )
        training = horizon_rows[:split_index]
        holdout = horizon_rows[split_index:]
        for row in training:
            row.split = "training"
        for row in holdout:
            row.split = "holdout"

        scale = min(
            SCALE_CANDIDATES,
            key=lambda candidate: _mean(
                _brier(
                    _probabilities_for_scale(row, candidate),
                    row.actual_direction,
                )
                for row in training
            ),
        )
        for row in horizon_rows:
            row.calibrated_brier_score = _brier(
                _probabilities_for_scale(row, scale),
                row.actual_direction,
            )

        holdout_raw = _mean(row.raw_brier_score for row in holdout)
        holdout_calibrated = _mean(
            row.calibrated_brier_score
            for row in holdout
            if row.calibrated_brier_score is not None
        )
        holdout_baseline = _mean(
            _brier(BASELINE_PROBABILITIES, row.actual_direction)
            for row in holdout
        )
        approved = (
            len(training) >= 20
            and len(holdout) >= 8
            and holdout_calibrated < holdout_raw
            and holdout_calibrated < holdout_baseline
        )
        reasons = []
        if len(training) < 20 or len(holdout) < 8:
            reasons.append("insufficient chronological sample")
        if holdout_calibrated >= holdout_raw:
            reasons.append("calibration did not improve raw holdout Brier score")
        if holdout_calibrated >= holdout_baseline:
            reasons.append("calibration did not beat stable-rate baseline")
        calibrations.append(
            CalibrationResult(
                horizon_months=horizon,
                strength_scale=scale,
                training_count=len(training),
                holdout_count=len(holdout),
                training_brier_score=_mean(
                    row.calibrated_brier_score
                    for row in training
                    if row.calibrated_brier_score is not None
                ),
                holdout_raw_brier_score=holdout_raw,
                holdout_calibrated_brier_score=holdout_calibrated,
                holdout_baseline_brier_score=holdout_baseline,
                approved=approved,
                approval_reason=(
                    "approved on chronological holdout"
                    if approved
                    else "; ".join(reasons)
                ),
            )
        )

    if persist:
        _persist_results(
            predictions,
            calibrations,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            engine=engine,
        )
    return predictions, calibrations


def _persist_results(
    predictions: list[BacktestPrediction],
    calibrations: list[CalibrationResult],
    *,
    product_type: str,
    start_date: date,
    end_date: date,
    engine: Engine,
) -> None:
    with engine.begin() as conn:
        run_id = conn.scalar(
            text(
                """
                INSERT INTO mortgage_rate_backtest_runs (
                    product_type,
                    model_version,
                    start_date,
                    end_date,
                    origin_spacing_weeks,
                    stable_band,
                    training_fraction,
                    configuration
                )
                VALUES (
                    :product_type,
                    :model_version,
                    :start_date,
                    :end_date,
                    :origin_spacing_weeks,
                    :stable_band,
                    :training_fraction,
                    CAST(:configuration AS JSONB)
                )
                RETURNING id
                """
            ),
            {
                "product_type": product_type,
                "model_version": MODEL_VERSION,
                "start_date": start_date,
                "end_date": end_date,
                "origin_spacing_weeks": ORIGIN_SPACING_WEEKS,
                "stable_band": STABLE_BAND,
                "training_fraction": TRAINING_FRACTION,
                "configuration": json.dumps(
                    {
                        "scale_candidates": [
                            str(value) for value in SCALE_CANDIDATES
                        ],
                        "baseline_probabilities": [
                            str(value) for value in BASELINE_PROBABILITIES
                        ],
                    }
                ),
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO mortgage_rate_backtest_predictions (
                    run_id,
                    horizon_months,
                    split,
                    origin_date,
                    target_date,
                    current_rate,
                    actual_rate,
                    actual_direction,
                    probability_lower,
                    probability_stable,
                    probability_higher,
                    combined_strength,
                    raw_brier_score,
                    calibrated_brier_score,
                    input_snapshot
                )
                VALUES (
                    :run_id,
                    :horizon_months,
                    :split,
                    :origin_date,
                    :target_date,
                    :current_rate,
                    :actual_rate,
                    :actual_direction,
                    :probability_lower,
                    :probability_stable,
                    :probability_higher,
                    :combined_strength,
                    :raw_brier_score,
                    :calibrated_brier_score,
                    CAST(:input_snapshot AS JSONB)
                )
                """
            ),
            [
                {
                    **row.__dict__,
                    "run_id": run_id,
                    "input_snapshot": json.dumps(row.input_snapshot),
                }
                for row in predictions
            ],
        )
        conn.execute(
            text(
                """
                INSERT INTO mortgage_rate_calibrations (
                    run_id,
                    horizon_months,
                    calibration_version,
                    strength_scale,
                    training_count,
                    holdout_count,
                    training_brier_score,
                    holdout_raw_brier_score,
                    holdout_calibrated_brier_score,
                    holdout_baseline_brier_score,
                    approved,
                    approval_reason
                )
                VALUES (
                    :run_id,
                    :horizon_months,
                    :calibration_version,
                    :strength_scale,
                    :training_count,
                    :holdout_count,
                    :training_brier_score,
                    :holdout_raw_brier_score,
                    :holdout_calibrated_brier_score,
                    :holdout_baseline_brier_score,
                    :approved,
                    :approval_reason
                )
                """
            ),
            [
                {
                    **row.__dict__,
                    "run_id": run_id,
                    "calibration_version": CALIBRATION_VERSION,
                }
                for row in calibrations
            ],
        )


def _probabilities_for_scale(
    row: BacktestPrediction,
    scale: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    strength = max(
        Decimal("-1"),
        min(row.combined_strength * scale, Decimal("1")),
    )
    return _probabilities(strength)


def _actual_direction(
    current_rate: Decimal,
    actual_rate: Decimal,
) -> str:
    change = actual_rate - current_rate
    if change > STABLE_BAND:
        return "higher"
    if change < -STABLE_BAND:
        return "lower"
    return "roughly_stable"


def _brier(
    probabilities: tuple[Decimal, Decimal, Decimal],
    actual_direction: str,
) -> Decimal:
    actual = {
        "lower": (Decimal("1"), Decimal("0"), Decimal("0")),
        "roughly_stable": (Decimal("0"), Decimal("1"), Decimal("0")),
        "higher": (Decimal("0"), Decimal("0"), Decimal("1")),
    }[actual_direction]
    return sum(
        (probability - outcome) ** 2
        for probability, outcome in zip(probabilities, actual, strict=True)
    )


def _mean(values: Any) -> Decimal:
    values = list(values)
    if not values:
        raise ValueError("Cannot average an empty collection")
    return sum(values, Decimal("0")) / len(values)


def _nearest_rate(
    rates: list[tuple[date, Decimal]],
    target_date: date,
) -> tuple[date, Decimal]:
    return min(rates, key=lambda item: abs((item[0] - target_date).days))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backtest and calibrate multi-signal mortgage-rate outlooks.",
    )
    parser.add_argument(
        "--product",
        default="30_year_fixed",
        choices=("30_year_fixed", "15_year_fixed"),
    )
    args = parser.parse_args()
    predictions, calibrations = run_backtest(product_type=args.product)
    print(f"Persisted {len(predictions)} completed forecast outcomes.")
    for result in calibrations:
        print(
            f"{result.horizon_months}-month: scale {result.strength_scale}, "
            f"training n={result.training_count}, "
            f"holdout n={result.holdout_count}, "
            f"raw Brier={result.holdout_raw_brier_score:.4f}, "
            f"calibrated={result.holdout_calibrated_brier_score:.4f}, "
            f"baseline={result.holdout_baseline_brier_score:.4f}, "
            f"approved={result.approved} ({result.approval_reason})."
        )


if __name__ == "__main__":
    main()

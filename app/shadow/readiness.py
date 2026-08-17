from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import asdict
from datetime import date
from sqlalchemy import Engine, text

from app.db import get_engine


READINESS_SQL = text(
    """
    WITH successful AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY started_at::date
                   ORDER BY started_at DESC, id DESC
               ) AS daily_rank
        FROM shadow_pipeline_runs
        WHERE source = :source
          AND status = 'succeeded'
    ),
    recent_dates AS (
        SELECT *
        FROM successful
        WHERE daily_rank = 1
        ORDER BY started_at DESC, id DESC
        LIMIT 3
    )
    SELECT
        (SELECT COUNT(*) FROM successful) AS successful_runs,
        (SELECT COUNT(*) FROM recent_dates) AS distinct_run_dates,
        (SELECT MIN(started_at::date) FROM recent_dates) AS first_success_date,
        (SELECT MAX(started_at::date) FROM recent_dates) AS last_success_date,
        COALESCE(
            (SELECT MAX(alert_eligible_count) FROM successful), 0
        ) AS max_alert_eligible_count,
        COALESCE(
            (SELECT MIN(
                supported_valuation_count::numeric
                / NULLIF(comparable_ready_count, 0)
            ) FROM recent_dates), 0
        ) AS min_supported_coverage
    """
)


@dataclass(frozen=True)
class ShadowReadiness:
    ready: bool
    successful_runs: int
    distinct_run_dates: int
    observation_days: int
    minimum_supported_valuation_coverage: float
    checks: dict[str, bool]
    blockers: tuple[str, ...]


def assess_shadow_readiness(
    *, source: str = "rentcast", engine: Engine | None = None
) -> ShadowReadiness:
    engine = engine or get_engine()
    with engine.connect() as connection:
        row = dict(connection.execute(
            READINESS_SQL, {"source": source}
        ).mappings().one())
    first_date: date | None = row["first_success_date"]
    last_date: date | None = row["last_success_date"]
    observation_days = (
        (last_date - first_date).days
        if first_date is not None and last_date is not None
        else 0
    )
    coverage = float(row["min_supported_coverage"] or 0)
    checks = {
        "three_successful_run_dates": int(row["distinct_run_dates"] or 0) >= 3,
        "fourteen_day_observation_window": observation_days >= 14,
        "no_shadow_listing_alert_eligible": int(
            row["max_alert_eligible_count"] or 0
        ) == 0,
        "supported_valuation_coverage_at_least_70pct": coverage >= 0.70,
    }
    blockers = tuple(name for name, passed in checks.items() if not passed)
    return ShadowReadiness(
        ready=not blockers,
        successful_runs=int(row["successful_runs"] or 0),
        distinct_run_dates=int(row["distinct_run_dates"] or 0),
        observation_days=observation_days,
        minimum_supported_valuation_coverage=round(coverage, 4),
        checks=checks,
        blockers=blockers,
    )


def main() -> None:
    result = assess_shadow_readiness()
    print("Shadow promotion readiness:")
    print(json.dumps(asdict(result), indent=2))


if __name__ == "__main__":
    main()

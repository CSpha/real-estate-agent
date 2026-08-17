from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from app.db import get_engine
from app.ingest.load_redfin_county_sales import load_redfin_county_sales
from app.ingest.load_wayne_county_comparable_sales import (
    import_wayne_county_comparable_sales,
)
from app.providers.sync_rentcast_shadow import sync_rentcast_shadow
from app.reports.shadow_scoring_review import build_shadow_scoring_review
from app.shadow.readiness import assess_shadow_readiness
from app.transforms.backfill_comparable_valuations import (
    backfill_comparable_valuations,
)


START_RUN_SQL = text(
    """
    INSERT INTO shadow_pipeline_runs (source, status)
    VALUES (:source, 'running')
    RETURNING id
    """
)
FINISH_RUN_SQL = text(
    """
    UPDATE shadow_pipeline_runs
    SET status = :status,
        finished_at = CURRENT_TIMESTAMP,
        listing_count = :listing_count,
        scoring_eligible_count = :scoring_eligible_count,
        comparable_ready_count = :comparable_ready_count,
        supported_valuation_count = :supported_valuation_count,
        alert_eligible_count = :alert_eligible_count,
        error_message = :error_message,
        details_json = CAST(:details_json AS JSONB)
    WHERE id = :run_id
    """
)
METRICS_SQL = text(
    """
    SELECT
        COUNT(*) AS listing_count,
        COUNT(*) FILTER (WHERE scoring_eligible) AS scoring_eligible_count,
        COUNT(*) FILTER (
            WHERE scoring_eligible AND sqft IS NOT NULL
        ) AS comparable_ready_count,
        COUNT(*) FILTER (WHERE alert_eligible) AS alert_eligible_count,
        COUNT(*) FILTER (
            WHERE scoring_eligible
              AND sqft IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM listing_comparable_valuations valuation
                  WHERE valuation.source = listing.source
                    AND valuation.source_listing_id = listing.source_listing_id
                    AND valuation.as_of_date = :as_of_date
                    AND valuation.status = 'valued'
              )
        ) AS supported_valuation_count
    FROM listings_current listing
    WHERE source = :source
    """
)


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime, Decimal, Path)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def run_shadow_pipeline(
    *,
    source: str = "rentcast",
    as_of_date: date | None = None,
    output_path: Path | None = None,
    skip_redfin: bool = False,
    skip_rentcast: bool = False,
    skip_comparables: bool = False,
    engine: Engine | None = None,
) -> dict[str, Any]:
    """Run data refresh, valuation, and review with no alert code path."""

    engine = engine or get_engine()
    as_of_date = as_of_date or date.today()
    with engine.begin() as connection:
        run_id = connection.execute(
            START_RUN_SQL, {"source": source}
        ).scalar_one()

    details: dict[str, Any] = {
        "as_of_date": as_of_date,
        "alerts_enabled": False,
    }
    metrics = {
        "listing_count": None,
        "scoring_eligible_count": None,
        "comparable_ready_count": None,
        "supported_valuation_count": None,
        "alert_eligible_count": None,
    }
    try:
        if not skip_redfin:
            details["redfin"] = asdict(load_redfin_county_sales(engine=engine))
        if not skip_rentcast:
            details["rentcast"] = asdict(sync_rentcast_shadow(engine=engine))
        if not skip_comparables:
            details["wayne_comparables"] = asdict(
                import_wayne_county_comparable_sales(
                    as_of_date=as_of_date,
                    engine=engine,
                )
            )
        details["valuations"] = backfill_comparable_valuations(
            source=source,
            as_of_date=as_of_date,
            engine=engine,
        )
        report_args: dict[str, Any] = {"source": source, "engine": engine}
        if output_path is not None:
            report_args["output_path"] = output_path
        details["review"] = build_shadow_scoring_review(**report_args)
        with engine.connect() as connection:
            metrics = dict(connection.execute(
                METRICS_SQL,
                {"source": source, "as_of_date": as_of_date},
            ).mappings().one())
        with engine.begin() as connection:
            connection.execute(
                FINISH_RUN_SQL,
                {
                    "run_id": run_id,
                    "status": "succeeded",
                    "error_message": None,
                    "details_json": json.dumps(
                        details, default=_json_default, sort_keys=True
                    ),
                    **metrics,
                },
            )
    except Exception as exc:
        with engine.begin() as connection:
            connection.execute(
                FINISH_RUN_SQL,
                {
                    "run_id": run_id,
                    "status": "failed",
                    "error_message": f"{type(exc).__name__}: {exc}",
                    "details_json": json.dumps(
                        details, default=_json_default, sort_keys=True
                    ),
                    **metrics,
                },
            )
        raise

    readiness = assess_shadow_readiness(source=source, engine=engine)
    result = {
        "run_id": run_id,
        "status": "succeeded",
        **metrics,
        "readiness": asdict(readiness),
        "alerts_queued": 0,
        "alerts_sent": 0,
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the dedicated alert-isolated shadow data pipeline."
    )
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--skip-redfin", action="store_true")
    parser.add_argument("--skip-rentcast", action="store_true")
    parser.add_argument("--skip-comparables", action="store_true")
    args = parser.parse_args()
    result = run_shadow_pipeline(
        as_of_date=args.as_of_date,
        output_path=args.output,
        skip_redfin=args.skip_redfin,
        skip_rentcast=args.skip_rentcast,
        skip_comparables=args.skip_comparables,
    )
    print("Shadow pipeline complete (alerts disabled):")
    print(json.dumps(result, default=_json_default, indent=2))


if __name__ == "__main__":
    main()

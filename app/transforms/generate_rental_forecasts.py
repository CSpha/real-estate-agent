from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal

from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.rental_forecast import FORECAST_VERSION, SCENARIOS, forecast_rental_scenario


ANALYSES_SQL = text("""
    SELECT DISTINCT ON (estimate.source, estimate.source_listing_id)
        analysis.id, analysis.monthly_rent, analysis.monthly_cash_flow
    FROM rental_underwriting_analyses analysis
    JOIN listing_rent_estimates estimate ON estimate.id = analysis.rent_estimate_id
    ORDER BY estimate.source, estimate.source_listing_id, analysis.created_at DESC, analysis.id DESC
""")
INSERT_SQL = text("""
    INSERT INTO rental_forecast_scenarios (
        rental_analysis_id, forecast_version, scenario_name, annual_rent_growth_pct,
        horizon_years, input_fingerprint, projection_json
    ) VALUES (
        :rental_analysis_id, :forecast_version, :scenario_name, :annual_rent_growth_pct,
        :horizon_years, :input_fingerprint, CAST(:projection_json AS JSONB)
    ) ON CONFLICT ON CONSTRAINT uq_rental_forecast_input DO NOTHING
""")


@dataclass(frozen=True)
class ForecastGenerationResult:
    analyses_considered: int
    scenarios_created: int


def generate_rental_forecasts(*, years: int = 5, engine: Engine | None = None) -> ForecastGenerationResult:
    engine = engine or get_engine()
    with engine.connect() as connection:
        analyses = [dict(row) for row in connection.execute(ANALYSES_SQL).mappings()]
    created = 0
    with engine.begin() as connection:
        for analysis in analyses:
            for name, growth in SCENARIOS.items():
                projection = forecast_rental_scenario(
                    monthly_rent=Decimal(str(analysis["monthly_rent"])),
                    monthly_cash_flow=Decimal(str(analysis["monthly_cash_flow"])),
                    annual_rent_growth_pct=growth,
                    years=years,
                )
                encoded = json.dumps(projection, default=str, separators=(",", ":"))
                fingerprint = hashlib.sha256(
                    f"{analysis['id']}|{FORECAST_VERSION}|{name}|{years}|{encoded}".encode()
                ).hexdigest()
                created += max(connection.execute(INSERT_SQL, {
                    "rental_analysis_id": analysis["id"], "forecast_version": FORECAST_VERSION,
                    "scenario_name": name, "annual_rent_growth_pct": growth,
                    "horizon_years": years, "input_fingerprint": fingerprint,
                    "projection_json": encoded,
                }).rowcount, 0)
    return ForecastGenerationResult(len(analyses), created)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Persist rent-growth forecast scenarios without provider calls.")
    parser.add_argument("--years", type=int, default=5)
    args = parser.parse_args()
    print(json.dumps(asdict(generate_rental_forecasts(years=args.years)), indent=2))

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal

from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.rental_assumptions import DEFAULT_ASSUMPTION_VERSION, load_active_assumptions
from app.market.rental_forecast import FORECAST_VERSION, forecast_rental_scenario


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
        horizon_years, input_fingerprint, projection_json, assumption_version
    ) VALUES (
        :rental_analysis_id, :forecast_version, :scenario_name, :annual_rent_growth_pct,
        :horizon_years, :input_fingerprint, CAST(:projection_json AS JSONB), :assumption_version
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
        assumptions = load_active_assumptions(connection)
    print(f"Using rental assumption version {assumptions.version}")
    created = 0
    with engine.begin() as connection:
        for analysis in analyses:
            for name, growth in assumptions.scenarios.items():
                projection = forecast_rental_scenario(
                    monthly_rent=Decimal(str(analysis["monthly_rent"])),
                    monthly_cash_flow=Decimal(str(analysis["monthly_cash_flow"])),
                    annual_rent_growth_pct=growth,
                    years=years,
                    pass_through=assumptions.pass_through,
                )
                encoded = json.dumps(projection, default=str, separators=(",", ":"))
                # Version 1 keeps its original fingerprint so existing rows are not
                # duplicated; later assumption versions are part of the hash.
                suffix = "" if assumptions.version == DEFAULT_ASSUMPTION_VERSION else f"|{assumptions.version}"
                fingerprint = hashlib.sha256(
                    f"{analysis['id']}|{FORECAST_VERSION}|{name}|{years}|{encoded}{suffix}".encode()
                ).hexdigest()
                created += max(connection.execute(INSERT_SQL, {
                    "rental_analysis_id": analysis["id"], "forecast_version": FORECAST_VERSION,
                    "scenario_name": name, "annual_rent_growth_pct": growth,
                    "horizon_years": years, "input_fingerprint": fingerprint,
                    "projection_json": encoded, "assumption_version": assumptions.version,
                }).rowcount, 0)
    return ForecastGenerationResult(len(analyses), created)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Persist rent-growth forecast scenarios without provider calls.")
    parser.add_argument("--years", type=int, default=5)
    args = parser.parse_args()
    print(json.dumps(asdict(generate_rental_forecasts(years=args.years)), indent=2))

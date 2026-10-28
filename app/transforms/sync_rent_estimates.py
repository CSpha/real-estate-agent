from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.analysis_dates import analysis_today
from app.market.rental_underwriting import (
    ANALYSIS_VERSION,
    UnderwritingAssumptions,
    calculate_rental_underwriting,
)
from app.providers.rentcast_rent import RentCastRentProvider


LISTINGS_SQL = text(
    """
    SELECT source, source_listing_id, address, city, state, zip, property_type,
           list_price, beds, baths, sqft
    FROM listings_current
    WHERE source = :source
      AND LOWER(status) = 'active'
      AND scoring_eligible
      AND address IS NOT NULL
      AND list_price > 0
    ORDER BY source_listing_id
    """
)
EXISTING_ESTIMATE_SQL = text(
    """
    SELECT id, monthly_rent, rent_range_low, rent_range_high, comparable_count,
           request_json, response_json, as_of_date
    FROM listing_rent_estimates
    WHERE source = :source
      AND source_listing_id = :source_listing_id
      AND provider = :provider
      AND request_fingerprint = :request_fingerprint
      AND as_of_date >= :fresh_after
    ORDER BY as_of_date DESC, id DESC
    LIMIT 1
    """
)
ESTIMATE_INSERT_SQL = text(
    """
    INSERT INTO listing_rent_estimates (
        source, source_listing_id, provider, as_of_date, request_fingerprint,
        monthly_rent, rent_range_low, rent_range_high, comparable_count,
        request_json, response_json
    ) VALUES (
        :source, :source_listing_id, :provider, :as_of_date, :request_fingerprint,
        :monthly_rent, :rent_range_low, :rent_range_high, :comparable_count,
        CAST(:request_json AS JSONB), CAST(:response_json AS JSONB)
    )
    ON CONFLICT (
        source, source_listing_id, provider, as_of_date, request_fingerprint
    ) DO NOTHING
    RETURNING id
    """
)
ESTIMATE_SELECT_SQL = text(
    """
    SELECT id
    FROM listing_rent_estimates
    WHERE source = :source
      AND source_listing_id = :source_listing_id
      AND provider = :provider
      AND as_of_date = :as_of_date
      AND request_fingerprint = :request_fingerprint
    """
)
RATE_SQL = text(
    """
    SELECT observation_date, rate_percent
    FROM mortgage_rate_observations
    WHERE source = 'freddie_mac_pmms'
      AND product_type = '30_year_fixed'
      AND observation_date <= :as_of_date
    ORDER BY observation_date DESC, id DESC
    LIMIT 1
    """
)
ANALYSIS_INSERT_SQL = text(
    """
    INSERT INTO rental_underwriting_analyses (
        rent_estimate_id, analysis_version, input_fingerprint, purchase_price,
        monthly_rent, gross_yield_pct, monthly_noi, cap_rate_pct,
        monthly_cash_flow, target_dscr, underwritten_dscr, interest_rate_pct,
        term_years, max_ltv_pct, max_loan_by_dscr, ltv_cap_loan,
        target_loan_amount, target_ltv_pct, down_payment,
        monthly_principal_interest, monthly_pitia,
        minimum_rent_for_target_dscr, "constraint", calculation_json
    ) VALUES (
        :rent_estimate_id, :analysis_version, :input_fingerprint, :purchase_price,
        :monthly_rent, :gross_yield_pct, :monthly_noi, :cap_rate_pct,
        :monthly_cash_flow, :target_dscr, :underwritten_dscr, :interest_rate_pct,
        :term_years, :max_ltv_pct, :max_loan_by_dscr, :ltv_cap_loan,
        :target_loan_amount, :target_ltv_pct, :down_payment,
        :monthly_principal_interest, :monthly_pitia,
        :minimum_rent_for_target_dscr, :constraint, CAST(:calculation_json AS JSONB)
    )
    ON CONFLICT (rent_estimate_id, analysis_version, input_fingerprint)
    DO NOTHING
    RETURNING id
    """
)


@dataclass(frozen=True)
class RentSyncResult:
    eligible_listings: int
    api_requests: int
    estimates_created: int
    fresh_estimates_reused: int
    analyses_created: int
    failures: int
    request_limit_reached: bool


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        default=_json_default,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _rate_assumption(
    engine: Engine,
    spread_pct: Decimal,
    as_of_date: date,
) -> tuple[Decimal, dict[str, Any]]:
    with engine.connect() as connection:
        row = connection.execute(
            RATE_SQL, {"as_of_date": as_of_date}
        ).mappings().one_or_none()
    if row is None:
        return Decimal("8.0"), {
            "basis": "fallback",
            "fallback_rate_pct": "8.0",
            "spread_pct": str(spread_pct),
        }
    benchmark = Decimal(str(row["rate_percent"]))
    return benchmark + spread_pct, {
        "basis": "freddie_mac_pmms_plus_spread",
        "benchmark_product": "30_year_fixed",
        "benchmark_date": row["observation_date"],
        "benchmark_rate_pct": benchmark,
        "spread_pct": spread_pct,
    }


def _persist_analysis(
    *,
    engine: Engine,
    rent_estimate_id: int,
    purchase_price: Decimal,
    monthly_rent: Decimal,
    assumptions: UnderwritingAssumptions,
    rate_basis: dict[str, Any],
) -> bool:
    calculation = calculate_rental_underwriting(
        purchase_price=purchase_price,
        monthly_rent=monthly_rent,
        assumptions=assumptions,
    )
    calculation["rate_basis"] = rate_basis
    input_fingerprint = _fingerprint(
        {
            "rent_estimate_id": rent_estimate_id,
            "purchase_price": purchase_price,
            "analysis_version": ANALYSIS_VERSION,
            "assumptions": asdict(assumptions),
            "rate_basis": rate_basis,
        }
    )
    parameters = {
        "rent_estimate_id": rent_estimate_id,
        "input_fingerprint": input_fingerprint,
        "calculation_json": _canonical_json(calculation),
        **{key: calculation[key] for key in (
            "analysis_version",
            "purchase_price",
            "monthly_rent",
            "gross_yield_pct",
            "monthly_noi",
            "cap_rate_pct",
            "monthly_cash_flow",
            "target_dscr",
            "underwritten_dscr",
            "interest_rate_pct",
            "term_years",
            "max_ltv_pct",
            "max_loan_by_dscr",
            "ltv_cap_loan",
            "target_loan_amount",
            "target_ltv_pct",
            "down_payment",
            "monthly_principal_interest",
            "monthly_pitia",
            "minimum_rent_for_target_dscr",
            "constraint",
        )},
    }
    with engine.begin() as connection:
        return connection.execute(ANALYSIS_INSERT_SQL, parameters).scalar_one_or_none() is not None


def sync_rent_estimates(
    *,
    source: str = "rentcast",
    as_of_date: date | None = None,
    max_requests: int = 25,
    refresh_days: int = 30,
    dscr_spread_pct: Decimal = Decimal("1.50"),
    assumptions: UnderwritingAssumptions | None = None,
    engine: Engine | None = None,
    provider: RentCastRentProvider | None = None,
) -> RentSyncResult:
    if max_requests < 0 or refresh_days < 0:
        raise ValueError("max_requests and refresh_days cannot be negative")
    engine = engine or get_engine()
    provider = provider or RentCastRentProvider()
    as_of_date = as_of_date or analysis_today()
    rate, rate_basis = _rate_assumption(engine, dscr_spread_pct, as_of_date)
    assumptions = replace(
        assumptions or UnderwritingAssumptions(),
        interest_rate_pct=rate,
    )
    with engine.connect() as connection:
        listings = [dict(row) for row in connection.execute(
            LISTINGS_SQL, {"source": source}
        ).mappings()]

    api_requests = estimates_created = reused = analyses_created = failures = 0
    request_limit_reached = False
    for listing in listings:
        request = provider.build_request(listing)
        request_fingerprint = _fingerprint(request)
        lookup = {
            "source": listing["source"],
            "source_listing_id": listing["source_listing_id"],
            "provider": provider.provider_name,
            "request_fingerprint": request_fingerprint,
            "fresh_after": as_of_date - timedelta(days=refresh_days),
        }
        with engine.connect() as connection:
            existing = connection.execute(EXISTING_ESTIMATE_SQL, lookup).mappings().one_or_none()
        if existing is not None:
            estimate_id = existing["id"]
            monthly_rent = Decimal(str(existing["monthly_rent"]))
            reused += 1
        else:
            if api_requests >= max_requests:
                request_limit_reached = True
                continue
            api_requests += 1
            try:
                estimate = provider.estimate(listing)
            except Exception:
                failures += 1
                continue
            parameters = {
                **lookup,
                "as_of_date": as_of_date,
                "monthly_rent": estimate.monthly_rent,
                "rent_range_low": estimate.rent_range_low,
                "rent_range_high": estimate.rent_range_high,
                "comparable_count": len(estimate.comparables),
                "request_json": _canonical_json(estimate.request),
                "response_json": _canonical_json(estimate.response),
            }
            with engine.begin() as connection:
                estimate_id = connection.execute(
                    ESTIMATE_INSERT_SQL, parameters
                ).scalar_one_or_none()
                if estimate_id is None:
                    estimate_id = connection.execute(
                        ESTIMATE_SELECT_SQL, parameters
                    ).scalar_one()
                else:
                    estimates_created += 1
            monthly_rent = estimate.monthly_rent
        if _persist_analysis(
            engine=engine,
            rent_estimate_id=estimate_id,
            purchase_price=Decimal(str(listing["list_price"])),
            monthly_rent=monthly_rent,
            assumptions=assumptions,
            rate_basis=rate_basis,
        ):
            analyses_created += 1

    return RentSyncResult(
        eligible_listings=len(listings),
        api_requests=api_requests,
        estimates_created=estimates_created,
        fresh_estimates_reused=reused,
        analyses_created=analyses_created,
        failures=failures,
        request_limit_reached=request_limit_reached,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch bounded RentCast rent AVMs and persist DSCR underwriting."
    )
    parser.add_argument("--max-requests", type=int, default=25)
    parser.add_argument("--refresh-days", type=int, default=30)
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--dscr-spread-pct", type=Decimal, default=Decimal("1.50"))
    args = parser.parse_args()
    result = sync_rent_estimates(
        max_requests=args.max_requests,
        refresh_days=args.refresh_days,
        as_of_date=args.as_of_date,
        dscr_spread_pct=args.dscr_spread_pct,
    )
    print("Rent estimate sync complete (no alerts):")
    print(json.dumps(asdict(result), indent=2))


if __name__ == "__main__":
    main()

"""Monthly cost and rent coverage for listing alerts.

Computed when an alert is queued and stored in its payload, so a message shows
the PMMS rate and rent estimate that were current when the event was detected.
The figures use the benchmark scenario from app.market.affordability (no
+/- spread) with its default tax, insurance, PMI and DSCR assumptions.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any, Iterable, Mapping

from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.affordability import (
    DSCR_TARGET,
    AffordabilityAssumptions,
    BenchmarkRate,
    calculate_affordability,
    get_latest_benchmark_rate,
)


CONVENTIONAL_DOWN_PCTS = (Decimal("10"), Decimal("20"))
DSCR_DOWN_PCT = Decimal("20")
_BENCHMARK = 1  # index of the benchmark scenario (lower, benchmark, higher)

LATEST_RENTS_SQL = text(
    """
    SELECT DISTINCT ON (source, source_listing_id)
        source,
        source_listing_id,
        monthly_rent
    FROM listing_rent_estimates
    WHERE source_listing_id = ANY(:listing_ids)
    ORDER BY source, source_listing_id, as_of_date DESC, id DESC
    """
)


def _str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def summarize_affordability(
    price: Any,
    benchmark: BenchmarkRate,
    monthly_rent: Any = None,
    assumptions: AffordabilityAssumptions | None = None,
) -> dict[str, str | None] | None:
    """Payload fields for one listing, or None when the price can't be used."""
    assumptions = assumptions or AffordabilityAssumptions()
    try:
        conventional = {
            down: calculate_affordability(
                price,
                benchmark.rate_pct,
                replace(assumptions, down_payment_pct=down),
            ).scenarios[_BENCHMARK]
            for down in CONVENTIONAL_DOWN_PCTS
        }
        dscr = calculate_affordability(
            price,
            benchmark.rate_pct,
            replace(assumptions, down_payment_pct=DSCR_DOWN_PCT),
            program="dscr",
            monthly_rent=monthly_rent,
        ).scenarios[_BENCHMARK]
    except (ValueError, ArithmeticError):
        return None
    return {
        "rate_pct": str(benchmark.rate_pct),
        "rate_as_of": benchmark.as_of_date.isoformat(),
        "property_tax_rate_pct": str(assumptions.property_tax_rate_pct),
        "insurance_rate_pct": str(assumptions.insurance_rate_pct),
        "conventional_10_down": str(conventional[Decimal("10")].total),
        "conventional_20_down": str(conventional[Decimal("20")].total),
        "dscr_rate_pct": str(dscr.rate_pct),
        "dscr_20_down": str(dscr.total),
        "monthly_rent": _str(
            None if monthly_rent is None else Decimal(str(monthly_rent))
        ),
        "dscr": _str(dscr.dscr),
        "dscr_target": str(DSCR_TARGET),
        "rent_for_target": str(dscr.rent_for_target),
    }


def get_latest_rents(
    events: Iterable[Mapping[str, Any]], engine: Engine
) -> dict[tuple[str, str], Decimal]:
    keys = {(event["source"], event["source_listing_id"]) for event in events}
    if not keys:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            LATEST_RENTS_SQL,
            {"listing_ids": sorted({listing_id for _, listing_id in keys})},
        ).mappings().all()
    return {
        (row["source"], row["source_listing_id"]): row["monthly_rent"]
        for row in rows
        if (row["source"], row["source_listing_id"]) in keys
        and row["monthly_rent"] is not None
    }


def attach_affordability(
    events: Iterable[Mapping[str, Any]],
    price_field: str,
    engine: Engine | None = None,
) -> list[dict[str, Any]]:
    """Copy each event with an "affordability" entry (None without a PMMS rate)."""
    events = [dict(event) for event in events]
    if not events:
        return events
    engine = engine or get_engine()
    benchmark = get_latest_benchmark_rate(engine)
    rents = get_latest_rents(events, engine) if benchmark is not None else {}
    for event in events:
        event["affordability"] = (
            summarize_affordability(
                event.get(price_field),
                benchmark,
                rents.get((event["source"], event["source_listing_id"])),
            )
            if benchmark is not None
            else None
        )
    return events

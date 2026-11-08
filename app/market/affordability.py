"""Owner-occupant monthly payment scenarios around the PMMS benchmark rate.

These are scenarios, not forecasts: they show what a listing's payment would be
if the 30-year fixed rate were 0.50 points lower than, equal to, or 0.50 points
higher than the latest stored Freddie Mac PMMS observation. They are not lender
quotes and say nothing about which way rates will move.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.mortgage_rates import PMMS_SOURCE
from app.market.rental_underwriting import (
    MONEY,
    UnderwritingAssumptions,
    payment_factor,
)


BENCHMARK_PRODUCT = "30_year_fixed"
MAX_RATE_PCT = Decimal("30")  # matches the DB check on mortgage_rate_observations
# PMMS publishes weekly, so an older benchmark means the rate load hasn't run.
STALE_BENCHMARK_DAYS = 14
_DEFAULTS = UnderwritingAssumptions()
SCENARIO_LABELS = ("benchmark - spread", "benchmark", "benchmark + spread")


@dataclass(frozen=True)
class AffordabilityAssumptions:
    """Explicit inputs for the payment estimate.

    Tax, insurance and HOA defaults come from UnderwritingAssumptions so the
    rental underwriting and affordability views stay consistent.
    """

    down_payment_pct: Decimal = Decimal("20")
    term_years: int = 30
    rate_spread_pct: Decimal = Decimal("0.50")
    # Annual percent of price.
    property_tax_rate_pct: Decimal = _DEFAULTS.property_tax_rate_pct
    # Annual percent of price.
    insurance_rate_pct: Decimal = _DEFAULTS.insurance_rate_pct
    # Dollars per month. The listing data has no HOA field, so this is an assumption.
    monthly_hoa: Decimal = _DEFAULTS.monthly_hoa


@dataclass(frozen=True)
class AffordabilityScenario:
    label: str
    rate_pct: Decimal
    principal_interest: Decimal
    property_tax: Decimal
    insurance: Decimal
    hoa: Decimal
    total: Decimal  # sum of the rounded components, so the table adds up


@dataclass(frozen=True)
class AffordabilityResult:
    price: Decimal
    down_payment: Decimal
    loan_amount: Decimal
    benchmark_rate_pct: Decimal
    assumptions: AffordabilityAssumptions
    scenarios: tuple[AffordabilityScenario, ...]  # lower, benchmark, higher


@dataclass(frozen=True)
class BenchmarkRate:
    rate_pct: Decimal
    as_of_date: date
    source: str
    source_url: str
    product_type: str = BENCHMARK_PRODUCT


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def calculate_affordability(
    price: Decimal | int | str | None,
    benchmark_rate_pct: Decimal | int | str,
    assumptions: AffordabilityAssumptions | None = None,
) -> AffordabilityResult:
    """Return the three rate scenarios for one price.

    Raises ValueError for a missing or non-positive price, a down payment
    outside 0-100%, or rates that would be negative or above 30%.
    """
    assumptions = assumptions or AffordabilityAssumptions()
    if price is None:
        raise ValueError("price is missing")
    price = Decimal(str(price))
    benchmark = Decimal(str(benchmark_rate_pct))
    if price <= 0:
        raise ValueError("price must be positive")
    if not Decimal("0") <= assumptions.down_payment_pct <= Decimal("100"):
        raise ValueError("down_payment_pct must be between 0 and 100")
    if assumptions.rate_spread_pct < 0:
        raise ValueError("rate_spread_pct cannot be negative")
    for name in ("property_tax_rate_pct", "insurance_rate_pct", "monthly_hoa"):
        if getattr(assumptions, name) < 0:
            raise ValueError(f"{name} cannot be negative")
    low_rate = benchmark - assumptions.rate_spread_pct
    high_rate = benchmark + assumptions.rate_spread_pct
    if low_rate < 0:
        raise ValueError(
            f"benchmark rate {benchmark}% minus {assumptions.rate_spread_pct} "
            "points would be negative"
        )
    if benchmark > MAX_RATE_PCT:
        raise ValueError(f"benchmark rate {benchmark}% is above {MAX_RATE_PCT}%")

    down_payment = price * assumptions.down_payment_pct / Decimal("100")
    loan = price - down_payment
    tax = _money(price * assumptions.property_tax_rate_pct / Decimal("1200"))
    insurance = _money(price * assumptions.insurance_rate_pct / Decimal("1200"))
    hoa = _money(assumptions.monthly_hoa)

    scenarios = []
    for label, rate in zip(SCENARIO_LABELS, (low_rate, benchmark, high_rate)):
        principal_interest = _money(
            loan * payment_factor(rate, assumptions.term_years)
        )
        scenarios.append(
            AffordabilityScenario(
                label=label,
                rate_pct=rate,
                principal_interest=principal_interest,
                property_tax=tax,
                insurance=insurance,
                hoa=hoa,
                total=principal_interest + tax + insurance + hoa,
            )
        )
    return AffordabilityResult(
        price=_money(price),
        down_payment=_money(down_payment),
        loan_amount=_money(loan),
        benchmark_rate_pct=benchmark,
        assumptions=assumptions,
        scenarios=tuple(scenarios),
    )


LATEST_BENCHMARK_SQL = text(
    """
    SELECT rate_percent, observation_date, source, source_url, product_type
    FROM mortgage_rate_observations
    WHERE source = :source
      AND product_type = :product_type
    ORDER BY observation_date DESC
    LIMIT 1
    """
)


def get_latest_benchmark_rate(
    engine: Engine | None = None,
    *,
    source: str = PMMS_SOURCE,
    product_type: str = BENCHMARK_PRODUCT,
) -> BenchmarkRate | None:
    """Latest stored rate, or None when nothing is stored (never a default)."""
    engine = engine or get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            LATEST_BENCHMARK_SQL,
            {"source": source, "product_type": product_type},
        ).mappings().first()
    if row is None:
        return None
    return BenchmarkRate(
        rate_pct=Decimal(str(row["rate_percent"])),
        as_of_date=row["observation_date"],
        source=row["source"],
        source_url=row["source_url"],
        product_type=row["product_type"],
    )


LISTINGS_SQL = text(
    """
    SELECT source, source_listing_id, address, city, state, list_price
    FROM listings_current
    WHERE scoring_eligible
      AND missing_from_feed_since IS NULL
      AND (CAST(:source AS text) IS NULL OR source = :source)
      AND (CAST(:listing_id AS text) IS NULL OR source_listing_id = :listing_id)
    ORDER BY source, source_listing_id
    LIMIT :limit
    """
)


def get_listings(
    engine: Engine,
    *,
    source: str | None,
    listing_id: str | None,
    limit: int,
) -> list[dict]:
    # Read-only: a single SELECT, and the transaction is marked READ ONLY.
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        rows = conn.execute(
            LISTINGS_SQL,
            {"source": source, "listing_id": listing_id, "limit": limit},
        ).mappings().all()
    return [dict(row) for row in rows]


def benchmark_age_warning(benchmark: BenchmarkRate, today: date) -> str | None:
    age_days = (today - benchmark.as_of_date).days
    if age_days <= STALE_BENCHMARK_DAYS:
        return None
    return (
        f"Warning: the benchmark is {age_days} days old; refresh it with "
        "`python -m app.market.mortgage_rates`."
    )


def _usd(value: Decimal) -> str:
    return f"${value:,.2f}"


def format_assumptions(assumptions: AffordabilityAssumptions) -> list[str]:
    return [
        f"Down payment: {assumptions.down_payment_pct}% of price",
        f"Loan: fixed rate, {assumptions.term_years} years, "
        "price minus down payment",
        f"Scenario spread: +/- {assumptions.rate_spread_pct} points "
        "around the benchmark",
        f"Property tax: {assumptions.property_tax_rate_pct}% of price per year "
        "(assumption)",
        f"Insurance: {assumptions.insurance_rate_pct}% of price per year "
        "(assumption)",
        f"HOA: {_usd(assumptions.monthly_hoa)} per month "
        "(assumption; listing data has no HOA field)",
    ]


def format_result(result: AffordabilityResult) -> list[str]:
    lines = [
        f"  Price {_usd(result.price)} | down payment {_usd(result.down_payment)} "
        f"| loan {_usd(result.loan_amount)}",
        f"  {'Scenario':<20}{'Rate':>7}{'P&I':>12}{'Tax':>10}"
        f"{'Ins.':>10}{'HOA':>9}{'Total/mo':>12}",
    ]
    for item in result.scenarios:
        lines.append(
            f"  {item.label:<20}{item.rate_pct:>6}%"
            f"{_usd(item.principal_interest):>12}{_usd(item.property_tax):>10}"
            f"{_usd(item.insurance):>10}{_usd(item.hoa):>9}{_usd(item.total):>12}"
        )
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only monthly payment scenarios around the PMMS "
        "30-year benchmark rate.",
    )
    parser.add_argument(
        "--source",
        default="rentcast",
        help="Only listings from this source (default rentcast; 'all' for every source).",
    )
    parser.add_argument("--limit", type=int, default=10, help="Max listings.")
    parser.add_argument("--listing-id", help="Only this source_listing_id.")
    parser.add_argument(
        "--down-payment-pct",
        type=Decimal,
        default=AffordabilityAssumptions().down_payment_pct,
        help="Down payment as a percent of price (default 20).",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    assumptions = AffordabilityAssumptions(down_payment_pct=args.down_payment_pct)

    print("Step 1: reading the latest stored benchmark rate")
    engine = get_engine()
    benchmark = get_latest_benchmark_rate(engine)
    if benchmark is None:
        print(
            "No benchmark rate stored: mortgage_rate_observations has no "
            f"{BENCHMARK_PRODUCT} rows from {PMMS_SOURCE}. Load them with "
            "`python -m app.market.mortgage_rates`; no default rate is used."
        )
        return 1
    print(
        f"Benchmark: {benchmark.rate_pct}% ({benchmark.product_type}), "
        f"as of {benchmark.as_of_date.isoformat()}, source {benchmark.source} "
        f"({benchmark.source_url})"
    )
    warning = benchmark_age_warning(benchmark, date.today())
    if warning:
        print(f"  {warning}")

    print("Step 2: assumptions")
    for line in format_assumptions(assumptions):
        print(f"  {line}")
    print(
        "  These are rate scenarios, not forecasts or lender quotes; "
        "the benchmark can move in either direction."
    )

    print("Step 3: loading current scoring-eligible listings")
    listings = get_listings(
        engine,
        source=None if args.source == "all" else args.source,
        listing_id=args.listing_id,
        limit=args.limit,
    )
    print(f"  {len(listings)} listing(s) selected (limit {args.limit}).")

    shown = 0
    for listing in listings:
        place = ", ".join(
            part
            for part in (listing["address"], listing["city"], listing["state"])
            if part
        )
        print(f"\n{listing['source']}/{listing['source_listing_id']}: {place}")
        try:
            result = calculate_affordability(
                listing["list_price"], benchmark.rate_pct, assumptions
            )
        except ValueError as exc:
            print(f"  Skipped: {exc}")
            continue
        for line in format_result(result):
            print(line)
        shown += 1
    print(f"\nDone: {shown} of {len(listings)} listing(s) calculated. No data written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

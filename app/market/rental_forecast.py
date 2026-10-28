from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


FORECAST_VERSION = "rental-growth-scenarios-v1"
SCENARIOS = {
    "conservative": Decimal("-1.0"),
    "base": Decimal("2.0"),
    "upside": Decimal("4.0"),
}


def forecast_rental_scenario(
    *,
    monthly_rent: Decimal,
    monthly_cash_flow: Decimal,
    annual_rent_growth_pct: Decimal,
    years: int = 5,
) -> list[dict[str, Decimal | int]]:
    if monthly_rent <= 0 or years < 1:
        raise ValueError("monthly_rent must be positive and years must be at least one")
    growth = Decimal("1") + annual_rent_growth_pct / Decimal("100")
    rows = []
    for year in range(1, years + 1):
        rent = (monthly_rent * growth**year).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        # Variable operating costs are already embedded in cash flow; retain the
        # current cost ratio and debt service while rent changes.
        cash_flow = (monthly_cash_flow + (rent - monthly_rent) * Decimal("0.74")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        rows.append({"year": year, "monthly_rent": rent, "monthly_cash_flow": cash_flow})
    return rows

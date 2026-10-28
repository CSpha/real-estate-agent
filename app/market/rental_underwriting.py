from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP


ANALYSIS_VERSION = "rental-underwriting-v1"
MONEY = Decimal("0.01")
PERCENT = Decimal("0.0001")


@dataclass(frozen=True)
class UnderwritingAssumptions:
    target_dscr: Decimal = Decimal("1.25")
    max_ltv_pct: Decimal = Decimal("75.0")
    interest_rate_pct: Decimal = Decimal("8.0")
    term_years: int = 30
    property_tax_rate_pct: Decimal = Decimal("1.50")
    insurance_rate_pct: Decimal = Decimal("0.50")
    vacancy_pct: Decimal = Decimal("5.0")
    management_pct: Decimal = Decimal("8.0")
    maintenance_pct: Decimal = Decimal("8.0")
    capital_reserve_pct: Decimal = Decimal("5.0")
    monthly_hoa: Decimal = Decimal("0")


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def _payment_factor(rate_pct: Decimal, term_years: int) -> Decimal:
    periods = term_years * 12
    if periods <= 0:
        raise ValueError("term_years must be positive")
    monthly_rate = rate_pct / Decimal("1200")
    if monthly_rate == 0:
        return Decimal("1") / Decimal(periods)
    growth = (Decimal("1") + monthly_rate) ** periods
    return monthly_rate * growth / (growth - Decimal("1"))


def calculate_rental_underwriting(
    *,
    purchase_price: Decimal,
    monthly_rent: Decimal,
    assumptions: UnderwritingAssumptions | None = None,
) -> dict[str, object]:
    assumptions = assumptions or UnderwritingAssumptions()
    if purchase_price <= 0 or monthly_rent <= 0:
        raise ValueError("purchase_price and monthly_rent must be positive")
    if assumptions.target_dscr <= 0:
        raise ValueError("target_dscr must be positive")
    if not Decimal("0") < assumptions.max_ltv_pct <= Decimal("100"):
        raise ValueError("max_ltv_pct must be between 0 and 100")
    for name in (
        "property_tax_rate_pct",
        "insurance_rate_pct",
        "vacancy_pct",
        "management_pct",
        "maintenance_pct",
        "capital_reserve_pct",
    ):
        if getattr(assumptions, name) < 0:
            raise ValueError(f"{name} cannot be negative")

    factor = _payment_factor(assumptions.interest_rate_pct, assumptions.term_years)
    monthly_tax = purchase_price * assumptions.property_tax_rate_pct / Decimal("1200")
    monthly_insurance = (
        purchase_price * assumptions.insurance_rate_pct / Decimal("1200")
    )
    fixed_housing_cost = monthly_tax + monthly_insurance + assumptions.monthly_hoa
    max_pitia = monthly_rent / assumptions.target_dscr
    max_principal_interest = max(Decimal("0"), max_pitia - fixed_housing_cost)
    max_loan_by_dscr = max_principal_interest / factor
    ltv_cap_loan = purchase_price * assumptions.max_ltv_pct / Decimal("100")
    target_loan = max(Decimal("0"), min(max_loan_by_dscr, ltv_cap_loan))
    principal_interest = target_loan * factor
    pitia = principal_interest + fixed_housing_cost
    underwritten_dscr = monthly_rent / pitia if pitia > 0 else None

    percentage_cost = (
        assumptions.management_pct
        + assumptions.maintenance_pct
        + assumptions.capital_reserve_pct
    ) / Decimal("100")
    effective_rent = monthly_rent * (
        Decimal("1") - assumptions.vacancy_pct / Decimal("100")
    )
    monthly_noi = (
        effective_rent
        - monthly_rent * percentage_cost
        - fixed_housing_cost
    )
    monthly_cash_flow = monthly_noi - principal_interest
    down_payment = purchase_price - target_loan
    target_ltv_pct = target_loan / purchase_price * Decimal("100")
    constraint = "ltv" if ltv_cap_loan <= max_loan_by_dscr else "dscr"

    result: dict[str, object] = {
        "analysis_version": ANALYSIS_VERSION,
        "purchase_price": _money(purchase_price),
        "monthly_rent": _money(monthly_rent),
        "gross_yield_pct": (
            monthly_rent * Decimal("12") / purchase_price * Decimal("100")
        ).quantize(PERCENT, rounding=ROUND_HALF_UP),
        "monthly_noi": _money(monthly_noi),
        "cap_rate_pct": (
            monthly_noi * Decimal("12") / purchase_price * Decimal("100")
        ).quantize(PERCENT, rounding=ROUND_HALF_UP),
        "monthly_cash_flow": _money(monthly_cash_flow),
        "target_dscr": assumptions.target_dscr,
        "underwritten_dscr": (
            underwritten_dscr.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            if underwritten_dscr is not None
            else None
        ),
        "interest_rate_pct": assumptions.interest_rate_pct,
        "term_years": assumptions.term_years,
        "max_ltv_pct": assumptions.max_ltv_pct,
        "max_loan_by_dscr": _money(max_loan_by_dscr),
        "ltv_cap_loan": _money(ltv_cap_loan),
        "target_loan_amount": _money(target_loan),
        "target_ltv_pct": target_ltv_pct.quantize(PERCENT, rounding=ROUND_HALF_UP),
        "down_payment": _money(down_payment),
        "monthly_principal_interest": _money(principal_interest),
        "monthly_pitia": _money(pitia),
        "minimum_rent_for_target_dscr": _money(pitia * assumptions.target_dscr),
        "constraint": constraint,
        "assumptions": asdict(assumptions),
        "monthly_costs": {
            "property_tax": _money(monthly_tax),
            "insurance": _money(monthly_insurance),
            "hoa": _money(assumptions.monthly_hoa),
            "vacancy": _money(monthly_rent - effective_rent),
            "management": _money(
                monthly_rent * assumptions.management_pct / Decimal("100")
            ),
            "maintenance": _money(
                monthly_rent * assumptions.maintenance_pct / Decimal("100")
            ),
            "capital_reserve": _money(
                monthly_rent * assumptions.capital_reserve_pct / Decimal("100")
            ),
        },
    }
    return result

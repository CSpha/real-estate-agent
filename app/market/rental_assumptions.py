"""Versioned, database-backed rental forecast assumptions."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection


DEFAULT_ASSUMPTION_VERSION = "rental-assumptions-v1"
SCENARIO_NAMES = ("conservative", "base", "upside")
DEFAULT_ASSUMPTIONS: dict[str, Any] = {
    "scenario_growth_pct": {"conservative": "-1.0", "base": "2.0", "upside": "4.0"},
    "rent_to_cash_flow_pass_through": "0.74",
}


@dataclass(frozen=True)
class RentalAssumptions:
    version: str
    assumptions: dict[str, Any]

    @property
    def scenarios(self) -> dict[str, Decimal]:
        return {k: Decimal(str(v)) for k, v in self.assumptions["scenario_growth_pct"].items()}

    @property
    def pass_through(self) -> Decimal:
        return Decimal(str(self.assumptions["rent_to_cash_flow_pass_through"]))


DEFAULT_POLICY = RentalAssumptions(DEFAULT_ASSUMPTION_VERSION, DEFAULT_ASSUMPTIONS)


def _decimal(value: Any, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def validate_assumptions(assumptions: dict[str, Any]) -> dict[str, Any]:
    if set(assumptions) != set(DEFAULT_ASSUMPTIONS):
        missing = sorted(set(DEFAULT_ASSUMPTIONS) - set(assumptions))
        extra = sorted(set(assumptions) - set(DEFAULT_ASSUMPTIONS))
        raise ValueError(f"Assumptions must match the schema; missing={missing}, extra={extra}")
    growth = assumptions["scenario_growth_pct"]
    if not isinstance(growth, dict) or set(growth) != set(SCENARIO_NAMES):
        raise ValueError(f"scenario_growth_pct must contain exactly {list(SCENARIO_NAMES)}")
    rates = {name: _decimal(growth[name], name) for name in SCENARIO_NAMES}
    for name, rate in rates.items():
        if not Decimal("-20") <= rate <= Decimal("20"):
            raise ValueError(f"{name} growth must be between -20 and 20 percent per year")
    if not rates["conservative"] <= rates["base"] <= rates["upside"]:
        raise ValueError("growth rates must satisfy conservative <= base <= upside")
    pass_through = _decimal(assumptions["rent_to_cash_flow_pass_through"], "rent_to_cash_flow_pass_through")
    if not Decimal("0") <= pass_through <= Decimal("1"):
        raise ValueError("rent_to_cash_flow_pass_through must be between 0 and 1")
    return {
        "scenario_growth_pct": {name: str(rates[name]) for name in SCENARIO_NAMES},
        "rent_to_cash_flow_pass_through": str(pass_through),
    }


def load_active_assumptions(connection: Connection) -> RentalAssumptions:
    row = connection.execute(
        text("SELECT policy_version, assumptions FROM rental_assumption_policies WHERE is_active = TRUE")
    ).mappings().one()
    return RentalAssumptions(row["policy_version"], validate_assumptions(dict(row["assumptions"])))

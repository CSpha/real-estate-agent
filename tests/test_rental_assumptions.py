from decimal import Decimal

import pytest

from app.market.rental_assumptions import (
    DEFAULT_ASSUMPTIONS,
    DEFAULT_POLICY,
    RentalAssumptions,
    load_active_assumptions,
    validate_assumptions,
)
from app.market.rental_forecast import forecast_rental_scenario


def _copy():
    return {
        "scenario_growth_pct": dict(DEFAULT_ASSUMPTIONS["scenario_growth_pct"]),
        "rent_to_cash_flow_pass_through": DEFAULT_ASSUMPTIONS["rent_to_cash_flow_pass_through"],
    }


def test_default_policy_preserves_previous_constants():
    assert DEFAULT_POLICY.scenarios == {
        "conservative": Decimal("-1.0"), "base": Decimal("2.0"), "upside": Decimal("4.0"),
    }
    assert DEFAULT_POLICY.pass_through == Decimal("0.74")
    assert validate_assumptions(_copy()) == DEFAULT_ASSUMPTIONS


def test_pass_through_changes_cash_flow():
    kwargs = dict(monthly_rent=Decimal("1500"), monthly_cash_flow=Decimal("-90"),
                  annual_rent_growth_pct=Decimal("2"))
    default = forecast_rental_scenario(**kwargs)
    assert default[0]["monthly_cash_flow"] == Decimal("-67.80")
    assert forecast_rental_scenario(**kwargs, pass_through=Decimal("0"))[0]["monthly_cash_flow"] == Decimal("-90.00")


@pytest.mark.parametrize("mutate", [
    lambda a: a.pop("rent_to_cash_flow_pass_through"),
    lambda a: a.update(extra=1),
    lambda a: a["scenario_growth_pct"].pop("base"),
    lambda a: a["scenario_growth_pct"].update(base="abc"),
    lambda a: a["scenario_growth_pct"].update(upside="25"),
    lambda a: a["scenario_growth_pct"].update(conservative="3"),
    lambda a: a.update(rent_to_cash_flow_pass_through="1.5"),
    lambda a: a.update(rent_to_cash_flow_pass_through="-0.1"),
    lambda a: a.update(rent_to_cash_flow_pass_through="NaN"),
])
def test_invalid_assumptions_rejected(mutate):
    assumptions = _copy()
    mutate(assumptions)
    with pytest.raises(ValueError):
        validate_assumptions(assumptions)


def test_load_active_validates_and_returns_version():
    class Result:
        def mappings(self):
            return self

        def one(self):
            return {"policy_version": "v9", "assumptions": _copy()}

    class Conn:
        def execute(self, *_args):
            return Result()

    loaded = load_active_assumptions(Conn())
    assert loaded == RentalAssumptions("v9", DEFAULT_ASSUMPTIONS)


def test_derive_cli_refuses_without_rent_history(capsys, monkeypatch):
    from app.transforms.derive_rental_assumptions import main

    monkeypatch.setattr("sys.argv", ["derive_rental_assumptions"])
    assert main() == 2
    assert "no market rent history" in capsys.readouterr().err

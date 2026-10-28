from decimal import Decimal

from app.market.rental_forecast import forecast_rental_scenario


def test_base_forecast_compounds_rent_and_improves_cash_flow():
    projection = forecast_rental_scenario(
        monthly_rent=Decimal("1500"),
        monthly_cash_flow=Decimal("-90"),
        annual_rent_growth_pct=Decimal("2"),
    )
    assert len(projection) == 5
    assert projection[0]["monthly_rent"] == Decimal("1530.00")
    assert projection[-1]["monthly_rent"] > Decimal("1500")
    assert projection[-1]["monthly_cash_flow"] > Decimal("-90")

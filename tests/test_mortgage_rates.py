from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.market.mortgage_rates import (
    calculate_rate_trend,
    parse_pmms_csv,
)


def test_parse_pmms_csv_normalizes_products_and_methodology():
    records = parse_pmms_csv(
        """date,pmms30,pmms30p,pmms15,pmms15p
11/10/2022,7.08,0.9,6.38,1.0
11/17/2022,6.61,,5.98,
"""
    )

    assert len(records) == 4
    assert records[0]["product_type"] == "30_year_fixed"
    assert records[0]["rate_percent"] == Decimal("7.08")
    assert records[0]["points"] == Decimal("0.9")
    assert records[0]["methodology_version"] == "lender_survey"
    assert records[2]["methodology_version"] == "application_based"


def test_parse_pmms_csv_skips_unavailable_product_observation():
    records = parse_pmms_csv(
        """date,pmms30,pmms30p,pmms15,pmms15p
4/2/1971,7.33,,,,
"""
    )

    assert len(records) == 1
    assert records[0]["product_type"] == "30_year_fixed"


def test_parse_pmms_csv_rejects_bad_input():
    with pytest.raises(ValueError, match="missing columns"):
        parse_pmms_csv("date,pmms30\n4/2/1971,7.33\n")

    with pytest.raises(ValueError, match="Invalid pmms30"):
        parse_pmms_csv(
            "date,pmms30,pmms15\n4/2/1971,not-a-rate,6.00\n"
        )


def test_calculate_rate_trend_uses_fixed_week_windows():
    latest = date(2026, 7, 23)
    observations = [
        {
            "product_type": "30_year_fixed",
            "observation_date": latest - timedelta(weeks=week),
            "rate_percent": Decimal("6.50") - Decimal(week) / 100,
        }
        for week in range(53)
    ]

    trend = calculate_rate_trend(
        observations,
        product_type="30_year_fixed",
    )

    assert trend.as_of_date == latest
    assert trend.current_rate == Decimal("6.50")
    assert trend.change_4_weeks == Decimal("0.04")
    assert trend.change_13_weeks == Decimal("0.13")
    assert trend.change_52_weeks == Decimal("0.52")
    assert trend.average_4_weeks == Decimal("6.485")
    assert trend.direction == "roughly_stable"
    assert trend.observation_count == 53


def test_calculate_rate_trend_requires_observations():
    with pytest.raises(ValueError, match="No observations"):
        calculate_rate_trend([], product_type="30_year_fixed")

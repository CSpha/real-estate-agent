from copy import deepcopy
from datetime import date
from decimal import Decimal

from app.market.comparable_valuation import calculate_comparable_valuation


def _selection(prices_per_sqft, *, tier_key="strict_zip"):
    tier = {
        "key": tier_key,
        "geography": "zip" if tier_key != "city_fallback" else "city",
        "max_sale_age_months": 12 if tier_key == "strict_zip" else 24,
        "sqft_tolerance": Decimal("0.20"),
        "bed_tolerance": Decimal("1"),
        "bath_tolerance": Decimal("1"),
        "lot_tolerance": Decimal("0.50"),
    }
    comparables = []
    for index, price_per_sqft in enumerate(prices_per_sqft, start=1):
        comparables.append(
            {
                "source": "test_comps",
                "source_sale_id": f"sale-{index}",
                "sale_date": date(2026, 6, index),
                "sale_price": Decimal(str(price_per_sqft)) * 1000,
                "sqft": Decimal("1000"),
                "sale_age_days": 30,
                "sqft_difference_pct": Decimal("0"),
                "beds_difference": Decimal("0"),
                "baths_difference": Decimal("0"),
                "lot_difference_pct": None,
            }
        )
    return {
        "selection_version": "comparable-selection-v1",
        "as_of_date": date(2026, 7, 1),
        "subject": {
            "source": "test_feed",
            "source_listing_id": "listing-1",
            "list_price": Decimal("90000"),
            "sqft": Decimal("1000"),
        },
        "selected_tier": tier,
        "available_comparable_count": len(comparables),
        "criteria_not_applied": ["lot_size_acres", "year_built"],
        "comparables": comparables,
    }


def test_weighted_median_limits_retained_outlier_influence():
    result = calculate_comparable_valuation(_selection([100, 110, 500]))

    assert result["status"] == "valued"
    assert result["outlier_filter_applied"] is False
    assert result["weighted_median_price_per_sqft"] == Decimal("110.00")
    assert result["estimated_value"] == Decimal("110000.00")
    assert result["comparables"][2]["outlier"] is True
    assert result["comparables"][2]["included_in_valuation"] is True


def test_outlier_is_excluded_when_three_ordinary_comps_remain():
    result = calculate_comparable_valuation(_selection([100, 105, 110, 400]))

    assert result["status"] == "valued"
    assert result["outlier_filter_applied"] is True
    assert result["included_comparable_count"] == 3
    assert result["weighted_median_price_per_sqft"] == Decimal("105.00")
    assert result["estimated_value"] == Decimal("105000.00")
    assert result["comparables"][3]["included_in_valuation"] is False
    assert result["comparables"][3]["normalized_weight"] == Decimal("0.0")


def test_valuation_requires_subject_square_footage():
    selection = _selection([100, 105, 110])
    selection["subject"]["sqft"] = None

    result = calculate_comparable_valuation(selection)

    assert result["status"] == "insufficient_subject_data"
    assert result["estimated_value"] is None
    assert result["included_comparable_count"] == 0


def test_valuation_requires_three_usable_comparables():
    result = calculate_comparable_valuation(_selection([100, 105]))

    assert result["status"] == "insufficient_comparables"
    assert result["estimated_value"] is None
    assert result["included_comparable_count"] == 2
    assert result["confidence_label"] == "very_low"


def test_city_fallback_reduces_confidence():
    strict_result = calculate_comparable_valuation(_selection([100, 105, 110]))
    city_selection = deepcopy(_selection([100, 105, 110]))
    city_selection["selected_tier"]["key"] = "city_fallback"
    city_selection["selected_tier"]["geography"] = "city"
    city_selection["selected_tier"]["max_sale_age_months"] = 24

    city_result = calculate_comparable_valuation(city_selection)

    assert city_result["confidence_score"] < strict_result["confidence_score"]

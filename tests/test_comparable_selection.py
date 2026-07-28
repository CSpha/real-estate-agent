from decimal import Decimal

import pytest

from app.market.comparable_selection import SELECTION_TIERS, select_comparables


def test_selection_tiers_widen_deterministically():
    assert [tier.key for tier in SELECTION_TIERS] == [
        "strict_zip",
        "broad_zip",
        "city_fallback",
    ]
    assert [tier.max_sale_age_months for tier in SELECTION_TIERS] == [12, 18, 24]
    assert [tier.sqft_tolerance for tier in SELECTION_TIERS] == [
        Decimal("0.20"),
        Decimal("0.30"),
        Decimal("0.40"),
    ]


@pytest.mark.parametrize(
    ("minimum", "maximum", "message"),
    [
        (0, 5, "min_comps must be at least 1"),
        (4, 3, "max_comps must be greater than or equal"),
    ],
)
def test_select_comparables_validates_limits_before_query(
    minimum,
    maximum,
    message,
):
    with pytest.raises(ValueError, match=message):
        select_comparables(
            "sample_feed",
            "1001",
            min_comps=minimum,
            max_comps=maximum,
        )


def test_select_comparables_requires_stable_listing_identifier():
    with pytest.raises(ValueError, match="cannot be empty"):
        select_comparables("", "1001")

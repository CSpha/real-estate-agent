from datetime import date

import pytest

from app.normalization import (
    normalize_listing,
    normalize_property_type,
    normalize_status,
    normalize_zip,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("active", "Active"),
        (" ACTIVE ", "Active"),
        ("under contract", "Pending"),
        ("cancelled", "Canceled"),
        ("Custom Status", "Custom Status"),
        (None, None),
    ],
)
def test_normalize_status(raw, expected):
    assert normalize_status(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Single Family", "SingleFamilyResidence"),
        ("condo", "Condominium"),
        ("Townhome", "Townhouse"),
        ("Custom Type", "Custom Type"),
        (None, None),
    ],
)
def test_normalize_property_type(raw, expected):
    assert normalize_property_type(raw) == expected


def test_normalize_zip_preserves_leading_zeroes():
    assert normalize_zip(1234) == "01234"
    assert normalize_zip("44691") == "44691"


def test_normalize_listing_produces_canonical_values():
    listing = normalize_listing(
        {
            "source": " sample_feed ",
            "source_listing_id": " 1001 ",
            "address": " 123   Main St ",
            "city": " Wooster ",
            "state": "oh",
            "zip": "44691",
            "list_price": 78500,
            "beds": 3,
            "baths": 1,
            "sqft": 1240,
            "lot_size_acres": 0.25,
            "property_type": "Single Family",
            "status": "active",
            "days_on_market": 12,
            "first_seen_date": "2026-04-23",
            "last_seen_date": "2026-04-24",
            "price_per_sqft": 63.31,
        }
    )

    assert listing["source"] == "sample_feed"
    assert listing["source_listing_id"] == "1001"
    assert listing["address"] == "123 Main St"
    assert listing["state"] == "OH"
    assert listing["property_type"] == "SingleFamilyResidence"
    assert listing["status"] == "Active"
    assert listing["lot_size_acres"] == 0.25
    assert listing["first_seen_date"] == date(2026, 4, 23)


def test_normalize_listing_requires_stable_identifiers():
    with pytest.raises(ValueError, match="source cannot be empty"):
        normalize_listing({"source": "", "source_listing_id": "1001"})

    with pytest.raises(ValueError, match="source_listing_id cannot be empty"):
        normalize_listing({"source": "feed", "source_listing_id": ""})

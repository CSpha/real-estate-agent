from datetime import date
from decimal import Decimal

import pytest

from app.ingest.load_comparable_sales import normalize_comparable_sale
from app.normalization import normalize_boolean


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("true", True),
        ("YES", True),
        (1, True),
        ("false", False),
        ("No", False),
        (0, False),
        (None, None),
    ],
)
def test_normalize_boolean(raw, expected):
    assert normalize_boolean(raw) is expected


def test_normalize_comparable_sale_produces_canonical_values():
    sale = normalize_comparable_sale(
        {
            "source": " county_export ",
            "source_sale_id": " sale-123 ",
            "parcel_number": " 56-00100.000 ",
            "sale_date": "2026-04-17",
            "sale_price": "89500.00",
            "address": " 137   Main St ",
            "city": " Wooster ",
            "state": "oh",
            "zip": "44691",
            "county_name": " Wayne ",
            "property_type": "single family",
            "beds": "3",
            "baths": "1.5",
            "sqft": "1350",
            "lot_size_acres": "0.21",
            "year_built": "1930",
            "latitude": "40.7986",
            "longitude": "-81.9375",
            "arms_length": "yes",
        }
    )

    assert sale["source"] == "county_export"
    assert sale["source_sale_id"] == "sale-123"
    assert sale["sale_date"] == date(2026, 4, 17)
    assert sale["sale_price"] == Decimal("89500.00")
    assert sale["address"] == "137 Main St"
    assert sale["state"] == "OH"
    assert sale["property_type"] == "SingleFamilyResidence"
    assert sale["sqft"] == Decimal("1350")
    assert sale["arms_length"] is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("sale_price", "0", "sale_price must be at least"),
        ("year_built", "1600", "year_built must be between"),
        ("latitude", "91", "latitude must be at most"),
        ("arms_length", "unknown", "Expected a boolean"),
    ],
)
def test_normalize_comparable_sale_rejects_invalid_values(field, value, message):
    record = {
        "source": "county_export",
        "source_sale_id": "sale-123",
        "sale_date": "2026-04-17",
        "sale_price": "89500",
        "address": "137 Main St",
        "city": "Wooster",
        "state": "OH",
        "zip": "44691",
        "property_type": "single family",
        "arms_length": "true",
    }
    record[field] = value

    with pytest.raises(ValueError, match=message):
        normalize_comparable_sale(record)

from app.providers.fixture import FixtureProvider


def test_fixture_provider_normalizes_a_listing() -> None:
    provider = FixtureProvider()

    raw_listing = {
        "source": "fixture",
        "source_listing_id": "fixture-001",
        "address": "123 Sample Street",
        "city": "Ann Arbor",
        "state": "MI",
        "zip": "48104",
        "list_price": 245000,
        "beds": 3,
        "baths": 2.5,
        "sqft": 1800,
        "property_type": "SingleFamilyResidence",
        "status": "Active",
        "days_on_market": 14,
        "first_seen_date": "2026-01-01",
        "last_seen_date": "2026-01-01",
        "price_per_sqft": 136.11,
    }

    normalized = provider.normalize(raw_listing)

    assert normalized["source"] == "fixture"
    assert normalized["source_listing_id"] == "fixture-001"
    assert normalized["city"] == "Ann Arbor"
    assert normalized["list_price"] == 245000

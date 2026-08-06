from decimal import Decimal

import pytest

from app.providers.audit_rentcast import summarize_coverage
from app.providers.rentcast import RentCastProvider


class FakeResponse:
    def __init__(self, payload, *, total_count=None):
        self._payload = payload
        self.headers = {}
        if total_count is not None:
            self.headers["X-Total-Count"] = str(total_count)

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def _listing(**overrides):
    listing = {
        "id": "123-Main-St,-Wooster,-OH-44691",
        "addressLine1": "123 Main St",
        "addressLine2": None,
        "city": "Wooster",
        "state": "OH",
        "zipCode": "44691",
        "county": "Wayne",
        "propertyType": "Single Family",
        "bedrooms": 3,
        "bathrooms": 2,
        "squareFootage": 2000,
        "lotSize": 43560,
        "status": "Active",
        "price": 300000,
        "listedDate": "2026-07-01T00:00:00.000Z",
        "lastSeenDate": "2026-08-06T12:00:00.000Z",
        "daysOnMarket": 36,
        "mlsName": "MLS Now",
        "mlsNumber": "510001",
        "history": {"2026-07-01": {"price": 300000}},
    }
    listing.update(overrides)
    return listing


def test_rentcast_provider_fetches_a_bounded_page_without_exposing_key():
    session = FakeSession(FakeResponse([_listing(), _listing(id="second")], total_count=3))
    provider = RentCastProvider(
        api_key="test-secret-key",
        city="Wooster",
        state="oh",
        page_size=2,
        session=session,
    )

    page = provider.fetch_page()

    assert len(page.records) == 2
    assert provider.name == "rentcast:city:wooster:oh"
    assert page.total_count == 3
    assert page.next_cursor == "2"
    _, request = session.calls[0]
    assert request["params"] == {
        "status": "Active",
        "limit": 2,
        "offset": 0,
        "includeTotalCount": "true",
        "city": "Wooster",
        "state": "OH",
    }
    assert request["headers"]["X-Api-Key"] == "test-secret-key"
    assert "test-secret-key" not in str(request["params"])


def test_rentcast_provider_normalizes_listing_fields():
    provider = RentCastProvider(
        api_key="test-key",
        zip_code="44691",
        session=FakeSession(FakeResponse([])),
    )

    normalized = provider.normalize(_listing())

    assert normalized == {
        "source": "rentcast",
        "source_listing_id": "MLS Now:510001",
        "address": "123 Main St",
        "city": "Wooster",
        "state": "OH",
        "zip": "44691",
        "list_price": Decimal("300000"),
        "beds": 3,
        "baths": 2,
        "sqft": Decimal("2000"),
        "lot_size_acres": Decimal("1"),
        "property_type": "Single Family",
        "status": "Active",
        "days_on_market": 36,
        "first_seen_date": "2026-07-01T00:00:00.000Z",
        "last_seen_date": "2026-08-06T12:00:00.000Z",
        "price_per_sqft": Decimal("150"),
    }


def test_rentcast_provider_requires_a_search_scope():
    with pytest.raises(ValueError, match="zip_code or both city and state"):
        RentCastProvider(api_key="test-key")


def test_coverage_summary_excludes_other_counties_and_sensitive_details():
    result = summarize_coverage(
        [
            _listing(),
            _listing(id="second", mlsNumber=None, squareFootage=None),
            _listing(id="third", county="Stark"),
        ],
        expected_county="Wayne",
    )

    assert result["returned_record_count"] == 3
    assert result["county_record_count"] == 2
    assert result["unique_county_property_count"] == 2
    assert result["field_completeness_pct"]["mlsNumber"] == 50.0
    assert result["field_completeness_pct"]["squareFootage"] == 50.0
    assert "address" not in result

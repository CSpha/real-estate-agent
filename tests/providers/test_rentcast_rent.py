from decimal import Decimal

import pytest
import requests

from app.providers.rentcast import RentCastAPIError
from app.providers.rentcast_rent import (
    RENTCAST_RENT_ESTIMATE_URL,
    RentCastRentProvider,
)


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError("failed")
            error.response = self
            raise error

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def listing():
    return {
        "address": "123 Main St",
        "city": "Wooster",
        "state": "OH",
        "zip": "44691",
        "property_type": "SingleFamilyResidence",
        "beds": 3,
        "baths": Decimal("2.5"),
        "sqft": Decimal("1800"),
    }


def test_rent_provider_requests_avm_with_subject_attributes():
    session = FakeSession(FakeResponse({
        "rent": 2100,
        "rentRangeLow": 1950,
        "rentRangeHigh": 2250,
        "comparables": [{"id": "comp-1"}],
    }))
    provider = RentCastRentProvider(api_key="test", session=session)

    result = provider.estimate(listing())

    assert result.monthly_rent == Decimal("2100")
    assert result.rent_range_low == Decimal("1950")
    assert len(result.comparables) == 1
    url, request = session.calls[0]
    assert url == RENTCAST_RENT_ESTIMATE_URL
    assert request["headers"]["X-Api-Key"] == "test"
    assert request["params"]["propertyType"] == "Single Family"
    assert request["params"]["squareFootage"] == Decimal("1800")


def test_rent_provider_rejects_missing_positive_estimate():
    provider = RentCastRentProvider(
        api_key="test",
        session=FakeSession(FakeResponse({"rent": None, "comparables": []})),
    )

    with pytest.raises(RentCastAPIError, match="no positive rent"):
        provider.estimate(listing())

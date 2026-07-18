from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.searches.evaluator import evaluate_listing
from app.searches.models import (
    ListingCandidate,
    SavedSearchUpdate,
    SearchCriteria,
)


def candidate(**overrides) -> ListingCandidate:
    values = {
        "source": "sample_feed",
        "source_listing_id": "1001",
        "address": "123 Main St",
        "city": "Wooster",
        "county_name": "Wayne",
        "state": "OH",
        "zip": "44691",
        "list_price": Decimal("150000"),
        "previous_price": Decimal("160000"),
        "beds": Decimal("3"),
        "baths": Decimal("2"),
        "sqft": Decimal("1500"),
        "lot_size_acres": Decimal("0.5"),
        "property_type": "SingleFamilyResidence",
        "status": "Active",
        "days_on_market": 20,
        "market_score": 80,
        "listing_state_timestamp": datetime(2026, 7, 18, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return ListingCandidate.model_validate(values)


def test_search_requires_at_least_one_criterion():
    with pytest.raises(ValidationError, match="At least one search criterion"):
        SearchCriteria()


def test_range_rejects_reversed_bounds():
    with pytest.raises(ValidationError, match="min cannot be greater"):
        SearchCriteria(price={"min": 200000, "max": 100000})


def test_criteria_normalizes_enumerations_and_locations():
    criteria = SearchCriteria(
        locations=[
            {
                "state": "oh",
                "cities": [" Wooster ", "wooster"],
                "zips": [44691],
            }
        ],
        property_types=["single family", "Single Family Residence"],
        statuses=["active", " ACTIVE "],
    )

    assert criteria.locations[0].state == "OH"
    assert criteria.locations[0].cities == ["Wooster"]
    assert criteria.locations[0].zips == ["44691"]
    assert criteria.property_types == ["SingleFamilyResidence"]
    assert criteria.statuses == ["Active"]


def test_all_supported_criteria_match_at_inclusive_boundaries():
    criteria = SearchCriteria(
        locations=[{"state": "OH", "counties": ["Wayne"]}],
        price={"min": 150000, "max": 150000},
        beds={"min": 3},
        baths={"min": 2},
        sqft={"min": 1500},
        lot_size_acres={"min": 0.5},
        property_types=["SingleFamilyResidence"],
        statuses=["Active"],
        max_days_on_market=20,
        price_drop={"min_amount": 10000, "min_percent": 6.25},
        deal_score={"min": 80},
    )

    result = evaluate_listing(criteria, candidate())

    assert result.is_match is True
    assert result.failed_reasons == []
    assert {reason.criterion for reason in result.match_reasons} == {
        "locations",
        "price",
        "beds",
        "baths",
        "sqft",
        "lot_size_acres",
        "property_types",
        "statuses",
        "max_days_on_market",
        "price_drop",
        "deal_score",
    }


def test_location_entries_are_combined_with_or_semantics():
    criteria = SearchCriteria(
        locations=[
            {"state": "OH", "counties": ["Ashland"]},
            {"state": "OH", "cities": ["Wooster"]},
        ]
    )

    assert evaluate_listing(criteria, candidate()).is_match is True


def test_missing_required_listing_value_fails_with_explanation():
    criteria = SearchCriteria(lot_size_acres={"min": 1})

    result = evaluate_listing(criteria, candidate(lot_size_acres=None))

    assert result.is_match is False
    assert result.failed_reasons[0].criterion == "lot_size_acres"
    assert result.failed_reasons[0].actual is None


def test_price_drop_requires_all_configured_thresholds():
    criteria = SearchCriteria(price_drop={"min_amount": 5000, "min_percent": 10})

    result = evaluate_listing(criteria, candidate())

    assert result.is_match is False
    assert result.failed_reasons[0].criterion == "price_drop"


def test_nonmatching_result_keeps_pass_and_failure_reasons_separate():
    criteria = SearchCriteria(
        price={"max": 200000},
        statuses=["Pending"],
    )

    result = evaluate_listing(criteria, candidate())

    assert result.is_match is False
    assert [reason.criterion for reason in result.match_reasons] == ["price"]
    assert [reason.criterion for reason in result.failed_reasons] == ["statuses"]


def test_saved_search_patch_rejects_empty_or_null_required_fields():
    with pytest.raises(ValidationError, match="At least one field"):
        SavedSearchUpdate()

    with pytest.raises(ValidationError, match="criteria cannot be null"):
        SavedSearchUpdate.model_validate({"criteria": None})

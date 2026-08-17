from app.market.listing_eligibility import evaluate_listing_eligibility


def listing(**overrides):
    record = {
        "address": "1 Main St",
        "city": "Wooster",
        "state": "OH",
        "zip": "44691",
        "list_price": 200000,
        "property_type": "Single Family",
        "status": "Active",
        "sqft": 1400,
    }
    record.update(overrides)
    return record


def test_standard_residential_listing_is_comparable_ready():
    result = evaluate_listing_eligibility(listing())
    assert result.status == "eligible"
    assert result.scoring_eligible is True
    assert result.comparable_ready is True


def test_missing_sqft_stays_visible_for_county_context():
    result = evaluate_listing_eligibility(listing(sqft=None))
    assert result.status == "review"
    assert result.scoring_eligible is True
    assert result.comparable_ready is False


def test_manufactured_and_multifamily_need_specific_models():
    for property_type in ("Manufactured", "Multi-Family"):
        result = evaluate_listing_eligibility(
            listing(property_type=property_type)
        )
        assert result.status == "review"
        assert result.scoring_eligible is False


def test_land_and_inactive_listings_are_excluded():
    assert evaluate_listing_eligibility(
        listing(property_type="Land")
    ).status == "excluded"
    assert evaluate_listing_eligibility(
        listing(status="Inactive")
    ).status == "excluded"

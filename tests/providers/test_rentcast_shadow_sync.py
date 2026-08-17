from __future__ import annotations

from decimal import Decimal

import app.providers.sync_rentcast_shadow as shadow_module


class FakeProvider:
    name = "rentcast:fake"

    def __init__(self, records):
        self.records = records

    def fetch_updated_since(self, cursor=None):
        assert cursor is None
        return self.records, None

    def normalize(self, record):
        return {
            "source": "rentcast",
            "source_listing_id": record["id"],
            "address": record.get("address", "123 Main St"),
            "city": record.get("city", "Wooster"),
            "state": "OH",
            "zip": "44691",
            "list_price": Decimal("200000"),
            "beds": 3,
            "baths": 2,
            "sqft": record.get("sqft"),
            "lot_size_acres": None,
            "property_type": record["property_type"],
            "status": "Active",
            "days_on_market": 30,
            "first_seen_date": "2026-08-01",
            "last_seen_date": "2026-08-16",
            "price_per_sqft": None,
        }


def test_shadow_sync_excludes_land_from_scoring_but_keeps_missing_sqft(monkeypatch):
    records_by_city = {
        "Wooster": [
            {
                "id": "home-with-sqft",
                "county": "Wayne",
                "property_type": "Single Family",
                "sqft": Decimal("1500"),
            },
            {
                "id": "home-without-sqft",
                "county": "Wayne",
                "property_type": "Condo",
                "sqft": None,
            },
            {
                "id": "land",
                "county": "Wayne",
                "property_type": "Land",
                "sqft": None,
            },
            {
                "id": "manufactured",
                "county": "Wayne",
                "property_type": "Manufactured",
                "sqft": Decimal("1100"),
            },
            {
                "id": "outside-county",
                "county": "Stark",
                "property_type": "Single Family",
                "sqft": Decimal("1200"),
            },
        ],
        "Orrville": [
            {
                "id": "home-with-sqft",
                "county": "Wayne",
                "property_type": "Single Family",
                "sqft": Decimal("1500"),
            }
        ],
        "Rittman": [],
    }
    captured = {}

    def fake_ingest(records, **kwargs):
        captured["records"] = records
        captured.update(kwargs)
        eligible = [
            record for record in records if kwargs["scoring_eligibility"](record)
        ]
        captured["eligible"] = eligible
        return {
            "raw_inserted": 3,
            "scoring_eligible": len(eligible),
            "current_changed": len(records),
        }

    monkeypatch.setattr(shadow_module, "ingest_records", fake_ingest)
    monkeypatch.setattr(shadow_module, "snapshot_current_listings", lambda _engine: 2)
    monkeypatch.setattr(shadow_module, "score_listings_against_market", lambda _engine: 2)

    result = shadow_module.sync_rentcast_shadow(
        engine=object(),
        provider_factory=lambda city: FakeProvider(records_by_city[city]),
    )

    assert result.returned_count == 6
    assert result.county_count == 5
    assert result.unique_county_count == 4
    assert result.excluded_count == 2
    assert result.review_count == 2
    assert result.scoring_eligible_count == 2
    assert result.missing_sqft_count == 1
    assert result.alert_eligible is False
    assert captured["alert_eligible"] is False
    assert {record["source_listing_id"] for record in captured["records"]} == {
        "home-with-sqft",
        "home-without-sqft",
        "land",
        "manufactured",
    }
    assert {record["source_listing_id"] for record in captured["eligible"]} == {
        "home-with-sqft",
        "home-without-sqft",
    }


def test_scoring_eligibility_is_neutral_for_missing_square_footage():
    base = {
        "address": "123 Main St",
        "city": "Wooster",
        "state": "OH",
        "zip": "44691",
        "list_price": 200000,
        "status": "Active",
        "sqft": None,
    }
    assert shadow_module.is_scoring_eligible(
        {**base, "property_type": "Single Family"}
    )
    assert not shadow_module.is_scoring_eligible(
        {**base, "property_type": "land"}
    )
    assert not shadow_module.is_scoring_eligible(
        {**base, "property_type": "Manufactured", "sqft": 1200}
    )

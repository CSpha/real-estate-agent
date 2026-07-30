from __future__ import annotations

from typing import Any

from app.providers.base import ProviderAdapter


class FixtureProvider:
    """Simple fixture-backed provider used for initial adapter tests."""

    name = "fixture"

    def __init__(self, records: list[dict[str, Any]] | None = None) -> None:
        self._records = records or [
            {
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
        ]

    def fetch_updated_since(self, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        return list(self._records), cursor

    def normalize(self, raw_listing: dict[str, Any]) -> dict[str, Any]:
        return {
            "source": raw_listing.get("source", self.name),
            "source_listing_id": raw_listing.get("source_listing_id", raw_listing.get("source_listing_id")),
            "address": raw_listing.get("address"),
            "city": raw_listing.get("city"),
            "state": raw_listing.get("state"),
            "zip": raw_listing.get("zip"),
            "list_price": raw_listing.get("list_price"),
            "beds": raw_listing.get("beds"),
            "baths": raw_listing.get("baths"),
            "sqft": raw_listing.get("sqft"),
            "property_type": raw_listing.get("property_type"),
            "status": raw_listing.get("status"),
            "days_on_market": raw_listing.get("days_on_market"),
            "first_seen_date": raw_listing.get("first_seen_date"),
            "last_seen_date": raw_listing.get("last_seen_date"),
            "price_per_sqft": raw_listing.get("price_per_sqft"),
        }

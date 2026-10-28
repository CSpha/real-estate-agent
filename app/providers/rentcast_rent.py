from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import requests

from app.config import get_settings, require_setting
from app.providers.rentcast import RentCastAPIError


RENTCAST_RENT_ESTIMATE_URL = "https://api.rentcast.io/v1/avm/rent/long-term"
PROPERTY_TYPE_MAP = {
    "SingleFamilyResidence": "Single Family",
    "Condominium": "Condo",
    "Townhouse": "Townhouse",
    "Manufactured": "Manufactured",
    "MultiFamily": "Multi-Family",
    "Apartment": "Apartment",
}


@dataclass(frozen=True)
class RentEstimate:
    monthly_rent: Decimal
    rent_range_low: Decimal | None
    rent_range_high: Decimal | None
    comparables: list[dict[str, Any]]
    request: dict[str, Any]
    response: dict[str, Any]


class RentCastRentProvider:
    provider_name = "rentcast_long_term_avm"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        timeout_seconds: float = 20,
        session: Any | None = None,
        max_radius: int = 5,
        days_old: int = 270,
        comp_count: int = 20,
    ) -> None:
        if not 5 <= comp_count <= 25:
            raise ValueError("comp_count must be between 5 and 25")
        self._api_key = require_setting(
            api_key or get_settings().rentcast_api_key,
            "RENTCAST_API_KEY",
        )
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self.max_radius = max_radius
        self.days_old = days_old
        self.comp_count = comp_count

    def build_request(self, listing: dict[str, Any]) -> dict[str, Any]:
        address_parts = [
            listing.get("address"),
            listing.get("city"),
            listing.get("state"),
            listing.get("zip"),
        ]
        params: dict[str, Any] = {
            "address": ", ".join(str(value).strip() for value in address_parts if value),
            "maxRadius": self.max_radius,
            "daysOld": self.days_old,
            "compCount": self.comp_count,
            "lookupSubjectAttributes": "true",
        }
        property_type = PROPERTY_TYPE_MAP.get(str(listing.get("property_type")))
        if property_type:
            params["propertyType"] = property_type
        for source_key, request_key in (
            ("beds", "bedrooms"),
            ("baths", "bathrooms"),
            ("sqft", "squareFootage"),
        ):
            value = listing.get(source_key)
            if value is not None and Decimal(str(value)) >= 0:
                params[request_key] = value
        if not params["address"]:
            raise ValueError("A property address is required for a rent estimate")
        return params

    def estimate(self, listing: dict[str, Any]) -> RentEstimate:
        params = self.build_request(listing)
        try:
            response = self._session.get(
                RENTCAST_RENT_ESTIMATE_URL,
                headers={"Accept": "application/json", "X-Api-Key": self._api_key},
                params=params,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            detail = f" HTTP {status_code}" if status_code is not None else ""
            raise RentCastAPIError(f"RentCast rent estimate failed.{detail}") from exc

        try:
            payload = response.json()
        except (ValueError, requests.RequestException) as exc:
            raise RentCastAPIError("RentCast returned invalid rent-estimate JSON") from exc
        if not isinstance(payload, dict):
            raise RentCastAPIError("RentCast returned an unexpected rent-estimate shape")
        monthly_rent = _positive_decimal(payload.get("rent"))
        if monthly_rent is None:
            raise RentCastAPIError("RentCast returned no positive rent estimate")
        comparables = payload.get("comparables") or []
        if not isinstance(comparables, list):
            raise RentCastAPIError("RentCast returned invalid rent comparables")
        return RentEstimate(
            monthly_rent=monthly_rent,
            rent_range_low=_positive_decimal(payload.get("rentRangeLow")),
            rent_range_high=_positive_decimal(payload.get("rentRangeHigh")),
            comparables=[item for item in comparables if isinstance(item, dict)],
            request=params,
            response=payload,
        )


def _positive_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    return result if result > 0 else None

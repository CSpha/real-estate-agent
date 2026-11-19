from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from math import isnan
from typing import Any


STATUS_ALIASES = {
    "active": "Active",
    "coming soon": "ComingSoon",
    "comingsoon": "ComingSoon",
    "pending": "Pending",
    "under contract": "Pending",
    "sold": "Sold",
    "closed": "Sold",
    "expired": "Expired",
    "cancelled": "Canceled",
    "canceled": "Canceled",
    "withdrawn": "Withdrawn",
}

PROPERTY_TYPE_ALIASES = {
    "single family": "SingleFamilyResidence",
    "single-family": "SingleFamilyResidence",
    "single family residence": "SingleFamilyResidence",
    "condo": "Condominium",
    "condominium": "Condominium",
    "townhouse": "Townhouse",
    "townhome": "Townhouse",
    "multi family": "MultiFamily",
    "multi-family": "MultiFamily",
    "land": "Land",
}


def none_if_missing(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and isnan(value):
        return None
    return value


def normalize_status(value: Any) -> str | None:
    value = none_if_missing(value)
    if value is None:
        return None

    normalized = " ".join(str(value).strip().split())
    if not normalized:
        return None
    return STATUS_ALIASES.get(normalized.casefold(), normalized)


def normalize_property_type(value: Any) -> str | None:
    value = none_if_missing(value)
    if value is None:
        return None

    normalized = " ".join(str(value).strip().split())
    if not normalized:
        return None
    return PROPERTY_TYPE_ALIASES.get(normalized.casefold(), normalized)


def normalize_state(value: Any) -> str | None:
    value = none_if_missing(value)
    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized or None


def normalize_zip(value: Any) -> str | None:
    value = none_if_missing(value)
    if value is None:
        return None

    if isinstance(value, float) and value.is_integer():
        value = int(value)

    normalized = str(value).strip()
    if normalized.endswith(".0") and normalized[:-2].isdigit():
        normalized = normalized[:-2]
    return (
        normalized.zfill(5)
        if normalized.isdigit() and len(normalized) < 5
        else normalized
    )


def normalize_date(value: Any) -> date | None:
    value = none_if_missing(value)
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def normalize_number(value: Any) -> int | float | Decimal | None:
    value = none_if_missing(value)
    if value is None or value == "":
        return None
    return value


def normalize_integer(value: Any) -> int | None:
    value = none_if_missing(value)
    if value is None or value == "":
        return None
    return int(value)


def normalize_boolean(value: Any) -> bool | None:
    value = none_if_missing(value)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().casefold()
    if normalized in {"true", "t", "yes", "y", "1"}:
        return True
    if normalized in {"false", "f", "no", "n", "0"}:
        return False
    raise ValueError(f"Expected a boolean value, got: {value!r}")


COORDINATE_PLACES = Decimal("0.000001")


def normalize_coordinates(
    latitude: Any,
    longitude: Any,
) -> tuple[Decimal | None, Decimal | None]:
    """Return a (latitude, longitude) pair rounded to six places, or (None, None).

    Coordinates are optional, so an unusable pair is dropped rather than
    quarantining the listing: a missing half, a non-number, an out-of-range
    value, or the (0, 0) placeholder some feeds use for "unknown".
    """
    lat = _coordinate(latitude, 90)
    lon = _coordinate(longitude, 180)
    if lat is None or lon is None or (lat == 0 and lon == 0):
        return None, None
    return lat, lon


def _coordinate(value: Any, limit: int) -> Decimal | None:
    value = none_if_missing(value)
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        result = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    if not result.is_finite() or not -limit <= result <= limit:
        return None
    return result.quantize(COORDINATE_PLACES)


def normalize_listing(record: dict[str, Any]) -> dict[str, Any]:
    source = str(record.get("source", "")).strip()
    source_listing_id = str(record.get("source_listing_id", "")).strip()
    if not source:
        raise ValueError("Listing source cannot be empty")
    if not source_listing_id:
        raise ValueError("Listing source_listing_id cannot be empty")

    latitude, longitude = normalize_coordinates(
        record.get("latitude"), record.get("longitude")
    )
    return {
        "source": source,
        "source_listing_id": source_listing_id,
        "address": _normalize_text(record.get("address")),
        "city": _normalize_text(record.get("city")),
        "state": normalize_state(record.get("state")),
        "zip": normalize_zip(record.get("zip")),
        "list_price": normalize_number(record.get("list_price")),
        "beds": normalize_number(record.get("beds")),
        "baths": normalize_number(record.get("baths")),
        "sqft": normalize_number(record.get("sqft")),
        "lot_size_acres": normalize_number(record.get("lot_size_acres")),
        "property_type": normalize_property_type(record.get("property_type")),
        "status": normalize_status(record.get("status")),
        "days_on_market": normalize_integer(record.get("days_on_market")),
        "first_seen_date": normalize_date(record.get("first_seen_date")),
        "last_seen_date": normalize_date(record.get("last_seen_date")),
        "price_per_sqft": normalize_number(record.get("price_per_sqft")),
        "latitude": latitude,
        "longitude": longitude,
    }


def _normalize_text(value: Any) -> str | None:
    value = none_if_missing(value)
    if value is None:
        return None
    normalized = " ".join(str(value).strip().split())
    return normalized or None

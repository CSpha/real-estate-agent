from __future__ import annotations

import random
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

import requests

from app.config import get_settings, require_setting


RENTCAST_SALE_LISTINGS_URL = "https://api.rentcast.io/v1/listings/sale"
RENTCAST_MAX_PAGE_SIZE = 500
# Each retry is another billable request, so keep this small; see
# RENTCAST_MAX_REQUESTS_PER_RUN in app.shadow.scheduler.
DEFAULT_MAX_RETRIES = 2
RETRY_BASE_SECONDS = 2.0
MAX_RETRY_AFTER_SECONDS = 60.0
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
SQFT_PER_ACRE = Decimal("43560")


class RentCastAPIError(RuntimeError):
    """Raised when RentCast cannot return a valid listing page."""


@dataclass(frozen=True)
class RentCastPage:
    records: list[dict[str, Any]]
    total_count: int | None
    next_cursor: str | None


class RentCastProvider:
    """Read active sale listings from RentCast's REST API."""

    source_name = "rentcast"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        city: str | None = None,
        state: str | None = None,
        zip_code: str | None = None,
        page_size: int = 100,
        status: str = "Active",
        timeout_seconds: float = 20,
        session: Any | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        if not 1 <= page_size <= RENTCAST_MAX_PAGE_SIZE:
            raise ValueError(
                f"page_size must be between 1 and {RENTCAST_MAX_PAGE_SIZE}"
            )
        if zip_code is None and not (city and state):
            raise ValueError("Provide zip_code or both city and state")
        if zip_code is not None and city is not None:
            raise ValueError("Use either zip_code or city/state, not both")
        if status not in {"Active", "Inactive"}:
            raise ValueError("status must be Active or Inactive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        configured_key = api_key or get_settings().rentcast_api_key
        self._api_key = require_setting(configured_key, "RENTCAST_API_KEY")
        self.city = city
        self.state = state.upper() if state else None
        self.zip_code = zip_code
        scope = (
            f"zip:{zip_code}"
            if zip_code is not None
            else f"city:{self.city}:{self.state}"
        )
        self.name = f"{self.source_name}:{scope.casefold()}"
        self.page_size = page_size
        self.status = status
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self.max_retries = max_retries
        self._sleep = sleep
        self._jitter = jitter
        self.request_count = 0

    def fetch_page(self, cursor: str | None = None) -> RentCastPage:
        try:
            offset = int(cursor) if cursor is not None else 0
        except ValueError as exc:
            raise ValueError("RentCast cursor must be a numeric offset") from exc
        if offset < 0:
            raise ValueError("RentCast cursor cannot be negative")

        params: dict[str, Any] = {
            "status": self.status,
            "limit": self.page_size,
            "offset": offset,
            "includeTotalCount": "true",
        }
        if self.zip_code is not None:
            params["zipCode"] = self.zip_code
        else:
            params["city"] = self.city
            params["state"] = self.state

        response = self._get_with_retries(params)

        try:
            payload = response.json()
        except (ValueError, requests.RequestException) as exc:
            raise RentCastAPIError("RentCast returned invalid JSON") from exc
        if not isinstance(payload, list) or not all(
            isinstance(record, dict) for record in payload
        ):
            raise RentCastAPIError("RentCast returned an unexpected response shape")

        total_header = response.headers.get("X-Total-Count")
        try:
            total_count = int(total_header) if total_header is not None else None
        except ValueError as exc:
            raise RentCastAPIError("RentCast returned an invalid total count") from exc

        next_offset = offset + len(payload)
        has_more = (
            next_offset < total_count
            if total_count is not None
            else len(payload) == self.page_size
        )
        return RentCastPage(
            records=payload,
            total_count=total_count,
            next_cursor=str(next_offset) if has_more else None,
        )

    def _get_with_retries(self, params: dict[str, Any]) -> Any:
        """GET one page, retrying timeouts, 429s, and 5xx a bounded number of times."""

        for attempt in range(self.max_retries + 1):
            self.request_count += 1
            try:
                response = self._session.get(
                    RENTCAST_SALE_LISTINGS_URL,
                    headers={
                        "Accept": "application/json",
                        "X-Api-Key": self._api_key,
                    },
                    params=params,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                status_code = getattr(
                    getattr(exc, "response", None), "status_code", None
                )
                delay = self._retry_delay(exc, status_code, attempt)
                if delay is None or attempt == self.max_retries:
                    raise RentCastAPIError(
                        _failure_message(status_code, attempts=attempt + 1)
                    ) from exc
                print(
                    "RentCast request failed"
                    f"{f' with HTTP {status_code}' if status_code else ''}; "
                    f"retrying in {delay:.1f}s "
                    f"(retry {attempt + 1} of {self.max_retries}).",
                    flush=True,
                )
                self._sleep(delay)
        raise AssertionError("unreachable")

    def _retry_delay(
        self,
        exc: requests.RequestException,
        status_code: int | None,
        attempt: int,
    ) -> float | None:
        if status_code is None:
            if not isinstance(exc, (requests.Timeout, requests.ConnectionError)):
                return None
        elif status_code not in RETRYABLE_STATUS_CODES:
            return None
        base = RETRY_BASE_SECONDS * 2**attempt
        delay = base / 2 + self._jitter() * base / 2
        retry_after = _retry_after_seconds(getattr(exc, "response", None))
        if retry_after is not None:
            if retry_after > MAX_RETRY_AFTER_SECONDS:
                return None
            delay = max(delay, retry_after)
        return delay

    def fetch_updated_since(
        self,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        page = self.fetch_page(cursor)
        return page.records, page.next_cursor

    def normalize(self, raw_listing: dict[str, Any]) -> dict[str, Any]:
        source_listing_id = _source_listing_id(raw_listing)
        sqft = _positive_decimal(raw_listing.get("squareFootage"))
        price = _positive_decimal(raw_listing.get("price"))
        lot_size_sqft = _positive_decimal(raw_listing.get("lotSize"))
        address_parts = [
            raw_listing.get("addressLine1"),
            raw_listing.get("addressLine2"),
        ]
        address = " ".join(
            str(part).strip() for part in address_parts if part
        ) or None
        return {
            "source": self.source_name,
            "source_listing_id": source_listing_id,
            "address": address,
            "city": raw_listing.get("city"),
            "state": raw_listing.get("state"),
            "zip": raw_listing.get("zipCode"),
            "list_price": price,
            "beds": raw_listing.get("bedrooms"),
            "baths": raw_listing.get("bathrooms"),
            "sqft": sqft,
            "lot_size_acres": (
                lot_size_sqft / SQFT_PER_ACRE if lot_size_sqft else None
            ),
            "property_type": raw_listing.get("propertyType"),
            "status": raw_listing.get("status"),
            "days_on_market": raw_listing.get("daysOnMarket"),
            "first_seen_date": raw_listing.get("listedDate"),
            "last_seen_date": raw_listing.get("lastSeenDate"),
            "price_per_sqft": price / sqft if price and sqft else None,
        }


def _failure_message(status_code: int | None, *, attempts: int) -> str:
    if status_code in {401, 403}:
        detail = (
            f" HTTP {status_code}. Confirm the API key is valid and "
            "the free Developer API plan is active"
        )
    else:
        detail = f" HTTP {status_code}" if status_code is not None else ""
    suffix = f" after {attempts} attempts" if attempts > 1 else ""
    return f"RentCast listing request failed{suffix}.{detail}"


def _retry_after_seconds(response: Any) -> float | None:
    if response is None:
        return None
    raw = getattr(response, "headers", {}).get("Retry-After")
    if raw is None:
        return None
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        return None


def _source_listing_id(raw_listing: dict[str, Any]) -> str:
    mls_name = str(raw_listing.get("mlsName") or "").strip()
    mls_number = str(raw_listing.get("mlsNumber") or "").strip()
    if mls_name and mls_number:
        return f"{mls_name}:{mls_number}"
    if mls_number:
        return mls_number
    rentcast_id = str(raw_listing.get("id") or "").strip()
    if not rentcast_id:
        raise ValueError("RentCast listing has no stable identifier")
    return rentcast_id


def _positive_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    return result if result > 0 else None

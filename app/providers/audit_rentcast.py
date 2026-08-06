from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Any

from app.providers.rentcast import RentCastProvider


DEFAULT_PILOT_CITIES = ("Wooster", "Orrville", "Rittman")
COMPLETENESS_FIELDS = (
    "price",
    "bedrooms",
    "bathrooms",
    "squareFootage",
    "daysOnMarket",
    "lastSeenDate",
    "mlsNumber",
    "history",
)


def summarize_coverage(
    records: list[dict[str, Any]],
    *,
    expected_county: str,
) -> dict[str, Any]:
    county_records = [
        record
        for record in records
        if str(record.get("county") or "").casefold()
        == expected_county.casefold()
    ]
    unique_records = {
        str(record.get("id") or index): record
        for index, record in enumerate(county_records)
    }
    records_to_measure = list(unique_records.values())
    denominator = len(records_to_measure)
    completeness_pct = {
        field: round(
            sum(record.get(field) not in (None, "", {}) for record in records_to_measure)
            / denominator
            * 100,
            1,
        )
        if denominator
        else 0.0
        for field in COMPLETENESS_FIELDS
    }
    return {
        "expected_county": expected_county,
        "returned_record_count": len(records),
        "county_record_count": len(county_records),
        "unique_county_property_count": denominator,
        "property_types": dict(
            sorted(
                Counter(
                    str(record.get("propertyType") or "Missing")
                    for record in records_to_measure
                ).items()
            )
        ),
        "mls_names": dict(
            sorted(
                Counter(
                    str(record.get("mlsName") or "Missing")
                    for record in records_to_measure
                ).items()
            )
        ),
        "field_completeness_pct": completeness_pct,
    }


def audit_rentcast_coverage(
    *,
    cities: list[str],
    state: str,
    expected_county: str,
    max_requests: int = 3,
    page_size: int = 500,
) -> dict[str, Any]:
    if not 1 <= max_requests <= 10:
        raise ValueError("max_requests must be between 1 and 10")
    if len(cities) > max_requests:
        raise ValueError("The city count exceeds the API request budget")

    records = []
    city_results = []
    for city in cities:
        provider = RentCastProvider(
            city=city,
            state=state,
            page_size=page_size,
        )
        page = provider.fetch_page()
        records.extend(page.records)
        city_results.append(
            {
                "city": city,
                "reported_total_count": page.total_count,
                "returned_count": len(page.records),
                "all_results_returned": (
                    page.total_count is not None
                    and len(page.records) >= page.total_count
                ),
            }
        )

    summary = summarize_coverage(records, expected_county=expected_county)
    summary.update(
        {
            "provider": "rentcast",
            "state": state.upper(),
            "cities": city_results,
            "api_requests_used": len(cities),
            "read_only": True,
        }
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a bounded, read-only RentCast listing coverage audit without "
            "storing listing records."
        )
    )
    parser.add_argument("--cities", nargs="+", default=list(DEFAULT_PILOT_CITIES))
    parser.add_argument("--state", default="OH")
    parser.add_argument("--county", default="Wayne")
    parser.add_argument("--max-requests", type=int, default=3)
    parser.add_argument("--page-size", type=int, default=500)
    args = parser.parse_args()

    result = audit_rentcast_coverage(
        cities=args.cities,
        state=args.state,
        expected_county=args.county,
        max_requests=args.max_requests,
        page_size=args.page_size,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

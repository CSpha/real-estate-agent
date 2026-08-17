from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import Engine

from app.ingest.load_comparable_sales import ingest_comparable_records


FEATURE_URL = (
    "https://services6.arcgis.com/WiOy9S7NUTWyXUe4/arcgis/rest/services/"
    "parcel_joined/FeatureServer/0/query"
)
SOURCE = "wayne_county_auditor_arcgis_provisional"
PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = (15, 60)
OUT_FIELDS = ",".join(
    (
        "OBJECTID",
        "Parcel",
        "PPAddress",
        "PPAmount",
        "PPSaleDate",
        "PPAcres",
        "PPLivingArea",
        "PPYearBuilt",
        "PPDwellCount",
        "PPTotalValue",
        "PPClassCode",
        "PPClassNumber",
        "PPListed",
    )
)

WAYNE_CITIES = tuple(
    sorted(
        {
            "Apple Creek",
            "Barberton",
            "Big Prairie",
            "Burbank",
            "Clinton",
            "Congress",
            "Creston",
            "Dalton",
            "Doylestown",
            "Dundee",
            "Fredericksburg",
            "Jeromesville",
            "Kidron",
            "Lakeville",
            "Marshallville",
            "Mount Eaton",
            "Navarre",
            "North Lawrence",
            "Orrville",
            "Rittman",
            "Shreve",
            "Smithville",
            "Sterling",
            "West Salem",
            "Wooster",
        },
        key=len,
        reverse=True,
    )
)

ADDRESS_OVERRIDES = {
    "121 N PORTAGE ST": ("121 N Portage St", "Doylestown", "44230"),
    "2520 BARRINGTON WAY UNIT 505": (
        "2520 Barrington Way Unit 505",
        "Wooster",
        "44691",
    ),
}


@dataclass(frozen=True)
class WayneComparableImportResult:
    fetched_count: int
    normalized_count: int
    proxy_eligible_count: int
    bundle_excluded_count: int
    raw_inserted: int
    current_changed: int
    start_date: date
    end_date: date


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, AttributeError) as exc:
        raise ValueError(f"Invalid numeric auditor value: {value!r}") from exc
    return result if result.is_finite() else None


def _integer(value: Any) -> int | None:
    number = _decimal(value)
    return int(number) if number is not None else None


def _sale_date(value: Any) -> date:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(
            value / 1000, tz=timezone.utc
        ).date()
    return date.fromisoformat(str(value).strip()[:10])


def _split_address(value: Any) -> tuple[str, str, str]:
    full = " ".join(str(value or "").split("|", 1)[0].strip().split())
    if not full:
        raise ValueError("Auditor sale is missing PPAddress")
    if full.upper() in ADDRESS_OVERRIDES:
        return ADDRESS_OVERRIDES[full.upper()]
    parts = full.rsplit(" OH ", 1)
    if len(parts) != 2 or not parts[1][:5].isdigit():
        raise ValueError(f"Could not parse Ohio address: {full!r}")
    before_state, zip_code = parts
    for city in WAYNE_CITIES:
        suffix = " " + city.upper()
        if before_state.upper().endswith(suffix):
            return before_state[: -len(suffix)], city, zip_code[:5]
    raise ValueError(f"Could not identify Wayne County city: {full!r}")


def _property_type(class_code: int | None) -> str | None:
    if class_code is None:
        return None
    base = class_code - (class_code % 10)
    return {
        510: "SingleFamilyResidence",
        520: "MultiFamily",
        530: "MultiFamily",
        550: "Condominium",
        560: "Manufactured",
    }.get(base)


def _proxy_arms_length(attributes: dict[str, Any]) -> bool:
    """Conservative proxy; the ArcGIS layer omits the auditor Valid flag."""

    amount = _decimal(attributes.get("PPAmount"))
    sqft = _decimal(attributes.get("PPLivingArea"))
    total_value = _decimal(attributes.get("PPTotalValue"))
    property_type = _property_type(_integer(attributes.get("PPClassCode")))
    if property_type not in {"SingleFamilyResidence", "Condominium"}:
        return False
    if str(attributes.get("PPClassNumber") or "").strip().upper() != "R":
        return False
    if amount is None or amount < Decimal("25000"):
        return False
    if sqft is None or sqft < Decimal("300"):
        return False
    if total_value is None or total_value <= 0:
        return False
    ratio = amount / total_value
    return Decimal("0.25") <= ratio <= Decimal("4")


def fetch_wayne_county_sales(
    *,
    start_date: date,
    end_date: date,
    session: Any | None = None,
) -> list[dict[str, Any]]:
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    session = session or requests.Session()
    records: list[dict[str, Any]] = []
    offset = 0
    where = (
        f"PPSaleDate >= DATE '{start_date.isoformat()}' "
        f"AND PPSaleDate < DATE '{(end_date + relativedelta(days=1)).isoformat()}' "
        "AND PPAmount > 0"
    )
    while True:
        response = session.get(
            FEATURE_URL,
            params={
                "f": "json",
                "where": where,
                "outFields": OUT_FIELDS,
                "returnGeometry": "false",
                "orderByFields": "PPSaleDate ASC,OBJECTID ASC",
                "resultOffset": offset,
                "resultRecordCount": PAGE_SIZE,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "real-estate-agent/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(f"Wayne County ArcGIS error: {payload['error']}")
        page = [feature["attributes"] for feature in payload.get("features", [])]
        records.extend(page)
        if len(page) < PAGE_SIZE:
            return records
        offset += len(page)


def normalize_wayne_sale(attributes: dict[str, Any]) -> dict[str, Any]:
    address, city, zip_code = _split_address(attributes.get("PPAddress"))
    sale_date = _sale_date(attributes.get("PPSaleDate"))
    parcel = str(attributes.get("Parcel") or "").strip()
    if not parcel:
        raise ValueError("Auditor sale is missing Parcel")
    return {
        "source": SOURCE,
        "source_sale_id": f"{parcel}:{sale_date.isoformat()}",
        "parcel_number": parcel,
        "sale_date": sale_date,
        "sale_price": _decimal(attributes.get("PPAmount")),
        "address": address.title(),
        "city": city,
        "state": "OH",
        "zip": zip_code,
        "county_name": "Wayne",
        "property_type": _property_type(_integer(attributes.get("PPClassCode"))),
        "beds": None,
        "baths": None,
        "sqft": _decimal(attributes.get("PPLivingArea")),
        "lot_size_acres": _decimal(attributes.get("PPAcres")),
        "year_built": _integer(attributes.get("PPYearBuilt")),
        "latitude": None,
        "longitude": None,
        "arms_length": _proxy_arms_length(attributes),
        "auditor_proxy_method": "residential_class_price_value_sqft_v1",
        "auditor_object_id": attributes.get("OBJECTID"),
        "auditor_current_owner": attributes.get("PPListed"),
    }


def _exclude_bundle_duplicates(records: list[dict[str, Any]]) -> int:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for record in records:
        key = (
            record["sale_date"],
            record["sale_price"],
            str(record.get("auditor_current_owner") or "").strip().casefold(),
        )
        groups.setdefault(key, []).append(record)
    excluded = 0
    for group in groups.values():
        eligible = [record for record in group if record["arms_length"]]
        if len(eligible) <= 1:
            continue
        eligible.sort(key=lambda row: row.get("sqft") or Decimal("0"), reverse=True)
        for duplicate in eligible[1:]:
            duplicate["arms_length"] = False
            duplicate["auditor_proxy_method"] += ":bundle_duplicate"
            excluded += 1
    return excluded


def import_wayne_county_comparable_sales(
    *,
    months: int = 24,
    as_of_date: date | None = None,
    engine: Engine | None = None,
    session: Any | None = None,
) -> WayneComparableImportResult:
    if months < 1 or months > 60:
        raise ValueError("months must be between 1 and 60")
    end_date = as_of_date or date.today()
    start_date = end_date - relativedelta(months=months)
    attributes = fetch_wayne_county_sales(
        start_date=start_date,
        end_date=end_date,
        session=session,
    )
    records: list[dict[str, Any]] = []
    skipped: Counter[str] = Counter()
    for item in attributes:
        if _property_type(_integer(item.get("PPClassCode"))) is None:
            continue
        try:
            record = normalize_wayne_sale(item)
        except ValueError as exc:
            skipped[str(exc)] += 1
            continue
        records.append(record)
    bundle_excluded = _exclude_bundle_duplicates(records)
    counts = ingest_comparable_records(records, engine=engine)
    result = WayneComparableImportResult(
        fetched_count=len(attributes),
        normalized_count=len(records),
        proxy_eligible_count=sum(record["arms_length"] for record in records),
        bundle_excluded_count=bundle_excluded,
        raw_inserted=counts["raw_inserted"],
        current_changed=counts["current_changed"],
        start_date=start_date,
        end_date=end_date,
    )
    if skipped:
        print(f"Skipped {sum(skipped.values())} unparseable auditor row(s).")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import provisional Wayne County property-level sales."
    )
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    args = parser.parse_args()
    result = import_wayne_county_comparable_sales(
        months=args.months,
        as_of_date=args.as_of_date,
    )
    print("Wayne County comparable-sales import complete:")
    for key, value in asdict(result).items():
        print(f"  {key}: {value}")
    print("No searches were evaluated and no alerts were queued or sent.")


if __name__ == "__main__":
    main()

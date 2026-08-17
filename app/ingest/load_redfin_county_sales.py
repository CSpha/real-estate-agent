from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable

import requests
from sqlalchemy import Engine, text

from app.db import get_engine


REDFIN_COUNTY_CSV_URL = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/"
    "redfin_data_center/housing_market/monthly/all_counties.csv"
)
REDFIN_SOURCE_FILE = "redfin:housing_market/monthly/all_counties.csv"
WAYNE_REGION_NAME = "Wayne County, OH"
REQUEST_TIMEOUT_SECONDS = (15, 180)

REQUIRED_COLUMNS = {
    "FREQUENCY",
    "PERIOD END",
    "REGION TYPE",
    "REGION NAME",
    "HOMES SOLD",
    "MEDIAN SALE PRICE NSA ($)",
    "MEDIAN DAYS ON MARKET (DAYS)",
    "NEW LISTINGS",
    "ACTIVE LISTINGS",
}

UPSERT_SQL = text(
    """
    INSERT INTO county_sales (
        county_name,
        state,
        period_date,
        median_sale_price,
        homes_sold,
        new_listings,
        active_listings,
        median_days_on_market,
        source_file
    )
    VALUES (
        :county_name,
        :state,
        :period_date,
        :median_sale_price,
        :homes_sold,
        :new_listings,
        :active_listings,
        :median_days_on_market,
        :source_file
    )
    ON CONFLICT (county_name, state, period_date)
    DO UPDATE SET
        median_sale_price = EXCLUDED.median_sale_price,
        homes_sold = EXCLUDED.homes_sold,
        new_listings = EXCLUDED.new_listings,
        active_listings = EXCLUDED.active_listings,
        median_days_on_market = EXCLUDED.median_days_on_market,
        source_file = EXCLUDED.source_file,
        loaded_at = CURRENT_TIMESTAMP
    WHERE ROW(
        county_sales.median_sale_price,
        county_sales.homes_sold,
        county_sales.new_listings,
        county_sales.active_listings,
        county_sales.median_days_on_market,
        county_sales.source_file
    ) IS DISTINCT FROM ROW(
        EXCLUDED.median_sale_price,
        EXCLUDED.homes_sold,
        EXCLUDED.new_listings,
        EXCLUDED.active_listings,
        EXCLUDED.median_days_on_market,
        EXCLUDED.source_file
    )
    """
)


@dataclass(frozen=True)
class ParsedRedfinCountySales:
    rows_scanned: int
    records: list[dict[str, Any]]


@dataclass(frozen=True)
class RedfinCountySalesImportResult:
    rows_scanned: int
    matched_rows: int
    inserted_or_changed: int
    earliest_period: date
    latest_period: date


def _optional_decimal(value: str | None, field_name: str) -> Decimal | None:
    cleaned = str(value or "").strip()
    if not cleaned or cleaned.casefold() == "na":
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid {field_name}: {value!r}") from exc


def _optional_integer(value: str | None, field_name: str) -> int | None:
    number = _optional_decimal(value, field_name)
    if number is None:
        return None
    return int(number.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def parse_redfin_county_sales(csv_lines: Iterable[str]) -> ParsedRedfinCountySales:
    reader = csv.DictReader(csv_lines)
    fieldnames = set(reader.fieldnames or [])
    missing_columns = sorted(REQUIRED_COLUMNS - fieldnames)
    if missing_columns:
        raise ValueError(f"Redfin county CSV is missing columns: {missing_columns}")

    rows_scanned = 0
    records_by_period: dict[date, dict[str, Any]] = {}
    for row in reader:
        rows_scanned += 1
        if str(row.get("REGION NAME") or "").strip() != WAYNE_REGION_NAME:
            continue
        if str(row.get("FREQUENCY") or "").strip() != "Monthly":
            continue
        if str(row.get("REGION TYPE") or "").strip() != "County":
            continue

        try:
            period_date = date.fromisoformat(str(row["PERIOD END"]).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid Redfin PERIOD END: {row.get('PERIOD END')!r}"
            ) from exc

        record = {
            "county_name": "Wayne",
            "state": "OH",
            "period_date": period_date,
            "median_sale_price": _optional_decimal(
                row.get("MEDIAN SALE PRICE NSA ($)"),
                "MEDIAN SALE PRICE NSA ($)",
            ),
            "homes_sold": _optional_integer(row.get("HOMES SOLD"), "HOMES SOLD"),
            "new_listings": _optional_integer(
                row.get("NEW LISTINGS"),
                "NEW LISTINGS",
            ),
            "active_listings": _optional_integer(
                row.get("ACTIVE LISTINGS"),
                "ACTIVE LISTINGS",
            ),
            "median_days_on_market": _optional_integer(
                row.get("MEDIAN DAYS ON MARKET (DAYS)"),
                "MEDIAN DAYS ON MARKET (DAYS)",
            ),
            "source_file": REDFIN_SOURCE_FILE,
        }
        existing = records_by_period.get(period_date)
        if existing is not None and existing != record:
            raise ValueError(f"Conflicting Wayne County rows for {period_date}")
        records_by_period[period_date] = record

    records = [records_by_period[key] for key in sorted(records_by_period)]
    if not records:
        raise ValueError("Redfin county CSV contained no Wayne County monthly rows")
    return ParsedRedfinCountySales(rows_scanned=rows_scanned, records=records)


def upsert_redfin_county_sales(
    records: list[dict[str, Any]],
    *,
    engine: Engine | None = None,
) -> int:
    if not records:
        return 0
    engine = engine or get_engine()
    with engine.begin() as conn:
        result = conn.execute(UPSERT_SQL, records)
    return max(result.rowcount, 0)


def load_redfin_county_sales(
    *,
    engine: Engine | None = None,
    url: str = REDFIN_COUNTY_CSV_URL,
    session: Any | None = None,
) -> RedfinCountySalesImportResult:
    session = session or requests.Session()
    try:
        response_context = session.get(
            url,
            stream=True,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "text/csv"},
        )
        with response_context as response:
            response.raise_for_status()
            response.encoding = "utf-8-sig"
            parsed = parse_redfin_county_sales(
                response.iter_lines(decode_unicode=True)
            )
    except requests.RequestException as exc:
        raise RuntimeError("Redfin county data download failed") from exc

    inserted_or_changed = upsert_redfin_county_sales(
        parsed.records,
        engine=engine,
    )
    periods = [record["period_date"] for record in parsed.records]
    result = RedfinCountySalesImportResult(
        rows_scanned=parsed.rows_scanned,
        matched_rows=len(parsed.records),
        inserted_or_changed=inserted_or_changed,
        earliest_period=min(periods),
        latest_period=max(periods),
    )
    print(
        "Redfin county sales load complete: "
        f"{result.rows_scanned} nationwide row(s) scanned, "
        f"{result.matched_rows} Wayne County month(s) matched, "
        f"{result.inserted_or_changed} row(s) inserted or changed, "
        f"covering {result.earliest_period} through {result.latest_period}."
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream Redfin monthly county data and load Wayne County, Ohio."
    )
    parser.add_argument(
        "--url",
        default=REDFIN_COUNTY_CSV_URL,
        help="Redfin-compatible county CSV URL (primarily for controlled testing).",
    )
    args = parser.parse_args()
    load_redfin_county_sales(url=args.url)


if __name__ == "__main__":
    main()

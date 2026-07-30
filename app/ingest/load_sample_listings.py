from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from app.normalization import normalize_listing
from app.providers.base import ProviderAdapter
from app.utils.db import get_engine


REQUIRED_COLUMNS = [
    "source",
    "source_listing_id",
    "address",
    "city",
    "state",
    "zip",
    "list_price",
    "beds",
    "baths",
    "sqft",
    "property_type",
    "status",
    "days_on_market",
    "first_seen_date",
    "last_seen_date",
    "price_per_sqft",
]

OPTIONAL_COLUMNS = ["lot_size_acres"]

RAW_INSERT_SQL = text(
    """
    INSERT INTO listings_raw (
        source,
        raw_record_json
    )
    VALUES (
        :source,
        CAST(:raw_record_json AS JSONB)
    )
    ON CONFLICT DO NOTHING
    """
)

CURRENT_UPSERT_SQL = text(
    """
    INSERT INTO listings_current (
        source,
        source_listing_id,
        address,
        city,
        state,
        zip,
        list_price,
        beds,
        baths,
        sqft,
        property_type,
        status,
        days_on_market,
        first_seen_date,
        last_seen_date,
        price_per_sqft
    )
    VALUES (
        :source,
        :source_listing_id,
        :address,
        :city,
        :state,
        :zip,
        :list_price,
        :beds,
        :baths,
        :sqft,
        :property_type,
        :status,
        :days_on_market,
        :first_seen_date,
        :last_seen_date,
        :price_per_sqft
    )
    ON CONFLICT (source, source_listing_id)
    DO UPDATE SET
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        zip = EXCLUDED.zip,
        list_price = EXCLUDED.list_price,
        beds = EXCLUDED.beds,
        baths = EXCLUDED.baths,
        sqft = EXCLUDED.sqft,
        property_type = EXCLUDED.property_type,
        status = EXCLUDED.status,
        days_on_market = EXCLUDED.days_on_market,
        price_per_sqft = EXCLUDED.price_per_sqft,
        updated_at = CURRENT_TIMESTAMP
    """
)

EXISTING_CURRENT_SELECT_SQL = text(
    """
    SELECT address, city, state, zip, list_price, beds, baths, sqft,
           property_type, status, days_on_market, first_seen_date,
           last_seen_date, price_per_sqft
    FROM listings_current
    WHERE source = :source AND source_listing_id = :source_listing_id
    """
)


def _json_default(value: Any) -> str | int | float:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def _prepare_raw_record(record: dict[str, Any]) -> dict[str, str]:
    canonical_payload = json.dumps(
        record,
        default=_json_default,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return {
        "source": str(record["source"]).strip(),
        "raw_record_json": canonical_payload,
    }


def _normalize_for_compare(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def _record_matches_existing(existing_row: Any, record: dict[str, Any]) -> bool:
    return (
        _normalize_for_compare(existing_row.address) == _normalize_for_compare(record.get("address"))
        and _normalize_for_compare(existing_row.city) == _normalize_for_compare(record.get("city"))
        and _normalize_for_compare(existing_row.state) == _normalize_for_compare(record.get("state"))
        and _normalize_for_compare(existing_row.zip) == _normalize_for_compare(record.get("zip"))
        and _normalize_for_compare(existing_row.list_price) == _normalize_for_compare(record.get("list_price"))
        and _normalize_for_compare(existing_row.beds) == _normalize_for_compare(record.get("beds"))
        and _normalize_for_compare(existing_row.baths) == _normalize_for_compare(record.get("baths"))
        and _normalize_for_compare(existing_row.sqft) == _normalize_for_compare(record.get("sqft"))
        and _normalize_for_compare(existing_row.property_type) == _normalize_for_compare(record.get("property_type"))
        and _normalize_for_compare(existing_row.status) == _normalize_for_compare(record.get("status"))
        and _normalize_for_compare(existing_row.days_on_market) == _normalize_for_compare(record.get("days_on_market"))
        and _normalize_for_compare(existing_row.first_seen_date) == _normalize_for_compare(record.get("first_seen_date"))
        and _normalize_for_compare(existing_row.last_seen_date) == _normalize_for_compare(record.get("last_seen_date"))
        and _normalize_for_compare(existing_row.price_per_sqft) == _normalize_for_compare(record.get("price_per_sqft"))
    )


def ingest_records(records: list[dict[str, Any]], *, engine: Engine | None = None) -> dict[str, int]:
    if not records:
        return {"received": 0, "raw_inserted": 0, "current_changed": 0}

    raw_records = [_prepare_raw_record(record) for record in records]
    normalized_records = [normalize_listing(record) for record in records]
    engine = engine or get_engine()
    with engine.begin() as conn:
        raw_result = conn.execute(RAW_INSERT_SQL, raw_records)
        current_changed = 0
        for record in normalized_records:
            existing_row = conn.execute(
                EXISTING_CURRENT_SELECT_SQL,
                {
                    "source": record.get("source"),
                    "source_listing_id": record.get("source_listing_id"),
                },
            ).fetchone()
            conn.execute(CURRENT_UPSERT_SQL, record)
            if existing_row is None or not _record_matches_existing(existing_row, record):
                current_changed += 1

    counts = {
        "received": len(records),
        "raw_inserted": max(raw_result.rowcount, 0),
        "current_changed": current_changed,
    }
    print(
        "Listing load complete: "
        f"{counts['received']} received, "
        f"{counts['raw_inserted']} new raw payload(s), "
        f"{counts['current_changed']} current listing(s) inserted or changed."
    )
    return counts


def load_provider_records(provider: ProviderAdapter, *, engine: Engine | None = None) -> dict[str, int]:
    raw_records, _ = provider.fetch_updated_since()
    normalized_records = [provider.normalize(record) for record in raw_records]
    return ingest_records(normalized_records, engine=engine)


def load_sample_csv(file_path: str, engine: Engine | None = None) -> dict[str, int]:
    csv_path = Path(file_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    df = pd.read_csv(
        csv_path,
        dtype={"source": "string", "source_listing_id": "string", "zip": "string"},
    )

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in CSV: {missing_columns}")

    selected_columns = REQUIRED_COLUMNS + [
        column for column in OPTIONAL_COLUMNS if column in df.columns
    ]
    clean_df = (
        df[selected_columns]
        .astype(object)
        .where(pd.notnull(df[selected_columns]), None)
    )
    records = clean_df.to_dict(orient="records")
    return ingest_records(records, engine=engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load listings from a CSV or a provider adapter.")
    parser.add_argument("--provider", choices=["sample", "fixture"], default="sample")
    parser.add_argument("--csv-path", default="data/sample_listings.csv")
    args = parser.parse_args()

    if args.provider == "fixture":
        from app.providers.fixture import FixtureProvider

        provider = FixtureProvider()
        load_provider_records(provider)
    else:
        load_sample_csv(args.csv_path)

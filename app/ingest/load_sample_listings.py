from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from sqlalchemy import Engine, text

from app.db import get_engine
from app.normalization import normalize_listing
from app.providers.base import ProviderAdapter


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
        source_listing_id,
        payload_hash,
        raw_record_json
    )
    VALUES (
        :source,
        :source_listing_id,
        :payload_hash,
        CAST(:raw_record_json AS JSONB)
    )
    ON CONFLICT (source, source_listing_id, payload_hash)
    DO NOTHING
    """
)

SQLITE_RAW_INSERT_SQL = text(
    """
    INSERT INTO listings_raw (
        source,
        source_listing_id,
        payload_hash,
        raw_record_json
    )
    VALUES (
        :source,
        :source_listing_id,
        :payload_hash,
        :raw_record_json
    )
    ON CONFLICT (source, source_listing_id, payload_hash)
    DO NOTHING
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
        lot_size_acres,
        property_type,
        status,
        days_on_market,
        first_seen_date,
        last_seen_date,
        price_per_sqft,
        alert_eligible,
        scoring_eligible
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
        :lot_size_acres,
        :property_type,
        :status,
        :days_on_market,
        :first_seen_date,
        :last_seen_date,
        :price_per_sqft,
        :alert_eligible,
        :scoring_eligible
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
        lot_size_acres = EXCLUDED.lot_size_acres,
        property_type = EXCLUDED.property_type,
        status = EXCLUDED.status,
        days_on_market = EXCLUDED.days_on_market,
        first_seen_date = LEAST(
            listings_current.first_seen_date,
            EXCLUDED.first_seen_date
        ),
        last_seen_date = GREATEST(
            listings_current.last_seen_date,
            EXCLUDED.last_seen_date
        ),
        price_per_sqft = EXCLUDED.price_per_sqft,
        alert_eligible = EXCLUDED.alert_eligible,
        scoring_eligible = EXCLUDED.scoring_eligible,
        updated_at = CURRENT_TIMESTAMP
    WHERE ROW(
        listings_current.address,
        listings_current.city,
        listings_current.state,
        listings_current.zip,
        listings_current.list_price,
        listings_current.beds,
        listings_current.baths,
        listings_current.sqft,
        listings_current.lot_size_acres,
        listings_current.property_type,
        listings_current.status,
        listings_current.days_on_market,
        listings_current.first_seen_date,
        listings_current.last_seen_date,
        listings_current.price_per_sqft,
        listings_current.alert_eligible,
        listings_current.scoring_eligible
    ) IS DISTINCT FROM ROW(
        EXCLUDED.address,
        EXCLUDED.city,
        EXCLUDED.state,
        EXCLUDED.zip,
        EXCLUDED.list_price,
        EXCLUDED.beds,
        EXCLUDED.baths,
        EXCLUDED.sqft,
        EXCLUDED.lot_size_acres,
        EXCLUDED.property_type,
        EXCLUDED.status,
        EXCLUDED.days_on_market,
        LEAST(listings_current.first_seen_date, EXCLUDED.first_seen_date),
        GREATEST(listings_current.last_seen_date, EXCLUDED.last_seen_date),
        EXCLUDED.price_per_sqft,
        EXCLUDED.alert_eligible,
        EXCLUDED.scoring_eligible
    )
    """
)

SQLITE_CURRENT_UPSERT_SQL = text(
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
        lot_size_acres,
        property_type,
        status,
        days_on_market,
        first_seen_date,
        last_seen_date,
        price_per_sqft,
        alert_eligible,
        scoring_eligible
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
        :lot_size_acres,
        :property_type,
        :status,
        :days_on_market,
        :first_seen_date,
        :last_seen_date,
        :price_per_sqft,
        :alert_eligible,
        :scoring_eligible
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
        lot_size_acres = EXCLUDED.lot_size_acres,
        property_type = EXCLUDED.property_type,
        status = EXCLUDED.status,
        days_on_market = EXCLUDED.days_on_market,
        first_seen_date = EXCLUDED.first_seen_date,
        last_seen_date = EXCLUDED.last_seen_date,
        price_per_sqft = EXCLUDED.price_per_sqft,
        alert_eligible = EXCLUDED.alert_eligible,
        scoring_eligible = EXCLUDED.scoring_eligible,
        updated_at = CURRENT_TIMESTAMP
    """
)

EXISTING_CURRENT_SELECT_SQL = text(
    """
    SELECT
        address,
        city,
        state,
        zip,
        list_price,
        beds,
        baths,
        sqft,
        lot_size_acres,
        property_type,
        status,
        days_on_market,
        first_seen_date,
        last_seen_date,
        price_per_sqft,
        alert_eligible,
        scoring_eligible
    FROM listings_current
    WHERE source = :source
      AND source_listing_id = :source_listing_id
    """
)

TRACKED_CURRENT_FIELDS = (
    "address",
    "city",
    "state",
    "zip",
    "list_price",
    "beds",
    "baths",
    "sqft",
    "lot_size_acres",
    "property_type",
    "status",
    "days_on_market",
    "first_seen_date",
    "last_seen_date",
    "price_per_sqft",
    "alert_eligible",
    "scoring_eligible",
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
        "source_listing_id": str(record["source_listing_id"]).strip(),
        "payload_hash": hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest(),
        "raw_record_json": canonical_payload,
    }


def _sqlite_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _sqlite_record_changed(
    existing: Any,
    record: dict[str, Any],
) -> bool:
    if existing is None:
        return True
    return any(
        _sqlite_value(existing._mapping[field])
        != _sqlite_value(record.get(field))
        for field in TRACKED_CURRENT_FIELDS
    )


def ingest_records(
    records: list[dict[str, Any]],
    *,
    engine: Engine | None = None,
    alert_eligible: bool = True,
    scoring_eligibility: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, int]:
    if not records:
        return {
            "received": 0,
            "raw_inserted": 0,
            "scoring_eligible": 0,
            "scoring_excluded": 0,
            "current_changed": 0,
        }

    raw_records = [_prepare_raw_record(record) for record in records]
    normalized_records = [normalize_listing(record) for record in records]
    for record in normalized_records:
        record["alert_eligible"] = alert_eligible
        record["scoring_eligible"] = (
            scoring_eligibility(record) if scoring_eligibility else True
        )
    engine = engine or get_engine()
    with engine.begin() as conn:
        if engine.dialect.name == "sqlite":
            raw_result = conn.execute(SQLITE_RAW_INSERT_SQL, raw_records)
            current_changed = 0
            for record in normalized_records:
                existing = conn.execute(
                    EXISTING_CURRENT_SELECT_SQL,
                    {
                        "source": record["source"],
                        "source_listing_id": record["source_listing_id"],
                    },
                ).one_or_none()
                if _sqlite_record_changed(existing, record):
                    conn.execute(SQLITE_CURRENT_UPSERT_SQL, record)
                    current_changed += 1
        else:
            raw_result = conn.execute(RAW_INSERT_SQL, raw_records)
            current_result = conn.execute(CURRENT_UPSERT_SQL, normalized_records)
            current_changed = max(current_result.rowcount, 0)

    counts = {
        "received": len(records),
        "raw_inserted": max(raw_result.rowcount, 0),
        "scoring_eligible": sum(
            record["scoring_eligible"] for record in normalized_records
        ),
        "scoring_excluded": sum(
            not record["scoring_eligible"] for record in normalized_records
        ),
        "current_changed": current_changed,
    }
    print(
        "Listing load complete: "
        f"{counts['received']} received, "
        f"{counts['raw_inserted']} new raw payload(s), "
        f"{counts['scoring_excluded']} excluded from scoring, "
        f"{counts['current_changed']} current listing(s) inserted or changed."
    )
    return counts


def load_provider_records(
    provider: ProviderAdapter,
    *,
    engine: Engine | None = None,
) -> dict[str, int]:
    raw_records, _ = provider.fetch_updated_since()
    normalized_records = [provider.normalize(record) for record in raw_records]
    return ingest_records(normalized_records, engine=engine)


def load_sample_csv(
    file_path: str,
    engine: Engine | None = None,
) -> dict[str, int]:
    csv_path = Path(file_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    df = pd.read_csv(
        csv_path,
        dtype={"source": "string", "source_listing_id": "string", "zip": "string"},
    )
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df]
    if missing_columns:
        raise ValueError(f"Missing columns in CSV: {missing_columns}")

    selected_columns = REQUIRED_COLUMNS + [
        column for column in OPTIONAL_COLUMNS if column in df
    ]
    clean_df = (
        df[selected_columns]
        .astype(object)
        .where(pd.notnull(df[selected_columns]), None)
    )
    records = clean_df.to_dict(orient="records")
    return ingest_records(records, engine=engine)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load listings from a CSV or provider adapter."
    )
    parser.add_argument("--provider", choices=["sample", "fixture"], default="sample")
    parser.add_argument("--csv-path", default="data/sample_listings.csv")
    args = parser.parse_args()

    if args.provider == "fixture":
        from app.providers.fixture import FixtureProvider

        load_provider_records(FixtureProvider())
    else:
        load_sample_csv(args.csv_path)


if __name__ == "__main__":
    main()

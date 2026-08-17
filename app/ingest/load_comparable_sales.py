from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from app.db import get_engine
from app.normalization import (
    normalize_boolean,
    normalize_date,
    normalize_integer,
    normalize_property_type,
    normalize_state,
    normalize_zip,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "sample_comparable_sales.csv"

REQUIRED_COLUMNS = [
    "source",
    "source_sale_id",
    "sale_date",
    "sale_price",
    "address",
    "city",
    "state",
    "zip",
    "property_type",
    "arms_length",
]

OPTIONAL_COLUMNS = [
    "parcel_number",
    "county_name",
    "beds",
    "baths",
    "sqft",
    "lot_size_acres",
    "year_built",
    "latitude",
    "longitude",
]

RAW_INSERT_SQL = text(
    """
    INSERT INTO comparable_sales_raw (
        source,
        source_sale_id,
        payload_hash,
        raw_record_json
    )
    VALUES (
        :source,
        :source_sale_id,
        :payload_hash,
        CAST(:raw_record_json AS JSONB)
    )
    ON CONFLICT (source, source_sale_id, payload_hash)
    DO NOTHING
    """
)

CURRENT_UPSERT_SQL = text(
    """
    INSERT INTO comparable_sales (
        source,
        source_sale_id,
        parcel_number,
        sale_date,
        sale_price,
        address,
        city,
        state,
        zip,
        county_name,
        property_type,
        beds,
        baths,
        sqft,
        lot_size_acres,
        year_built,
        latitude,
        longitude,
        arms_length
    )
    VALUES (
        :source,
        :source_sale_id,
        :parcel_number,
        :sale_date,
        :sale_price,
        :address,
        :city,
        :state,
        :zip,
        :county_name,
        :property_type,
        :beds,
        :baths,
        :sqft,
        :lot_size_acres,
        :year_built,
        :latitude,
        :longitude,
        :arms_length
    )
    ON CONFLICT (source, source_sale_id)
    DO UPDATE SET
        parcel_number = EXCLUDED.parcel_number,
        sale_date = EXCLUDED.sale_date,
        sale_price = EXCLUDED.sale_price,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        zip = EXCLUDED.zip,
        county_name = EXCLUDED.county_name,
        property_type = EXCLUDED.property_type,
        beds = EXCLUDED.beds,
        baths = EXCLUDED.baths,
        sqft = EXCLUDED.sqft,
        lot_size_acres = EXCLUDED.lot_size_acres,
        year_built = EXCLUDED.year_built,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        arms_length = EXCLUDED.arms_length,
        updated_at = CURRENT_TIMESTAMP
    WHERE ROW(
        comparable_sales.parcel_number,
        comparable_sales.sale_date,
        comparable_sales.sale_price,
        comparable_sales.address,
        comparable_sales.city,
        comparable_sales.state,
        comparable_sales.zip,
        comparable_sales.county_name,
        comparable_sales.property_type,
        comparable_sales.beds,
        comparable_sales.baths,
        comparable_sales.sqft,
        comparable_sales.lot_size_acres,
        comparable_sales.year_built,
        comparable_sales.latitude,
        comparable_sales.longitude,
        comparable_sales.arms_length
    ) IS DISTINCT FROM ROW(
        EXCLUDED.parcel_number,
        EXCLUDED.sale_date,
        EXCLUDED.sale_price,
        EXCLUDED.address,
        EXCLUDED.city,
        EXCLUDED.state,
        EXCLUDED.zip,
        EXCLUDED.county_name,
        EXCLUDED.property_type,
        EXCLUDED.beds,
        EXCLUDED.baths,
        EXCLUDED.sqft,
        EXCLUDED.lot_size_acres,
        EXCLUDED.year_built,
        EXCLUDED.latitude,
        EXCLUDED.longitude,
        EXCLUDED.arms_length
    )
    """
)


def _normalize_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    normalized = " ".join(str(value).strip().split())
    return normalized or None


def _normalize_decimal(
    value: Any,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        normalized = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field} must be numeric, got: {value!r}") from exc
    if not normalized.is_finite():
        raise ValueError(f"{field} must be finite, got: {value!r}")
    if minimum is not None and normalized < minimum:
        raise ValueError(f"{field} must be at least {minimum}, got: {value!r}")
    if maximum is not None and normalized > maximum:
        raise ValueError(f"{field} must be at most {maximum}, got: {value!r}")
    return normalized


def normalize_comparable_sale(record: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "source": _normalize_text(record.get("source")),
        "source_sale_id": _normalize_text(record.get("source_sale_id")),
        "parcel_number": _normalize_text(record.get("parcel_number")),
        "sale_date": normalize_date(record.get("sale_date")),
        "sale_price": _normalize_decimal(
            record.get("sale_price"),
            "sale_price",
            minimum=Decimal("0.01"),
        ),
        "address": _normalize_text(record.get("address")),
        "city": _normalize_text(record.get("city")),
        "state": normalize_state(record.get("state")),
        "zip": normalize_zip(record.get("zip")),
        "county_name": _normalize_text(record.get("county_name")),
        "property_type": normalize_property_type(record.get("property_type")),
        "beds": _normalize_decimal(record.get("beds"), "beds", minimum=Decimal("0")),
        "baths": _normalize_decimal(
            record.get("baths"),
            "baths",
            minimum=Decimal("0"),
        ),
        "sqft": _normalize_decimal(
            record.get("sqft"),
            "sqft",
            minimum=Decimal("0.1"),
        ),
        "lot_size_acres": _normalize_decimal(
            record.get("lot_size_acres"),
            "lot_size_acres",
            minimum=Decimal("0"),
        ),
        "year_built": normalize_integer(record.get("year_built")),
        "latitude": _normalize_decimal(
            record.get("latitude"),
            "latitude",
            minimum=Decimal("-90"),
            maximum=Decimal("90"),
        ),
        "longitude": _normalize_decimal(
            record.get("longitude"),
            "longitude",
            minimum=Decimal("-180"),
            maximum=Decimal("180"),
        ),
        "arms_length": normalize_boolean(record.get("arms_length")),
    }

    for field in REQUIRED_COLUMNS:
        if normalized[field] is None:
            raise ValueError(f"{field} cannot be empty")
    if len(normalized["state"]) != 2:
        raise ValueError(
            f"state must be a two-letter code, got: {normalized['state']!r}"
        )
    year_built = normalized["year_built"]
    if year_built is not None and not 1700 <= year_built <= 2200:
        raise ValueError(f"year_built must be between 1700 and 2200, got: {year_built}")
    return normalized


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
        "source_sale_id": str(record["source_sale_id"]).strip(),
        "payload_hash": hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest(),
        "raw_record_json": canonical_payload,
    }


def load_comparable_sales(
    file_path: str | Path = DATA_PATH,
    engine: Engine | None = None,
) -> dict[str, int]:
    csv_path = Path(file_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {csv_path}")

    df = pd.read_csv(
        csv_path,
        dtype={"source": "string", "source_sale_id": "string", "zip": "string"},
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
    counts = ingest_comparable_records(records, engine=engine)
    print(
        "Comparable sales load complete: "
        f"{counts['received']} received, "
        f"{counts['raw_inserted']} new raw payload(s), "
        f"{counts['current_changed']} current sale(s) inserted or changed."
    )
    return counts


def ingest_comparable_records(
    records: list[dict[str, Any]],
    *,
    engine: Engine | None = None,
) -> dict[str, int]:
    """Normalize and idempotently store in-memory comparable-sale records."""

    raw_records = [_prepare_raw_record(record) for record in records]
    normalized_records = [normalize_comparable_sale(record) for record in records]
    engine = engine or get_engine()
    with engine.begin() as conn:
        raw_result = conn.execute(RAW_INSERT_SQL, raw_records)
        current_result = conn.execute(CURRENT_UPSERT_SQL, normalized_records)

    counts = {
        "received": len(records),
        "raw_inserted": max(raw_result.rowcount, 0),
        "current_changed": max(current_result.rowcount, 0),
    }
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Load property-level comparable sales.")
    parser.add_argument(
        "file",
        nargs="?",
        type=Path,
        default=DATA_PATH,
        help=f"CSV file to load (default: {DATA_PATH})",
    )
    args = parser.parse_args()
    load_comparable_sales(args.file)


if __name__ == "__main__":
    main()

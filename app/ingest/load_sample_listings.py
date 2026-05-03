from pathlib import Path

import pandas as pd
from sqlalchemy import text

from app.utils.db import get_engine


def insert_raw_records(records):
    insert_sql = text(
    """
    INSERT INTO listings_raw (
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
    """)

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(insert_sql, records)


def load_sample_csv(file_path: str):
    csv_path = Path(file_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    df = pd.read_csv(csv_path)

    expected_columns = [
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

    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in CSV: {missing_columns}")

    df["first_seen_date"] = pd.to_datetime(df["first_seen_date"]).dt.date
    df["last_seen_date"] = pd.to_datetime(df["last_seen_date"]).dt.date

    records = df.to_dict(orient="records")

    insert_raw_records(records)

    upsert_sql = text(
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
        first_seen_date = LEAST(listings_current.first_seen_date, EXCLUDED.first_seen_date),
        last_seen_date = GREATEST(listings_current.last_seen_date, EXCLUDED.last_seen_date),
        price_per_sqft = EXCLUDED.price_per_sqft,
        updated_at = CURRENT_TIMESTAMP
    """
)

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(upsert_sql, records)

    print(f"Inserted {len(records)} rows into listings_raw.")
    print(f"Upserted {len(records)} rows into listings_current.")


if __name__ == "__main__":
    load_sample_csv("data/sample_listings.csv")
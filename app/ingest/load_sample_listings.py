from pathlib import Path

import pandas as pd
from sqlalchemy import text

from app.utils.db import get_engine


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

    insert_sql = text(
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
        """
    )

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"Inserted {len(records)} rows into listings_current.")


if __name__ == "__main__":
    load_sample_csv("data/sample_listings.csv")
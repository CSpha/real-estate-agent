import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "county_sales.csv"


def get_required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realestate"),
        user=os.getenv("DB_USER", "realestate"),
        password=get_required_env("DB_PASSWORD"),
    )

def load_county_sales():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Could not find file: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)

    expected_columns = [
        "county_name",
        "period_date",
        "median_sale_price",
        "homes_sold",
        "new_listings",
        "active_listings",
        "median_days_on_market",
    ]

    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing expected columns: {missing_columns}")

    df["period_date"] = pd.to_datetime(df["period_date"]).dt.date
    df = df.where(pd.notnull(df), None)

    rows = [
        (
            row["county_name"],
            row["period_date"],
            row["median_sale_price"],
            row["homes_sold"],
            row["new_listings"],
            row["active_listings"],
            row["median_days_on_market"],
            DATA_PATH.name,
        )
        for _, row in df.iterrows()
    ]

    insert_sql = """
        INSERT INTO county_sales (
            county_name,
            period_date,
            median_sale_price,
            homes_sold,
            new_listings,
            active_listings,
            median_days_on_market,
            source_file
        )
        VALUES %s
        ON CONFLICT (county_name, period_date)
        DO UPDATE SET
            median_sale_price = EXCLUDED.median_sale_price,
            homes_sold = EXCLUDED.homes_sold,
            new_listings = EXCLUDED.new_listings,
            active_listings = EXCLUDED.active_listings,
            median_days_on_market = EXCLUDED.median_days_on_market,
            source_file = EXCLUDED.source_file,
            loaded_at = CURRENT_TIMESTAMP;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)

    print(f"Loaded {len(rows)} county sales rows into county_sales.")


if __name__ == "__main__":
    load_county_sales()
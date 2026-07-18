from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from app.db import get_engine


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "county_sales.csv"


def load_county_sales(
    file_path: str | Path = DATA_PATH,
    *,
    default_state: str = "OH",
    engine: Engine | None = None,
) -> int:
    data_path = Path(file_path)
    if not data_path.exists():
        raise FileNotFoundError(f"Could not find file: {data_path}")

    df = pd.read_csv(data_path)
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

    if "state" not in df.columns:
        df["state"] = default_state

    df["state"] = df["state"].astype(str).str.strip().str.upper()
    df["period_date"] = pd.to_datetime(df["period_date"]).dt.date
    df = df.astype(object).where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    for record in records:
        if len(record["state"]) != 2:
            raise ValueError(
                f"State must be a two-letter code, got: {record['state']!r}"
            )
        for field in (
            "homes_sold",
            "new_listings",
            "active_listings",
            "median_days_on_market",
        ):
            if record[field] is not None:
                record[field] = int(record[field])
        record["source_file"] = data_path.name

    insert_sql = text(
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

    engine = engine or get_engine()
    with engine.begin() as conn:
        result = conn.execute(insert_sql, records)

    changed = max(result.rowcount, 0)
    print(
        f"County sales load complete: {len(records)} received, "
        f"{changed} row(s) inserted or changed."
    )
    return changed


if __name__ == "__main__":
    load_county_sales()

import json
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from app.utils.db import get_engine


def load_wayne_county_sales(file_path: str):
    csv_path = Path(file_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    df = pd.read_csv(csv_path, encoding="utf-16", skiprows=1)
    df = df.fillna("")

    records = [
        {
            "source": "wayne_county_sales_report",
            "raw_record_json": json.dumps(row.to_dict(), default=str),
        }
        for _, row in df.iterrows()
    ]

    insert_sql = text(
        """
        INSERT INTO listings_raw (
            source,
            raw_record_json
        )
        VALUES (
            :source,
            :raw_record_json
        )
        """
    )

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"Inserted {len(records)} raw Wayne County sales records.")


if __name__ == "__main__":
    load_wayne_county_sales("data/parcel.csv")
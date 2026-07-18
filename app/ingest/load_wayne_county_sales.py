from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from app.db import get_engine


def load_wayne_county_sales(
    file_path: str,
    engine: Engine | None = None,
) -> int:
    csv_path = Path(file_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    df = pd.read_csv(csv_path, encoding="utf-16", skiprows=1)
    df = df.fillna("")

    records = []
    for row_number, (_, row) in enumerate(df.iterrows(), start=1):
        payload = json.dumps(
            row.to_dict(),
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        )
        records.append(
            {
                "source": "wayne_county_sales_report",
                "source_listing_id": f"{csv_path.name}:{row_number}",
                "payload_hash": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                "raw_record_json": payload,
            }
        )

    insert_sql = text(
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

    engine = engine or get_engine()
    with engine.begin() as conn:
        result = conn.execute(insert_sql, records)

    inserted = max(result.rowcount, 0)
    print(
        f"Wayne County raw load complete: {len(records)} received, "
        f"{inserted} new payload(s)."
    )
    return inserted


if __name__ == "__main__":
    load_wayne_county_sales("data/parcel.csv")

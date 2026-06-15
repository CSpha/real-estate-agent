import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from app.db import get_connection

from app.utils.db import get_engine


app = FastAPI(title="Real Estate Agent API")

@app.get("/")
def root():
    return {"message": "Real Estate AgentAPI is running"}


@app.get("/health")
def health_check():
    return {"status": "ok"}


class Listing(BaseModel):
    source: str
    source_listing_id: str
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]
    list_price: Optional[float]
    beds: Optional[float]
    baths: Optional[float]
    sqft: Optional[float]
    property_type: Optional[str]
    status: Optional[str]
    days_on_market: Optional[int]
    first_seen_date: Optional[date]
    last_seen_date: Optional[date]
    price_per_sqft: Optional[float]


class ScoreResult(BaseModel):
    source: str
    source_listing_id: str
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    current_price: Optional[float]
    previous_price: Optional[float]
    price_change: Optional[float]
    price_change_pct: Optional[float]
    days_on_market: Optional[int]
    price_per_sqft: Optional[float]
    market_score: int
    score_reason: str


class AlertRecord(BaseModel):
    id: int
    alert_type: str
    source: str
    source_listing_id: str
    event_timestamp: datetime
    payload: Dict[str, Any]
    sent_at: datetime


def serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def row_to_dict(row) -> Dict[str, Any]:
    return {key: serialize_value(value) for key, value in row._mapping.items()}


@app.get("/listings")
def get_listings():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM listings_current
                ORDER BY listing_id
                LIMIT 100;
            """)
            rows = cur.fetchall()
            return rows
    finally:
        conn.close()

    engine = get_engine()
    with engine.connect() as conn:
        results = conn.execute(query, {"limit": limit, "offset": offset}).all()

    return [row_to_dict(row) for row in results]


@app.post("/score", response_model=List[ScoreResult])
def run_scoring(limit: int = Query(100, ge=1, le=1000)):
    query = text(
        """
        WITH ranked_history AS (
            SELECT
                source,
                source_listing_id,
                address,
                city,
                state,
                list_price,
                status,
                days_on_market,
                sqft,
                price_per_sqft,
                snapshot_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY source, source_listing_id
                    ORDER BY snapshot_timestamp DESC
                ) AS rn
            FROM listing_history
        ),
        latest AS (
            SELECT
                source,
                source_listing_id,
                address,
                city,
                state,
                list_price AS current_price,
                status AS current_status,
                days_on_market,
                sqft,
                price_per_sqft,
                snapshot_timestamp AS current_snapshot
            FROM ranked_history
            WHERE rn = 1
        ),
        previous AS (
            SELECT
                source,
                source_listing_id,
                list_price AS previous_price,
                snapshot_timestamp AS previous_snapshot
            FROM ranked_history
            WHERE rn = 2
        )
        SELECT
            l.source,
            l.source_listing_id,
            l.address,
            l.city,
            l.state,
            l.current_price,
            p.previous_price,
            (l.current_price - p.previous_price) AS price_change,
            ROUND(
                ((l.current_price - p.previous_price) / NULLIF(p.previous_price, 0)) * 100,
                2
            ) AS price_change_pct,
            l.days_on_market,
            l.price_per_sqft,
            CASE
                WHEN l.current_price < p.previous_price THEN 40
                ELSE 10
            END
            + CASE
                WHEN l.status = 'active' THEN 30
                ELSE 0
            END
            + CASE
                WHEN l.days_on_market IS NOT NULL AND l.days_on_market > 30 THEN 20
                ELSE 0
            END
            + CASE
                WHEN l.price_per_sqft IS NOT NULL AND l.price_per_sqft < 300 THEN 20
                ELSE 0
            END
            AS market_score,
            CASE
                WHEN l.current_price < p.previous_price THEN 'Price dropped since last snapshot.'
                WHEN l.status = 'active' THEN 'Listing is active and in market.'
                ELSE 'Standard listing scoring.'
            END AS score_reason
        FROM latest l
        JOIN previous p
            ON l.source = p.source
           AND l.source_listing_id = p.source_listing_id
        ORDER BY market_score DESC, price_change_pct ASC
        LIMIT :limit
        """
    )

    engine = get_engine()
    with engine.connect() as conn:
        results = conn.execute(query, {"limit": limit}).all()

    return [row_to_dict(row) for row in results]

@app.get("/listings/{listing_id}")
def get_listing(listing_id: int):
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM listings_current
                WHERE listing_id = %s;
            """, (listing_id,))

            row = cur.fetchone()

            if row is None:
                raise HTTPException(status_code=404, detail="Listing not found")

            return row
    finally:
        conn.close()

@app.get("/alerts", response_model=List[AlertRecord])
def list_alerts(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    query = text(
        """
        SELECT
            id,
            alert_type,
            source,
            source_listing_id,
            event_timestamp,
            payload_json,
            sent_at
        FROM alerts_sent
        ORDER BY sent_at DESC
        LIMIT :limit OFFSET :offset
        """
    )

    engine = get_engine()
    with engine.connect() as conn:
        results = conn.execute(query, {"limit": limit, "offset": offset}).all()

    alerts = []
    for row in results:
        payload_raw = row.payload_json
        payload = {}
        if payload_raw:
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload = {"raw": payload_raw}

        row_data = row_to_dict(row)
        row_data["payload"] = payload
        row_data.pop("payload_json", None)
        alerts.append(row_data)

    return alerts


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)

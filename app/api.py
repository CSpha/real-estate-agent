from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import get_engine
from app.transforms.score_listings_against_market import (
    score_listings_against_market,
)


app = FastAPI(title="Real Estate Agent API")


class MarketScoreResult(BaseModel):
    source: str
    source_listing_id: str
    address: str | None
    city: str | None
    state: str | None
    county_name: str | None
    list_price: float | None
    county_median_sale_price: float | None
    price_vs_county_median: float | None
    pct_below_or_above_median: float | None
    market_score: int
    score_reason: str
    scored_at: datetime


class AlertRecord(BaseModel):
    id: int
    alert_type: str
    source: str
    source_listing_id: str
    event_timestamp: datetime
    payload: dict[str, Any]
    sent_at: datetime


def serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "_mapping"):
        items = row._mapping.items()
    elif hasattr(row, "items"):
        items = row.items()
    else:
        items = dict(row).items()
    return {key: serialize_value(value) for key, value in items}


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "Real Estate Agent API is running"}


@app.get("/health")
def health_check() -> dict[str, str]:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Database is unavailable",
        ) from exc
    return {"status": "ok", "database": "connected"}


@app.get("/listings")
def get_listings(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT *
        FROM listings_current
        ORDER BY id
        LIMIT :limit OFFSET :offset
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(query, {"limit": limit, "offset": offset}).all()
    return [row_to_dict(row) for row in rows]


@app.get("/listings/{listing_id}")
def get_listing(listing_id: int) -> dict[str, Any]:
    query = text(
        """
        SELECT *
        FROM listings_current
        WHERE id = :listing_id
        """
    )
    with get_engine().connect() as conn:
        row = conn.execute(query, {"listing_id": listing_id}).first()

    if row is None:
        raise HTTPException(status_code=404, detail="Listing not found")
    return row_to_dict(row)


@app.post("/score", response_model=list[MarketScoreResult])
def run_scoring(
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict[str, Any]]:
    engine = get_engine()
    score_listings_against_market(engine)
    query = text(
        """
        SELECT
            source,
            source_listing_id,
            address,
            city,
            state,
            county_name,
            list_price,
            county_median_sale_price,
            price_vs_county_median,
            pct_below_or_above_median,
            market_score,
            score_reason,
            scored_at
        FROM listing_market_scores
        ORDER BY market_score DESC, pct_below_or_above_median ASC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit}).all()
    return [row_to_dict(row) for row in rows]


@app.get("/alerts", response_model=list[AlertRecord])
def list_alerts(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
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
    with get_engine().connect() as conn:
        rows = conn.execute(query, {"limit": limit, "offset": offset}).all()

    alerts = []
    for row in rows:
        row_data = row_to_dict(row)
        payload_raw = row_data.pop("payload_json", None)
        if isinstance(payload_raw, str):
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload = {"raw": payload_raw}
        else:
            payload = payload_raw or {}
        row_data["payload"] = payload
        alerts.append(row_data)

    return alerts


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)

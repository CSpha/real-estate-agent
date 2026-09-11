import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import get_connection
from app.analyst import create_analysis
from app.searches.router import router as saved_searches_router
from app.utils.db import get_engine


app = FastAPI(title="Real Estate Agent API")
app.include_router(saved_searches_router)


SHADOW_HEALTH_SQL = text(
    """
    SELECT
        (SELECT finished_at
         FROM shadow_pipeline_runs
         WHERE source = 'rentcast' AND status = 'succeeded'
         ORDER BY finished_at DESC NULLS LAST, id DESC
         LIMIT 1) AS last_successful_run_at,
        (SELECT status
         FROM shadow_pipeline_runs
         WHERE source = 'rentcast'
         ORDER BY started_at DESC, id DESC
         LIMIT 1) AS latest_status,
        (SELECT COALESCE(finished_at, started_at)
         FROM shadow_pipeline_runs
         WHERE source = 'rentcast'
         ORDER BY started_at DESC, id DESC
         LIMIT 1) AS latest_run_at,
        (SELECT COUNT(*)
         FROM shadow_pipeline_runs
         WHERE source = 'rentcast' AND status = 'failed'
           AND COALESCE(finished_at, started_at) >= CURRENT_TIMESTAMP - INTERVAL '7 days'
        ) AS recent_failure_count
    """
)

SHADOW_FAILURES_SQL = text(
    """
    SELECT id, started_at, finished_at
    FROM shadow_pipeline_runs
    WHERE source = 'rentcast' AND status = 'failed'
      AND COALESCE(finished_at, started_at) >= CURRENT_TIMESTAMP - INTERVAL '7 days'
    ORDER BY COALESCE(finished_at, started_at) DESC, id DESC
    LIMIT 5
    """
)


def _shadow_schedule_hours(name: str, default: int, minimum: int = 0) -> int:
    """Read scheduler timing without allowing malformed health configuration."""
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value >= minimum else default


def _shadow_health(conn, now: datetime) -> dict[str, Any]:
    summary = conn.execute(SHADOW_HEALTH_SQL).mappings().one()
    last_success = summary["last_successful_run_at"]
    if last_success is not None:
        if last_success.tzinfo is None:
            last_success = last_success.replace(tzinfo=timezone.utc)
        age_seconds = max(0, int((now - last_success).total_seconds()))
        freshness = (
            "fresh"
            if age_seconds
            <= (
                _shadow_schedule_hours("SHADOW_INTERVAL_HOURS", 72, 1)
                + _shadow_schedule_hours("SHADOW_RETRY_HOURS", 6, 1)
            )
            * 3600
            else "stale"
        )
    else:
        age_seconds = None
        freshness = "unknown"

    latest_at = summary["latest_run_at"]
    if latest_at is not None:
        if latest_at.tzinfo is None:
            latest_at = latest_at.replace(tzinfo=timezone.utc)
        delay_hours = (
            _shadow_schedule_hours("SHADOW_INTERVAL_HOURS", 72, 1)
            if summary["latest_status"] == "succeeded"
            else _shadow_schedule_hours("SHADOW_RETRY_HOURS", 6, 1)
        )
        next_run = latest_at + timedelta(hours=delay_hours)
    else:
        next_run = None

    failures = [
        {
            "id": row["id"],
            "started_at": serialize_value(row["started_at"]),
            "finished_at": serialize_value(row["finished_at"]),
        }
        for row in conn.execute(SHADOW_FAILURES_SQL).mappings().all()
    ]
    return {
        "last_successful_run_at": serialize_value(last_success),
        "last_successful_run_age_seconds": age_seconds,
        "next_run_at": serialize_value(next_run),
        "latest_status": summary["latest_status"],
        "recent_failure_count": int(summary["recent_failure_count"] or 0),
        "recent_failures": failures,
        "freshness": freshness,
    }


def analyst_feature_enabled() -> bool:
    for env_name in ("ANALYST_FEATURES_ENABLED", "ENABLE_ANALYST_FEATURES"):
        value = os.getenv(env_name)
        if value is not None:
            return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


@app.get("/")
def root():
    return {"message": "Real Estate Agent API is running"}


@app.get("/health")
def health_check():
    now = datetime.now(timezone.utc)
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
            shadow = _shadow_health(conn, now)
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Database is unavailable",
        ) from exc
    status = "ok" if shadow["freshness"] == "fresh" else "degraded"
    return {"status": status, "database": "connected", "shadow_pipeline": shadow}


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


class AnalysisRequest(BaseModel):
    model: Optional[str] = None


class AnalysisReviewRequest(BaseModel):
    decision: Literal["approved", "rejected", "corrected"]
    reviewer: Optional[str] = None
    notes: Optional[str] = None
    corrections: Optional[Dict[str, Any]] = None


def serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def row_to_dict(row) -> Dict[str, Any]:
    if hasattr(row, "_mapping"):
        items = row._mapping.items()
    elif hasattr(row, "items"):
        items = row.items()
    else:
        items = dict(row).items()
    return {key: serialize_value(value) for key, value in items}


@app.get("/listings")
def get_listings():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM listings_current
                ORDER BY id
                LIMIT 100;
            """)
            rows = cur.fetchall()
            return [row_to_dict(row) for row in rows]
    finally:
        conn.close()


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
                scoring_eligible,
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
                scoring_eligible,
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
                WHEN l.current_status = 'active' THEN 30
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
                WHEN l.current_status = 'active' THEN 'Listing is active and in market.'
                ELSE 'Standard listing scoring.'
            END AS score_reason
        FROM latest l
        JOIN previous p
            ON l.source = p.source
           AND l.source_listing_id = p.source_listing_id
        WHERE l.scoring_eligible
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
            cur.execute(
                """
                SELECT *
                FROM listings_current
                WHERE id = %s;
            """,
                (listing_id,),
            )

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


@app.post("/listings/{source}/{source_listing_id}/analyses")
def analyze_listing(source: str, source_listing_id: str, request: AnalysisRequest):
    if not analyst_feature_enabled():
        raise HTTPException(status_code=404, detail="Analyst feature is disabled")

    try:
        return create_analysis(source, source_listing_id, request.model)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/listings/{source}/{source_listing_id}/analyses")
def list_analyses(
    source: str, source_listing_id: str, limit: int = Query(20, ge=1, le=100)
):
    if not analyst_feature_enabled():
        raise HTTPException(status_code=404, detail="Analyst feature is disabled")

    with get_engine().connect() as conn:
        rows = conn.execute(
            text("""
            SELECT id, source, source_listing_id, model, prompt_version,
                   evidence_json AS evidence, analysis_json AS analysis, created_at
            FROM investment_analyses
            WHERE source = :source AND source_listing_id = :listing_id
            ORDER BY created_at DESC LIMIT :limit
        """),
            {"source": source, "listing_id": source_listing_id, "limit": limit},
        ).all()
    return [row_to_dict(row) for row in rows]


@app.post("/analyses/{analysis_id}/reviews", status_code=201)
def review_analysis(analysis_id: int, request: AnalysisReviewRequest):
    if not analyst_feature_enabled():
        raise HTTPException(status_code=404, detail="Analyst feature is disabled")

    if request.decision == "corrected" and not request.corrections:
        raise HTTPException(
            status_code=422, detail="Corrected reviews require corrections"
        )
    with get_engine().begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM investment_analyses WHERE id = :id"),
            {"id": analysis_id},
        ).first()
        if not exists:
            raise HTTPException(status_code=404, detail="Analysis not found")
        row = conn.execute(
            text("""
            INSERT INTO investment_analysis_reviews
                (analysis_id, decision, reviewer, notes, corrections_json)
            VALUES (:analysis_id, :decision, :reviewer, :notes, CAST(:corrections AS JSONB))
            RETURNING id, created_at
        """),
            {
                "analysis_id": analysis_id,
                "decision": request.decision,
                "reviewer": request.reviewer,
                "notes": request.notes,
                "corrections": json.dumps(request.corrections)
                if request.corrections
                else None,
            },
        ).one()
    return {
        "id": row.id,
        "analysis_id": analysis_id,
        **request.dict(),
        "created_at": row.created_at,
    }


@app.get("/analyses/{analysis_id}/reviews")
def list_analysis_reviews(analysis_id: int):
    if not analyst_feature_enabled():
        raise HTTPException(status_code=404, detail="Analyst feature is disabled")

    with get_engine().connect() as conn:
        rows = conn.execute(
            text("""
            SELECT id, analysis_id, decision, reviewer, notes,
                   corrections_json AS corrections, created_at
            FROM investment_analysis_reviews
            WHERE analysis_id = :analysis_id
            ORDER BY created_at DESC
        """),
            {"analysis_id": analysis_id},
        ).all()
    return [row_to_dict(row) for row in rows]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)

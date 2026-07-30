import json
import os
from typing import Any, Dict, List

import requests
from sqlalchemy import text

from app.utils.db import get_engine


PROMPT_VERSION = "investment-analyst-v1"
DEFAULT_MODEL = "gpt-5.4-mini"

ANALYSIS_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "confidence", "summary", "strengths", "risks", "due_diligence_questions"],
    "properties": {
        "verdict": {"type": "string", "enum": ["attractive", "needs_review", "unattractive"]},
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "summary": {"type": "string"},
        "strengths": {"type": "array", "items": {"$ref": "#/$defs/finding"}},
        "risks": {"type": "array", "items": {"$ref": "#/$defs/finding"}},
        "due_diligence_questions": {"type": "array", "items": {"type": "string"}},
    },
    "$defs": {
        "finding": {
            "type": "object",
            "additionalProperties": False,
            "required": ["claim", "evidence_ids"],
            "properties": {
                "claim": {"type": "string"},
                "evidence_ids": {"type": "array", "items": {"type": "string"}},
            },
        }
    },
}


def _rows(conn, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [dict(row._mapping) for row in conn.execute(text(query), params)]


def retrieve_evidence(source: str, source_listing_id: str) -> Dict[str, Any]:
    engine = get_engine()
    with engine.connect() as conn:
        listing_rows = _rows(conn, """
            SELECT lc.*, lms.county_name, lms.county_median_sale_price,
                   lms.pct_below_or_above_median, lms.market_score, lms.score_reason,
                   lms.scored_at
            FROM listings_current lc
            LEFT JOIN listing_market_scores lms
              ON lms.source = lc.source AND lms.source_listing_id = lc.source_listing_id
            WHERE lc.source = :source AND lc.source_listing_id = :listing_id
        """, {"source": source, "listing_id": source_listing_id})
        if not listing_rows:
            raise LookupError("Listing not found")
        listing = listing_rows[0]

        history = _rows(conn, """
            SELECT list_price, status, days_on_market, price_per_sqft, snapshot_timestamp
            FROM listing_history
            WHERE source = :source AND source_listing_id = :listing_id
            ORDER BY snapshot_timestamp DESC LIMIT 12
        """, {"source": source, "listing_id": source_listing_id})

        county_sales = []
        if listing.get("county_name"):
            county_sales = _rows(conn, """
                SELECT period_date, median_sale_price, homes_sold, new_listings,
                       active_listings, median_days_on_market
                FROM county_sales WHERE LOWER(county_name) = LOWER(:county)
                ORDER BY period_date DESC LIMIT 12
            """, {"county": listing["county_name"]})

        comparables = _rows(conn, """
            SELECT source, source_listing_id, address, list_price, beds, baths, sqft,
                   property_type, days_on_market, price_per_sqft
            FROM listings_current
            WHERE LOWER(city) = LOWER(:city) AND LOWER(state) = LOWER(:state)
              AND NOT (source = :source AND source_listing_id = :listing_id)
              AND (:property_type IS NULL OR property_type = :property_type)
              AND (:sqft IS NULL OR sqft BETWEEN :sqft * 0.75 AND :sqft * 1.25)
            ORDER BY ABS(COALESCE(sqft, :sqft, 0) - COALESCE(:sqft, sqft, 0)),
                     ABS(COALESCE(list_price, 0) - COALESCE(:list_price, 0))
            LIMIT 8
        """, {"city": listing.get("city"), "state": listing.get("state"),
                 "source": source, "listing_id": source_listing_id,
                 "property_type": listing.get("property_type"), "sqft": listing.get("sqft"),
                 "list_price": listing.get("list_price")})

    evidence = []
    for evidence_type, records in (("listing", [listing]), ("history", history),
                                   ("county_market", county_sales), ("listing_comparable", comparables)):
        for index, record in enumerate(records, 1):
            evidence.append({"evidence_id": f"{evidence_type}-{index}",
                             "evidence_type": evidence_type, "data": record})
    return {"source": source, "source_listing_id": source_listing_id, "evidence": evidence}


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def generate_analysis(evidence: Dict[str, Any], model: str = None) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required to generate an analysis")
    selected_model = model or os.getenv("OPENAI_MODEL", DEFAULT_MODEL)
    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": selected_model,
                "instructions": (
                    "You are a cautious real-estate investment analyst. Use only the supplied evidence. "
                    "The market score is deterministic and must not be recalculated. Distinguish active "
                    "listing comparables from closed sales. Every strength and risk must cite valid evidence_ids. "
                    "Call out missing evidence and never invent rent, repair, tax, title, or financing facts."
                ),
                "input": json.dumps(evidence, default=_json_default),
                "text": {"format": {"type": "json_schema", "name": "investment_analysis",
                                    "strict": True, "schema": ANALYSIS_SCHEMA}},
            },
            timeout=90,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise RuntimeError(f"OpenAI analysis request failed: {exc}") from exc
    output_text = payload.get("output_text")
    if not output_text:
        for item in payload.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    output_text = content.get("text")
                    break
    if not output_text:
        raise RuntimeError("OpenAI response did not contain structured output")
    analysis = json.loads(output_text)
    valid_ids = {item["evidence_id"] for item in evidence["evidence"]}
    cited_ids = {citation for group in (analysis["strengths"], analysis["risks"])
                 for finding in group for citation in finding["evidence_ids"]}
    invalid_ids = cited_ids - valid_ids
    if invalid_ids:
        raise RuntimeError(f"Analysis cited unknown evidence IDs: {sorted(invalid_ids)}")
    return {"model": selected_model, "analysis": analysis}


def create_analysis(source: str, source_listing_id: str, model: str = None) -> Dict[str, Any]:
    evidence = retrieve_evidence(source, source_listing_id)
    generated = generate_analysis(evidence, model)
    with get_engine().begin() as conn:
        row = conn.execute(text("""
            INSERT INTO investment_analyses
                (source, source_listing_id, model, prompt_version, evidence_json, analysis_json)
            VALUES (:source, :listing_id, :model, :prompt_version,
                    CAST(:evidence AS JSONB), CAST(:analysis AS JSONB))
            RETURNING id, created_at
        """), {"source": source, "listing_id": source_listing_id, "model": generated["model"],
                "prompt_version": PROMPT_VERSION,
                "evidence": json.dumps(evidence, default=_json_default),
                "analysis": json.dumps(generated["analysis"])}).one()
    return {"id": row.id, "source": source, "source_listing_id": source_listing_id,
            "model": generated["model"], "prompt_version": PROMPT_VERSION,
            "evidence": evidence, "analysis": generated["analysis"], "created_at": row.created_at}

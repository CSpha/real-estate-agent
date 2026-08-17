from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from app.db import get_engine
from app.market.listing_eligibility import evaluate_listing_eligibility


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "shadow_scoring_review.csv"

LISTINGS_SQL = text(
    """
    SELECT
        listing.source,
        listing.source_listing_id,
        listing.address,
        listing.city,
        listing.state,
        listing.zip,
        listing.property_type,
        listing.status,
        listing.list_price,
        listing.beds,
        listing.baths,
        listing.sqft,
        listing.lot_size_acres,
        listing.days_on_market,
        listing.price_per_sqft,
        listing.scoring_eligible AS stored_scoring_eligible,
        listing.alert_eligible,
        market.market_score,
        market.score_reason AS market_score_reason,
        market.county_median_sale_price,
        market.pct_below_or_above_median,
        valuation.status AS comparable_status,
        valuation.available_comparable_count,
        valuation.confidence_label AS comparable_confidence,
        valuation.estimated_value,
        valuation.list_price_discount_pct AS valuation_discount_pct,
        deal.status AS deal_score_status,
        deal.total_points AS deal_score_total,
        deal.coverage_pct AS deal_score_coverage_pct,
        deal.normalized_available_score,
        COALESCE(
            deal.total_points,
            deal.normalized_available_score
        ) AS review_deal_score
    FROM listings_current listing
    LEFT JOIN listing_market_scores market
      ON market.source = listing.source
     AND market.source_listing_id = listing.source_listing_id
    LEFT JOIN LATERAL (
        SELECT valuation.*
        FROM listing_comparable_valuations valuation
        WHERE valuation.source = listing.source
          AND valuation.source_listing_id = listing.source_listing_id
        ORDER BY valuation.as_of_date DESC, valuation.created_at DESC
        LIMIT 1
    ) valuation ON TRUE
    LEFT JOIN LATERAL (
        SELECT score.*
        FROM deal_score_v2_scores score
        WHERE score.valuation_id = valuation.id
        ORDER BY score.created_at DESC
        LIMIT 1
    ) deal ON TRUE
    WHERE listing.source = :source
    ORDER BY market.market_score DESC NULLS LAST,
             COALESCE(
                 deal.total_points,
                 deal.normalized_available_score
             ) DESC NULLS LAST,
             listing.list_price,
             listing.source_listing_id
    """
)


def _csv_value(value: Any) -> Any:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    if isinstance(value, bool):
        return str(value).lower()
    return value


def build_shadow_scoring_review(
    *,
    source: str = "rentcast",
    output_path: Path = DEFAULT_OUTPUT,
    engine: Engine | None = None,
) -> dict[str, Any]:
    engine = engine or get_engine()
    with engine.connect() as connection:
        rows = [dict(row) for row in connection.execute(
            LISTINGS_SQL, {"source": source}
        ).mappings()]

    reviewed: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    missing_counts: Counter[str] = Counter()
    for rank, row in enumerate(rows, start=1):
        decision = evaluate_listing_eligibility(row)
        status_counts[decision.status] += 1
        type_counts[str(row.get("property_type") or "missing")] += 1
        for field in ("beds", "baths", "sqft", "days_on_market"):
            if row.get(field) is None:
                missing_counts[field] += 1
        review_reasons = list(decision.reasons)
        priority_reasons: list[str] = []
        if (
            row.get("market_score") is not None
            and row["market_score"] >= 80
            and decision.status != "eligible"
        ):
            priority_reasons.append("High county score outside automated policy")
        if (
            row.get("valuation_discount_pct") is not None
            and row["valuation_discount_pct"] >= Decimal("0.35")
        ):
            priority_reasons.append("Estimated discount is at least 35%")
        if row.get("comparable_confidence") in {"low", "very_low"}:
            priority_reasons.append("Comparable confidence is low")
        review_reasons.extend(priority_reasons)
        reviewed.append(
            {
                "review_rank": rank,
                **row,
                "policy_status": decision.status,
                "policy_scoring_eligible": decision.scoring_eligible,
                "comparable_ready": decision.comparable_ready,
                "policy_reasons": "; ".join(decision.reasons),
                "manual_review_reasons": "; ".join(review_reasons),
                "stored_policy_mismatch": (
                    bool(row["stored_scoring_eligible"])
                    != decision.scoring_eligible
                ),
                "manual_review_priority": (
                    "high" if priority_reasons else "normal"
                ),
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if reviewed:
        with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(reviewed[0]))
            writer.writeheader()
            writer.writerows(
                {key: _csv_value(value) for key, value in row.items()}
                for row in reviewed
            )

    top_15 = reviewed[:15]
    summary = {
        "source": source,
        "output_path": str(output_path),
        "listing_count": len(reviewed),
        "policy_status_counts": dict(sorted(status_counts.items())),
        "property_type_counts": dict(type_counts.most_common()),
        "missing_field_counts": dict(missing_counts),
        "stored_policy_mismatch_count": sum(
            bool(row["stored_policy_mismatch"]) for row in reviewed
        ),
        "high_priority_manual_review_count": sum(
            row["manual_review_priority"] == "high" for row in reviewed
        ),
        "top_15_noneligible_count": sum(
            row["policy_status"] != "eligible" for row in top_15
        ),
        "comparable_ready_count": sum(
            bool(row["comparable_ready"]) for row in reviewed
        ),
        "deal_score_available_count": sum(
            row["review_deal_score"] is not None for row in reviewed
        ),
        "alert_eligible_count": sum(
            bool(row["alert_eligible"]) for row in reviewed
        ),
        "comparable_source_is_provisional": True,
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate an all-listing shadow scoring review CSV."
    )
    parser.add_argument("--source", default="rentcast")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    summary = build_shadow_scoring_review(
        source=args.source,
        output_path=args.output,
    )
    print("Shadow scoring review complete:")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

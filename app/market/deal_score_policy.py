"""Versioned, database-backed thresholds for Deal Score v2."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection


DEFAULT_POLICY_VERSION = "deal-score-thresholds-v1"
DEFAULT_THRESHOLDS = {
    "minimum_confidence_score": "40.0",
    "full_credit_discount": "0.30",
    "full_credit_cumulative_reduction": "0.15",
    "full_credit_reduction_count": "3",
    "recent_reduction_full_credit_days": 30,
    "recent_reduction_partial_credit_days": 90,
    "days_on_market_zero_point_ratio": "0.75",
    "days_on_market_full_credit_ratio": "2.00",
    "max_listing_evidence_age_days": 30,
    "max_market_context_age_days": 180,
    "market_momentum_floor_change": "-0.10",
    "market_momentum_full_credit_change": "0.10",
    "full_credit_inventory_months": "2.00",
    "zero_credit_inventory_months": "8.00",
    "zero_credit_sales_to_new_listings": "0.50",
    "full_credit_sales_to_new_listings": "1.00",
}


@dataclass(frozen=True)
class DealScorePolicy:
    version: str
    thresholds: dict[str, Any]

    def decimal(self, key: str) -> Decimal:
        return Decimal(str(self.thresholds[key]))

    def integer(self, key: str) -> int:
        return int(self.thresholds[key])


DEFAULT_POLICY = DealScorePolicy(DEFAULT_POLICY_VERSION, DEFAULT_THRESHOLDS)


def load_active_policy(connection: Connection) -> DealScorePolicy:
    row = connection.execute(
        text(
            "SELECT policy_version, thresholds FROM deal_score_policies "
            "WHERE is_active = TRUE"
        )
    ).mappings().one()
    thresholds = dict(row["thresholds"])
    missing = set(DEFAULT_THRESHOLDS) - set(thresholds)
    if missing:
        raise ValueError(f"Deal Score policy is missing thresholds: {sorted(missing)}")
    return DealScorePolicy(row["policy_version"], thresholds)

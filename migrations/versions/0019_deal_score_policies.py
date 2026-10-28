"""Add versioned Deal Score threshold policies.

Revision ID: 0019_deal_score_policies
Revises: 0018_rental_forecasts
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0019_deal_score_policies"
down_revision: str | Sequence[str] | None = "0018_rental_forecasts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deal_score_policies",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("policy_version", sa.Text(), nullable=False, unique=True),
        sa.Column("thresholds", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.execute("CREATE UNIQUE INDEX uq_deal_score_policies_active ON deal_score_policies (is_active) WHERE is_active")
    policy_table = sa.table(
        "deal_score_policies",
        sa.column("policy_version", sa.Text()),
        sa.column("thresholds", postgresql.JSONB()),
        sa.column("is_active", sa.Boolean()),
    )
    op.bulk_insert(
        policy_table,
        [{
            "policy_version": "deal-score-thresholds-v1",
            "thresholds": {
                "minimum_confidence_score": "40.0", "full_credit_discount": "0.30",
                "full_credit_cumulative_reduction": "0.15", "full_credit_reduction_count": "3",
                "recent_reduction_full_credit_days": 30, "recent_reduction_partial_credit_days": 90,
                "days_on_market_zero_point_ratio": "0.75", "days_on_market_full_credit_ratio": "2.00",
                "max_listing_evidence_age_days": 30, "max_market_context_age_days": 180,
                "market_momentum_floor_change": "-0.10", "market_momentum_full_credit_change": "0.10",
                "full_credit_inventory_months": "2.00", "zero_credit_inventory_months": "8.00",
                "zero_credit_sales_to_new_listings": "0.50", "full_credit_sales_to_new_listings": "1.00",
            },
            "is_active": True,
        }],
    )


def downgrade() -> None:
    op.drop_index("uq_deal_score_policies_active", table_name="deal_score_policies")
    op.drop_table("deal_score_policies")

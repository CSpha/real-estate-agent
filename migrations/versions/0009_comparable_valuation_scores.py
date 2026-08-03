"""Persist comparable valuations and Deal Score v2 components.

Revision ID: 0009_comparable_valuation_scores
Revises: 0008_comparable_sales
Create Date: 2026-07-28
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0009_comparable_valuation_scores"
down_revision: str | Sequence[str] | None = "0008_comparable_sales"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "listing_comparable_valuations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("valuation_version", sa.Text(), nullable=False),
        sa.Column("selection_version", sa.Text(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("selected_tier", sa.Text(), nullable=False),
        sa.Column("available_comparable_count", sa.Integer(), nullable=False),
        sa.Column("included_comparable_count", sa.Integer(), nullable=False),
        sa.Column("weighted_median_price_per_sqft", sa.Numeric(12, 2)),
        sa.Column("estimated_value", sa.Numeric(14, 2)),
        sa.Column("estimated_value_low", sa.Numeric(14, 2)),
        sa.Column("estimated_value_high", sa.Numeric(14, 2)),
        sa.Column("list_price", sa.Numeric(14, 2)),
        sa.Column("list_price_discount_amount", sa.Numeric(14, 2)),
        sa.Column("list_price_discount_pct", sa.Numeric(8, 5)),
        sa.Column("confidence_score", sa.Numeric(5, 1), nullable=False),
        sa.Column("confidence_label", sa.Text(), nullable=False),
        sa.Column(
            "calculation_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('valued', 'insufficient_subject_data', "
            "'insufficient_comparables')",
            name="ck_listing_comparable_valuations_status",
        ),
        sa.CheckConstraint(
            "available_comparable_count >= 0 "
            "AND included_comparable_count >= 0",
            name="ck_listing_comparable_valuations_counts",
        ),
        sa.CheckConstraint(
            "confidence_score BETWEEN 0 AND 100",
            name="ck_listing_comparable_valuations_confidence",
        ),
        sa.UniqueConstraint(
            "source",
            "source_listing_id",
            "valuation_version",
            "as_of_date",
            "input_fingerprint",
            name="uq_listing_comparable_valuations_input",
        ),
    )
    op.create_index(
        "ix_listing_comparable_valuations_listing_created",
        "listing_comparable_valuations",
        [
            "source",
            "source_listing_id",
            sa.text("created_at DESC"),
        ],
    )

    op.create_table(
        "deal_score_v2_components",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "valuation_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "listing_comparable_valuations.id",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column("scoring_version", sa.Text(), nullable=False),
        sa.Column("component_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("points", sa.Numeric(5, 2)),
        sa.Column("max_points", sa.Numeric(5, 2), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "calculation_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('available', 'unavailable')",
            name="ck_deal_score_v2_components_status",
        ),
        sa.CheckConstraint(
            "max_points > 0 AND "
            "(points IS NULL OR points BETWEEN 0 AND max_points)",
            name="ck_deal_score_v2_components_points",
        ),
        sa.UniqueConstraint(
            "valuation_id",
            "scoring_version",
            "component_key",
            name="uq_deal_score_v2_components_valuation",
        ),
    )
    op.create_index(
        "ix_deal_score_v2_components_version_key",
        "deal_score_v2_components",
        ["scoring_version", "component_key"],
    )


def downgrade() -> None:
    op.drop_table("deal_score_v2_components")
    op.drop_table("listing_comparable_valuations")

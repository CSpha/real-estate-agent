"""Persist complete and partial Deal Score v2 aggregates.

Revision ID: 0011_deal_score_v2_aggregates
Revises: 0010_deal_component_fingerprints
Create Date: 2026-08-06
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0011_deal_score_v2_aggregates"
down_revision: str | Sequence[str] | None = "0010_deal_component_fingerprints"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deal_score_v2_scores",
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
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("total_points", sa.Numeric(5, 2)),
        sa.Column("available_points", sa.Numeric(5, 2), nullable=False),
        sa.Column("available_max_points", sa.Numeric(5, 2), nullable=False),
        sa.Column("coverage_pct", sa.Numeric(5, 2), nullable=False),
        sa.Column("normalized_available_score", sa.Numeric(5, 2)),
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
            "status IN ('complete', 'partial', 'unavailable')",
            name="ck_deal_score_v2_scores_status",
        ),
        sa.CheckConstraint(
            "available_max_points BETWEEN 0 AND 100 "
            "AND available_points BETWEEN 0 AND available_max_points",
            name="ck_deal_score_v2_scores_available_points",
        ),
        sa.CheckConstraint(
            "coverage_pct BETWEEN 0 AND 100 "
            "AND (normalized_available_score IS NULL "
            "OR normalized_available_score BETWEEN 0 AND 100)",
            name="ck_deal_score_v2_scores_percentages",
        ),
        sa.CheckConstraint(
            "(status = 'complete' AND total_points BETWEEN 0 AND 100) "
            "OR (status <> 'complete' AND total_points IS NULL)",
            name="ck_deal_score_v2_scores_total",
        ),
        sa.UniqueConstraint(
            "valuation_id",
            "scoring_version",
            "input_fingerprint",
            name="uq_deal_score_v2_scores_input",
        ),
    )
    op.create_index(
        "ix_deal_score_v2_scores_version_status",
        "deal_score_v2_scores",
        ["scoring_version", "status"],
    )


def downgrade() -> None:
    op.drop_table("deal_score_v2_scores")

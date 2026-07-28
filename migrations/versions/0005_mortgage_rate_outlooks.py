"""Add versioned mortgage rate outlooks.

Revision ID: 0005_mortgage_rate_outlooks
Revises: 0004_mortgage_rates
Create Date: 2026-07-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0005_mortgage_rate_outlooks"
down_revision: str | Sequence[str] | None = "0004_mortgage_rates"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "mortgage_rate_outlooks",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("product_type", sa.Text(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("horizon_months", sa.Integer(), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=False),
        sa.Column("model_version", sa.Text(), nullable=False),
        sa.Column("probability_lower", sa.Numeric(6, 5), nullable=False),
        sa.Column("probability_stable", sa.Numeric(6, 5), nullable=False),
        sa.Column("probability_higher", sa.Numeric(6, 5), nullable=False),
        sa.Column("confidence", sa.Text(), nullable=False),
        sa.Column("input_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("drivers", postgresql.JSONB(), nullable=False),
        sa.Column("actual_rate_percent", sa.Numeric(6, 3)),
        sa.Column("actual_direction", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "horizon_months IN (1, 3)",
            name="ck_mortgage_rate_outlooks_horizon",
        ),
        sa.CheckConstraint(
            "confidence IN ('low', 'medium', 'high')",
            name="ck_mortgage_rate_outlooks_confidence",
        ),
        sa.CheckConstraint(
            "probability_lower BETWEEN 0 AND 1 "
            "AND probability_stable BETWEEN 0 AND 1 "
            "AND probability_higher BETWEEN 0 AND 1",
            name="ck_mortgage_rate_outlooks_probabilities",
        ),
        sa.CheckConstraint(
            "ABS("
            "probability_lower + probability_stable + probability_higher - 1"
            ") <= 0.00001",
            name="ck_mortgage_rate_outlooks_probability_sum",
        ),
        sa.UniqueConstraint(
            "product_type",
            "as_of_date",
            "horizon_months",
            "model_version",
            name="uq_mortgage_rate_outlook_version",
        ),
    )
    op.create_index(
        "ix_mortgage_rate_outlooks_latest",
        "mortgage_rate_outlooks",
        [
            "product_type",
            "horizon_months",
            sa.text("as_of_date DESC"),
        ],
    )


def downgrade() -> None:
    op.drop_table("mortgage_rate_outlooks")

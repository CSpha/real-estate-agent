"""Add mortgage rate history.

Revision ID: 0004_mortgage_rates
Revises: 0003_alert_outbox
Create Date: 2026-07-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0004_mortgage_rates"
down_revision: str | Sequence[str] | None = "0003_alert_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "mortgage_rate_observations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("product_type", sa.Text(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("rate_percent", sa.Numeric(6, 3), nullable=False),
        sa.Column("points", sa.Numeric(6, 3)),
        sa.Column("methodology_version", sa.Text(), nullable=False),
        sa.Column(
            "source_url",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "rate_percent >= 0 AND rate_percent <= 30",
            name="ck_mortgage_rate_observations_rate",
        ),
        sa.CheckConstraint(
            "points IS NULL OR (points >= 0 AND points <= 10)",
            name="ck_mortgage_rate_observations_points",
        ),
        sa.UniqueConstraint(
            "source",
            "product_type",
            "observation_date",
            name="uq_mortgage_rate_observation",
        ),
    )
    op.create_index(
        "ix_mortgage_rate_observations_product_date",
        "mortgage_rate_observations",
        ["product_type", sa.text("observation_date DESC")],
    )


def downgrade() -> None:
    op.drop_table("mortgage_rate_observations")

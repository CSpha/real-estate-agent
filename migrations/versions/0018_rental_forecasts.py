"""Store reproducible rental growth scenarios.

Revision ID: 0018_rental_forecasts
Revises: 0017_listing_attribute_evidence
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0018_rental_forecasts"
down_revision: str | Sequence[str] | None = "0017_listing_attribute_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "rental_forecast_scenarios",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("rental_analysis_id", sa.BigInteger(), sa.ForeignKey("rental_underwriting_analyses.id", ondelete="CASCADE"), nullable=False),
        sa.Column("forecast_version", sa.Text(), nullable=False),
        sa.Column("scenario_name", sa.Text(), nullable=False),
        sa.Column("annual_rent_growth_pct", sa.Numeric(6, 3), nullable=False),
        sa.Column("horizon_years", sa.Integer(), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("projection_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint("horizon_years BETWEEN 1 AND 30", name="ck_rental_forecast_horizon"),
        sa.UniqueConstraint("rental_analysis_id", "forecast_version", "scenario_name", "input_fingerprint", name="uq_rental_forecast_input"),
    )


def downgrade() -> None:
    op.drop_table("rental_forecast_scenarios")

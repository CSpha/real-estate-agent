"""Add macroeconomic rate signals and Fed expectations.

Revision ID: 0006_macro_rate_signals
Revises: 0005_mortgage_rate_outlooks
Create Date: 2026-07-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0006_macro_rate_signals"
down_revision: str | Sequence[str] | None = "0005_mortgage_rate_outlooks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "macro_rate_observations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("value", sa.Numeric(14, 6), nullable=False),
        sa.Column("units", sa.Text(), nullable=False),
        sa.Column("frequency", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "source",
            "series_id",
            "observation_date",
            name="uq_macro_rate_observation",
        ),
    )
    op.create_index(
        "ix_macro_rate_observations_series_date",
        "macro_rate_observations",
        ["series_id", sa.text("observation_date DESC")],
    )

    op.create_table(
        "fed_expectation_observations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("release_date", sa.Date(), nullable=False),
        sa.Column("panel_type", sa.Text(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("value_tag", sa.Text(), nullable=False),
        sa.Column("horizon_date", sa.Date(), nullable=False),
        sa.Column("aggregation", sa.Text(), nullable=False),
        sa.Column("value_percent", sa.Numeric(8, 4), nullable=False),
        sa.Column("respondent_count", sa.Integer()),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "source",
            "release_date",
            "panel_type",
            "value_tag",
            "aggregation",
            name="uq_fed_expectation_observation",
        ),
    )
    op.create_index(
        "ix_fed_expectations_latest",
        "fed_expectation_observations",
        [
            "subject",
            sa.text("release_date DESC"),
            "horizon_date",
        ],
    )


def downgrade() -> None:
    op.drop_table("fed_expectation_observations")
    op.drop_table("macro_rate_observations")

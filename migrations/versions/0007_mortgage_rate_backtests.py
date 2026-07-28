"""Add mortgage rate backtest and calibration results.

Revision ID: 0007_mortgage_rate_backtests
Revises: 0006_macro_rate_signals
Create Date: 2026-07-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0007_mortgage_rate_backtests"
down_revision: str | Sequence[str] | None = "0006_macro_rate_signals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_fed_expectation_observation",
        "fed_expectation_observations",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_fed_expectation_value",
        "fed_expectation_observations",
        ["source", "panel_type", "value_tag", "aggregation"],
    )

    op.create_table(
        "mortgage_rate_backtest_runs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("product_type", sa.Text(), nullable=False),
        sa.Column("model_version", sa.Text(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("origin_spacing_weeks", sa.Integer(), nullable=False),
        sa.Column("stable_band", sa.Numeric(6, 3), nullable=False),
        sa.Column("training_fraction", sa.Numeric(5, 4), nullable=False),
        sa.Column("configuration", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )

    op.create_table(
        "mortgage_rate_backtest_predictions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "mortgage_rate_backtest_runs.id",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column("horizon_months", sa.Integer(), nullable=False),
        sa.Column("split", sa.Text(), nullable=False),
        sa.Column("origin_date", sa.Date(), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=False),
        sa.Column("current_rate", sa.Numeric(6, 3), nullable=False),
        sa.Column("actual_rate", sa.Numeric(6, 3), nullable=False),
        sa.Column("actual_direction", sa.Text(), nullable=False),
        sa.Column("probability_lower", sa.Numeric(6, 5), nullable=False),
        sa.Column("probability_stable", sa.Numeric(6, 5), nullable=False),
        sa.Column("probability_higher", sa.Numeric(6, 5), nullable=False),
        sa.Column("combined_strength", sa.Numeric(8, 6), nullable=False),
        sa.Column("raw_brier_score", sa.Numeric(8, 6), nullable=False),
        sa.Column("calibrated_brier_score", sa.Numeric(8, 6)),
        sa.Column("input_snapshot", postgresql.JSONB(), nullable=False),
        sa.CheckConstraint(
            "horizon_months IN (1, 3)",
            name="ck_backtest_predictions_horizon",
        ),
        sa.CheckConstraint(
            "split IN ('training', 'holdout')",
            name="ck_backtest_predictions_split",
        ),
        sa.UniqueConstraint(
            "run_id",
            "horizon_months",
            "origin_date",
            name="uq_backtest_prediction_origin",
        ),
    )

    op.create_table(
        "mortgage_rate_calibrations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey(
                "mortgage_rate_backtest_runs.id",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column("horizon_months", sa.Integer(), nullable=False),
        sa.Column("calibration_version", sa.Text(), nullable=False),
        sa.Column("strength_scale", sa.Numeric(6, 3), nullable=False),
        sa.Column("training_count", sa.Integer(), nullable=False),
        sa.Column("holdout_count", sa.Integer(), nullable=False),
        sa.Column("training_brier_score", sa.Numeric(8, 6), nullable=False),
        sa.Column("holdout_raw_brier_score", sa.Numeric(8, 6), nullable=False),
        sa.Column(
            "holdout_calibrated_brier_score",
            sa.Numeric(8, 6),
            nullable=False,
        ),
        sa.Column("holdout_baseline_brier_score", sa.Numeric(8, 6), nullable=False),
        sa.Column("approved", sa.Boolean(), nullable=False),
        sa.Column("approval_reason", sa.Text(), nullable=False),
        sa.UniqueConstraint(
            "run_id",
            "horizon_months",
            name="uq_mortgage_rate_calibration_horizon",
        ),
    )


def downgrade() -> None:
    op.drop_table("mortgage_rate_calibrations")
    op.drop_table("mortgage_rate_backtest_predictions")
    op.drop_table("mortgage_rate_backtest_runs")
    op.drop_constraint(
        "uq_fed_expectation_value",
        "fed_expectation_observations",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_fed_expectation_observation",
        "fed_expectation_observations",
        [
            "source",
            "release_date",
            "panel_type",
            "value_tag",
            "aggregation",
        ],
    )

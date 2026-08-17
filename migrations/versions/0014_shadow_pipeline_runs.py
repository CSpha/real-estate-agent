"""Track alert-isolated shadow pipeline runs.

Revision ID: 0014_shadow_pipeline_runs
Revises: 0013_scoring_eligibility
Create Date: 2026-08-16
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0014_shadow_pipeline_runs"
down_revision: str | Sequence[str] | None = "0013_scoring_eligibility"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "shadow_pipeline_runs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("listing_count", sa.Integer()),
        sa.Column("scoring_eligible_count", sa.Integer()),
        sa.Column("comparable_ready_count", sa.Integer()),
        sa.Column("supported_valuation_count", sa.Integer()),
        sa.Column("alert_eligible_count", sa.Integer()),
        sa.Column("error_message", sa.Text()),
        sa.Column(
            "details_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed')",
            name="ck_shadow_pipeline_runs_status",
        ),
    )
    op.create_index(
        "ix_shadow_pipeline_runs_source_started",
        "shadow_pipeline_runs",
        ["source", sa.text("started_at DESC")],
    )


def downgrade() -> None:
    op.drop_table("shadow_pipeline_runs")

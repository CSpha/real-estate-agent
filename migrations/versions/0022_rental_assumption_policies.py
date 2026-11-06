"""Add versioned rental forecast assumption policies.

Revision ID: 0022_rental_assumption_policies
Revises: 0021_listing_feed_presence

Moves the hard-coded rent growth scenarios and the rent-to-cash-flow
pass-through out of application code. Version 1 holds the previous constants so
behavior is unchanged. Stored forecasts record the assumption version used.
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0022_rental_assumption_policies"
down_revision: str | Sequence[str] | None = "0021_listing_feed_presence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

V1 = "rental-assumptions-v1"


def upgrade() -> None:
    op.create_table(
        "rental_assumption_policies",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("policy_version", sa.Text(), nullable=False, unique=True),
        sa.Column("assumptions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.execute("CREATE UNIQUE INDEX uq_rental_assumption_policies_active ON rental_assumption_policies (is_active) WHERE is_active")
    policy_table = sa.table(
        "rental_assumption_policies",
        sa.column("policy_version", sa.Text()),
        sa.column("assumptions", postgresql.JSONB()),
        sa.column("is_active", sa.Boolean()),
    )
    op.bulk_insert(
        policy_table,
        [{
            "policy_version": V1,
            "assumptions": {
                "scenario_growth_pct": {"conservative": "-1.0", "base": "2.0", "upside": "4.0"},
                "rent_to_cash_flow_pass_through": "0.74",
            },
            "is_active": True,
        }],
    )
    op.add_column("rental_forecast_scenarios", sa.Column("assumption_version", sa.Text()))
    op.execute(f"UPDATE rental_forecast_scenarios SET assumption_version = '{V1}'")


def downgrade() -> None:
    op.drop_column("rental_forecast_scenarios", "assumption_version")
    op.drop_index("uq_rental_assumption_policies_active", table_name="rental_assumption_policies")
    op.drop_table("rental_assumption_policies")

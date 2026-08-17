"""Add persistent alert eligibility for shadow listings.

Revision ID: 0012_shadow_alert_eligibility
Revises: 0011_deal_score_v2_aggregates
Create Date: 2026-08-16
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0012_shadow_alert_eligibility"
down_revision: str | Sequence[str] | None = "0011_deal_score_v2_aggregates"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "listings_current",
        sa.Column(
            "alert_eligible",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
    )
    op.add_column(
        "listing_history",
        sa.Column(
            "alert_eligible",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_listings_current_alert_eligible",
        "listings_current",
        ["alert_eligible"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_listings_current_alert_eligible",
        table_name="listings_current",
    )
    op.drop_column("listing_history", "alert_eligible")
    op.drop_column("listings_current", "alert_eligible")

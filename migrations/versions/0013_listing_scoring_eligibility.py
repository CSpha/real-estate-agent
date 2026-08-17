"""Add explicit listing eligibility for automated scoring.

Revision ID: 0013_scoring_eligibility
Revises: 0012_shadow_alert_eligibility
Create Date: 2026-08-16
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0013_scoring_eligibility"
down_revision: str | Sequence[str] | None = "0012_shadow_alert_eligibility"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "listings_current",
        sa.Column(
            "scoring_eligible",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
    )
    op.add_column(
        "listing_history",
        sa.Column(
            "scoring_eligible",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_listings_current_scoring_eligible",
        "listings_current",
        ["scoring_eligible"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_listings_current_scoring_eligible",
        table_name="listings_current",
    )
    op.drop_column("listing_history", "scoring_eligible")
    op.drop_column("listings_current", "scoring_eligible")

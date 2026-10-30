"""Track listings that disappear from a provider feed.

Revision ID: 0021_listing_feed_presence
Revises: 0020_provider_sync_state

A listing that stops appearing in a complete provider sync keeps its current
row and history; `missing_from_feed_since` records when it was first absent and
is cleared if the listing returns.
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0021_listing_feed_presence"
down_revision: str | Sequence[str] | None = "0020_provider_sync_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "listings_current",
        sa.Column("missing_from_feed_since", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    op.drop_column("listings_current", "missing_from_feed_since")

"""Add a transactional alert outbox.

Revision ID: 0003_alert_outbox
Revises: 0002_saved_searches
Create Date: 2026-07-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0003_alert_outbox"
down_revision: str | Sequence[str] | None = "0002_saved_searches"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "alert_outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("alert_type", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "status IN ("
            "'pending', 'processing', 'retryable_failed', "
            "'permanent_failed', 'sent'"
            ")",
            name="ck_alert_outbox_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_alert_outbox_attempt_count",
        ),
        sa.UniqueConstraint(
            "alert_type",
            "source",
            "source_listing_id",
            "event_timestamp",
            name="uq_alert_outbox_event",
        ),
    )
    op.create_index(
        "ix_alert_outbox_delivery",
        "alert_outbox",
        ["status", "available_at", "id"],
    )


def downgrade() -> None:
    op.drop_table("alert_outbox")

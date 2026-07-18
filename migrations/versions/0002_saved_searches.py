"""Add versioned saved searches and listing evaluations.

Revision ID: 0002_saved_searches
Revises: 0001_initial_schema
Create Date: 2026-07-18
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0002_saved_searches"
down_revision: str | Sequence[str] | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "listings_current",
        sa.Column("lot_size_acres", sa.Numeric(12, 3)),
    )
    op.add_column(
        "listing_history",
        sa.Column("lot_size_acres", sa.Numeric(12, 3)),
    )

    op.create_table(
        "saved_searches",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column(
            "criteria_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "criteria_version",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column(
            "enabled",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.Column(
            "notification_mode",
            sa.Text(),
            server_default=sa.text("'immediate'"),
            nullable=False,
        ),
        sa.Column(
            "cooldown_minutes",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "criteria_version >= 1",
            name="ck_saved_searches_criteria_version",
        ),
        sa.CheckConstraint(
            "notification_mode IN ('immediate', 'digest')",
            name="ck_saved_searches_notification_mode",
        ),
        sa.CheckConstraint(
            "cooldown_minutes >= 0 AND cooldown_minutes <= 10080",
            name="ck_saved_searches_cooldown_minutes",
        ),
    )
    op.create_index(
        "uq_saved_searches_name_ci",
        "saved_searches",
        [sa.text("LOWER(name)")],
        unique=True,
    )
    op.create_index(
        "ix_saved_searches_enabled",
        "saved_searches",
        ["enabled"],
    )

    op.create_table(
        "listing_search_evaluations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "saved_search_id",
            sa.BigInteger(),
            sa.ForeignKey("saved_searches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("criteria_version", sa.Integer(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("listing_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("event_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("is_match", sa.Boolean(), nullable=False),
        sa.Column("became_match", sa.Boolean(), nullable=False),
        sa.Column(
            "match_reasons",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "failed_reasons",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "listing_state_timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "evaluated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "criteria_version >= 1",
            name="ck_listing_search_evaluations_criteria_version",
        ),
        sa.UniqueConstraint(
            "event_fingerprint",
            name="uq_listing_search_evaluations_event",
        ),
    )
    op.create_index(
        "ix_listing_search_evaluations_latest",
        "listing_search_evaluations",
        [
            "saved_search_id",
            "source",
            "source_listing_id",
            sa.text("evaluated_at DESC"),
            sa.text("id DESC"),
        ],
    )
    op.create_index(
        "ix_listing_search_evaluations_new_matches",
        "listing_search_evaluations",
        ["saved_search_id", "became_match", sa.text("evaluated_at DESC")],
    )


def downgrade() -> None:
    op.drop_table("listing_search_evaluations")
    op.drop_table("saved_searches")
    op.drop_column("listing_history", "lot_size_acres")
    op.drop_column("listings_current", "lot_size_acres")

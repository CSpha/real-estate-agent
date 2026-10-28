"""Store verified property-attribute evidence separately from listing feeds.

Revision ID: 0017_listing_attribute_evidence
Revises: 0016_rental_underwriting
Create Date: 2026-09-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0017_listing_attribute_evidence"
down_revision: str | Sequence[str] | None = "0016_rental_underwriting"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "listing_property_attribute_evidence",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("attribute_name", sa.Text(), nullable=False),
        sa.Column("numeric_value", sa.Numeric(12, 1), nullable=False),
        sa.Column("evidence_source", sa.Text(), nullable=False),
        sa.Column("source_record_id", sa.Text(), nullable=False),
        sa.Column("match_method", sa.Text(), nullable=False),
        sa.Column("verification_status", sa.Text(), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "evidence_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("numeric_value > 0", name="ck_attribute_evidence_positive"),
        sa.CheckConstraint(
            "verification_status IN ('candidate', 'verified', 'rejected')",
            name="ck_attribute_evidence_status",
        ),
        sa.UniqueConstraint(
            "source",
            "source_listing_id",
            "attribute_name",
            "evidence_source",
            "source_record_id",
            "input_fingerprint",
            name="uq_attribute_evidence_input",
        ),
    )
    op.create_index(
        "ix_attribute_evidence_listing_attribute_created",
        "listing_property_attribute_evidence",
        ["source", "source_listing_id", "attribute_name", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_table("listing_property_attribute_evidence")

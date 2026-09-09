"""Persist rejected provider records for review.

Revision ID: 0015_ingest_errors
Revises: 0014_shadow_pipeline_runs
Create Date: 2026-09-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0015_ingest_errors"
down_revision: str | Sequence[str] | None = "0014_shadow_pipeline_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ingest_errors",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("source_record_id", sa.Text()),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column(
            "raw_record_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "source",
            "stage",
            "payload_hash",
            "error_code",
            name="uq_ingest_errors_payload_reason",
        ),
    )
    op.create_index(
        "ix_ingest_errors_source_detected",
        "ingest_errors",
        ["source", sa.text("detected_at DESC")],
    )


def downgrade() -> None:
    op.drop_table("ingest_errors")

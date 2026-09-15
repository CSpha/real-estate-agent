"""Add immutable rent estimates and rental underwriting analyses.

Revision ID: 0016_rental_underwriting
Revises: 0015_ingest_errors
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0016_rental_underwriting"
down_revision: str | Sequence[str] | None = "0015_ingest_errors"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "listing_rent_estimates",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("monthly_rent", sa.Numeric(12, 2), nullable=False),
        sa.Column("rent_range_low", sa.Numeric(12, 2)),
        sa.Column("rent_range_high", sa.Numeric(12, 2)),
        sa.Column("comparable_count", sa.Integer(), nullable=False),
        sa.Column(
            "request_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "response_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("monthly_rent > 0", name="ck_rent_estimates_positive"),
        sa.CheckConstraint(
            "comparable_count >= 0", name="ck_rent_estimates_comparable_count"
        ),
        sa.UniqueConstraint(
            "source",
            "source_listing_id",
            "provider",
            "as_of_date",
            "request_fingerprint",
            name="uq_rent_estimates_input",
        ),
    )
    op.create_index(
        "ix_rent_estimates_listing_date",
        "listing_rent_estimates",
        ["source", "source_listing_id", sa.text("as_of_date DESC")],
    )

    op.create_table(
        "rental_underwriting_analyses",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "rent_estimate_id",
            sa.BigInteger(),
            sa.ForeignKey("listing_rent_estimates.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("analysis_version", sa.Text(), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("purchase_price", sa.Numeric(14, 2), nullable=False),
        sa.Column("monthly_rent", sa.Numeric(12, 2), nullable=False),
        sa.Column("gross_yield_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("monthly_noi", sa.Numeric(12, 2), nullable=False),
        sa.Column("cap_rate_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("monthly_cash_flow", sa.Numeric(12, 2), nullable=False),
        sa.Column("target_dscr", sa.Numeric(5, 3), nullable=False),
        sa.Column("underwritten_dscr", sa.Numeric(6, 3)),
        sa.Column("interest_rate_pct", sa.Numeric(6, 3), nullable=False),
        sa.Column("term_years", sa.Integer(), nullable=False),
        sa.Column("max_ltv_pct", sa.Numeric(6, 3), nullable=False),
        sa.Column("max_loan_by_dscr", sa.Numeric(14, 2), nullable=False),
        sa.Column("ltv_cap_loan", sa.Numeric(14, 2), nullable=False),
        sa.Column("target_loan_amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("target_ltv_pct", sa.Numeric(6, 3), nullable=False),
        sa.Column("down_payment", sa.Numeric(14, 2), nullable=False),
        sa.Column("monthly_principal_interest", sa.Numeric(12, 2), nullable=False),
        sa.Column("monthly_pitia", sa.Numeric(12, 2), nullable=False),
        sa.Column("minimum_rent_for_target_dscr", sa.Numeric(12, 2), nullable=False),
        sa.Column("constraint", sa.Text(), nullable=False),
        sa.Column(
            "calculation_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("purchase_price > 0", name="ck_rental_analysis_price"),
        sa.CheckConstraint("monthly_rent > 0", name="ck_rental_analysis_rent"),
        sa.CheckConstraint("target_dscr > 0", name="ck_rental_analysis_target_dscr"),
        sa.CheckConstraint(
            "interest_rate_pct >= 0 AND interest_rate_pct <= 30",
            name="ck_rental_analysis_rate",
        ),
        sa.CheckConstraint(
            "max_ltv_pct > 0 AND max_ltv_pct <= 100",
            name="ck_rental_analysis_max_ltv",
        ),
        sa.UniqueConstraint(
            "rent_estimate_id",
            "analysis_version",
            "input_fingerprint",
            name="uq_rental_analysis_input",
        ),
    )
    op.create_index(
        "ix_rental_analysis_estimate_created",
        "rental_underwriting_analyses",
        ["rent_estimate_id", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_table("rental_underwriting_analyses")
    op.drop_table("listing_rent_estimates")

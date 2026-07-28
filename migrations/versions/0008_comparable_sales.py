"""Add property-level comparable sales.

Revision ID: 0008_comparable_sales
Revises: 0007_mortgage_rate_backtests
Create Date: 2026-07-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0008_comparable_sales"
down_revision: str | Sequence[str] | None = "0007_mortgage_rate_backtests"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "comparable_sales_raw",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_sale_id", sa.Text(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "raw_record_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "source",
            "source_sale_id",
            "payload_hash",
            name="uq_comparable_sales_raw_source_sale_payload",
        ),
    )
    op.create_index(
        "ix_comparable_sales_raw_source_sale_ingested",
        "comparable_sales_raw",
        ["source", "source_sale_id", sa.text("ingested_at DESC")],
    )

    op.create_table(
        "comparable_sales",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_sale_id", sa.Text(), nullable=False),
        sa.Column("parcel_number", sa.Text()),
        sa.Column("sale_date", sa.Date(), nullable=False),
        sa.Column("sale_price", sa.Numeric(14, 2), nullable=False),
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column("city", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("zip", sa.Text(), nullable=False),
        sa.Column("county_name", sa.Text()),
        sa.Column("property_type", sa.Text(), nullable=False),
        sa.Column("beds", sa.Numeric(5, 1)),
        sa.Column("baths", sa.Numeric(5, 1)),
        sa.Column("sqft", sa.Numeric(12, 1)),
        sa.Column("lot_size_acres", sa.Numeric(12, 3)),
        sa.Column("year_built", sa.Integer()),
        sa.Column("latitude", sa.Numeric(9, 6)),
        sa.Column("longitude", sa.Numeric(9, 6)),
        sa.Column("arms_length", sa.Boolean(), nullable=False),
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
            "sale_price > 0",
            name="ck_comparable_sales_positive_price",
        ),
        sa.CheckConstraint(
            "sqft IS NULL OR sqft > 0",
            name="ck_comparable_sales_positive_sqft",
        ),
        sa.CheckConstraint(
            "lot_size_acres IS NULL OR lot_size_acres >= 0",
            name="ck_comparable_sales_nonnegative_lot",
        ),
        sa.CheckConstraint(
            "year_built IS NULL OR year_built BETWEEN 1700 AND 2200",
            name="ck_comparable_sales_year_built",
        ),
        sa.CheckConstraint(
            "latitude IS NULL OR latitude BETWEEN -90 AND 90",
            name="ck_comparable_sales_latitude",
        ),
        sa.CheckConstraint(
            "longitude IS NULL OR longitude BETWEEN -180 AND 180",
            name="ck_comparable_sales_longitude",
        ),
        sa.UniqueConstraint(
            "source",
            "source_sale_id",
            name="uq_comparable_sales_source_sale",
        ),
    )
    op.create_index(
        "ix_comparable_sales_zip_type_date",
        "comparable_sales",
        ["zip", "property_type", sa.text("sale_date DESC")],
    )
    op.create_index(
        "ix_comparable_sales_county_state_date",
        "comparable_sales",
        ["county_name", "state", sa.text("sale_date DESC")],
    )


def downgrade() -> None:
    op.drop_table("comparable_sales")
    op.drop_table("comparable_sales_raw")

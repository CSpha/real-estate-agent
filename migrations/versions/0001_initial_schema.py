"""Create the stabilized initial schema.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-07-18
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0001_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "listings_raw",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
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
            "source_listing_id",
            "payload_hash",
            name="uq_listings_raw_source_listing_payload",
        ),
    )
    op.create_index(
        "ix_listings_raw_source_listing_ingested",
        "listings_raw",
        ["source", "source_listing_id", sa.text("ingested_at DESC")],
    )

    op.create_table(
        "listings_current",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("address", sa.Text()),
        sa.Column("city", sa.Text()),
        sa.Column("state", sa.Text()),
        sa.Column("zip", sa.Text()),
        sa.Column("list_price", sa.Numeric(14, 2)),
        sa.Column("beds", sa.Numeric(5, 1)),
        sa.Column("baths", sa.Numeric(5, 1)),
        sa.Column("sqft", sa.Numeric(12, 1)),
        sa.Column("property_type", sa.Text()),
        sa.Column("status", sa.Text()),
        sa.Column("days_on_market", sa.Integer()),
        sa.Column("first_seen_date", sa.Date()),
        sa.Column("last_seen_date", sa.Date()),
        sa.Column("price_per_sqft", sa.Numeric(12, 2)),
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
        sa.UniqueConstraint(
            "source",
            "source_listing_id",
            name="uq_listings_current_source_listing",
        ),
    )
    op.create_index(
        "ix_listings_current_location",
        "listings_current",
        ["state", "city", "zip"],
    )
    op.create_index(
        "ix_listings_current_status_price",
        "listings_current",
        ["status", "list_price"],
    )

    op.create_table(
        "listing_history",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("address", sa.Text()),
        sa.Column("city", sa.Text()),
        sa.Column("state", sa.Text()),
        sa.Column("zip", sa.Text()),
        sa.Column("list_price", sa.Numeric(14, 2)),
        sa.Column("beds", sa.Numeric(5, 1)),
        sa.Column("baths", sa.Numeric(5, 1)),
        sa.Column("sqft", sa.Numeric(12, 1)),
        sa.Column("property_type", sa.Text()),
        sa.Column("status", sa.Text()),
        sa.Column("days_on_market", sa.Integer()),
        sa.Column("first_seen_date", sa.Date()),
        sa.Column("last_seen_date", sa.Date()),
        sa.Column("price_per_sqft", sa.Numeric(12, 2)),
        sa.Column(
            "snapshot_timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_listing_history_source_listing_snapshot",
        "listing_history",
        ["source", "source_listing_id", sa.text("snapshot_timestamp DESC")],
    )

    op.create_table(
        "alerts_sent",
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
            "sent_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "alert_type",
            "source",
            "source_listing_id",
            "event_timestamp",
            name="uq_alerts_sent_event",
        ),
    )

    op.create_table(
        "county_sales",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("county_name", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("period_date", sa.Date(), nullable=False),
        sa.Column("median_sale_price", sa.Numeric(14, 2)),
        sa.Column("homes_sold", sa.Integer()),
        sa.Column("new_listings", sa.Integer()),
        sa.Column("active_listings", sa.Integer()),
        sa.Column("median_days_on_market", sa.Integer()),
        sa.Column("source_file", sa.Text()),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "county_name",
            "state",
            "period_date",
            name="uq_county_sales_period",
        ),
    )

    op.create_table(
        "city_county_lookup",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("city", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("county_name", sa.Text(), nullable=False),
        sa.UniqueConstraint(
            "city",
            "state",
            name="uq_city_county_lookup_city_state",
        ),
    )

    op.create_table(
        "listing_market_scores",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_listing_id", sa.Text(), nullable=False),
        sa.Column("county_name", sa.Text()),
        sa.Column("address", sa.Text()),
        sa.Column("city", sa.Text()),
        sa.Column("state", sa.String(length=2)),
        sa.Column("list_price", sa.Numeric(14, 2)),
        sa.Column("county_median_sale_price", sa.Numeric(14, 2)),
        sa.Column("price_vs_county_median", sa.Numeric(14, 2)),
        sa.Column("pct_below_or_above_median", sa.Numeric(8, 4)),
        sa.Column("market_score", sa.Integer(), nullable=False),
        sa.Column("score_reason", sa.Text(), nullable=False),
        sa.Column(
            "scored_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "market_score >= 0 AND market_score <= 100",
            name="ck_listing_market_scores_range",
        ),
        sa.UniqueConstraint(
            "source",
            "source_listing_id",
            name="uq_listing_market_scores_source_listing",
        ),
    )
    op.create_index(
        "ix_listing_market_scores_score",
        "listing_market_scores",
        [sa.text("market_score DESC"), "pct_below_or_above_median"],
    )

    op.execute(
        """
        INSERT INTO city_county_lookup (city, state, county_name)
        VALUES
            ('Wooster', 'OH', 'Wayne'),
            ('Orrville', 'OH', 'Wayne'),
            ('Rittman', 'OH', 'Wayne'),
            ('Ashland', 'OH', 'Ashland'),
            ('Loudonville', 'OH', 'Ashland'),
            ('Millersburg', 'OH', 'Holmes'),
            ('Berlin', 'OH', 'Holmes'),
            ('Medina', 'OH', 'Medina'),
            ('Wadsworth', 'OH', 'Medina'),
            ('Canton', 'OH', 'Stark'),
            ('Massillon', 'OH', 'Stark'),
            ('Alliance', 'OH', 'Stark')
        ON CONFLICT (city, state)
        DO UPDATE SET county_name = EXCLUDED.county_name
        """
    )

    op.execute(
        """
        CREATE VIEW potential_deals AS
        SELECT
            source,
            source_listing_id,
            county_name,
            city,
            state,
            address,
            list_price,
            county_median_sale_price,
            price_vs_county_median,
            pct_below_or_above_median,
            market_score,
            score_reason,
            scored_at
        FROM listing_market_scores
        WHERE market_score >= 80
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS potential_deals")
    op.drop_table("listing_market_scores")
    op.drop_table("city_county_lookup")
    op.drop_table("county_sales")
    op.drop_table("alerts_sent")
    op.drop_table("listing_history")
    op.drop_table("listings_current")
    op.drop_table("listings_raw")

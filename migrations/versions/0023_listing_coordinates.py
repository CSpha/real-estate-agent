"""Store listing latitude and longitude.

Revision ID: 0023_listing_coordinates
Revises: 0022_rental_assumption_policies

Coordinates come from the provider feed (RentCast `latitude`/`longitude`) and
are used by saved-search bounding boxes. Both are nullable but must be set or
cleared together. They are not part of `listing_history`, so filling them in
does not create history snapshots.

Existing rows are filled by `python -m app.transforms.backfill_listing_coordinates`
or by the next provider sync; this revision only changes the schema.
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0023_listing_coordinates"
down_revision: str | Sequence[str] | None = "0022_rental_assumption_policies"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("listings_current", sa.Column("latitude", sa.Numeric(9, 6)))
    op.add_column("listings_current", sa.Column("longitude", sa.Numeric(9, 6)))
    op.create_check_constraint(
        "ck_listings_current_latitude",
        "listings_current",
        "latitude IS NULL OR latitude BETWEEN -90 AND 90",
    )
    op.create_check_constraint(
        "ck_listings_current_longitude",
        "listings_current",
        "longitude IS NULL OR longitude BETWEEN -180 AND 180",
    )
    op.create_check_constraint(
        "ck_listings_current_coordinates_paired",
        "listings_current",
        "(latitude IS NULL) = (longitude IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_listings_current_coordinates_paired", "listings_current", type_="check"
    )
    op.drop_constraint(
        "ck_listings_current_longitude", "listings_current", type_="check"
    )
    op.drop_constraint(
        "ck_listings_current_latitude", "listings_current", type_="check"
    )
    op.drop_column("listings_current", "longitude")
    op.drop_column("listings_current", "latitude")

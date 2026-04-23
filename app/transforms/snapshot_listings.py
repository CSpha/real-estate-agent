from sqlalchemy import text

from app.utils.db import get_engine


def snapshot_current_listings():
    snapshot_sql = text(
        """
        INSERT INTO listing_history (
            source,
            source_listing_id,
            address,
            city,
            state,
            zip,
            list_price,
            beds,
            baths,
            sqft,
            property_type,
            status,
            days_on_market,
            first_seen_date,
            last_seen_date,
            price_per_sqft
        )
        SELECT
            source,
            source_listing_id,
            address,
            city,
            state,
            zip,
            list_price,
            beds,
            baths,
            sqft,
            property_type,
            status,
            days_on_market,
            first_seen_date,
            last_seen_date,
            price_per_sqft
        FROM listings_current
        """
    )

    engine = get_engine()

    with engine.begin() as conn:
        result = conn.execute(snapshot_sql)

    print("Snapshot of listings_current written to listing_history.")


if __name__ == "__main__":
    snapshot_current_listings()
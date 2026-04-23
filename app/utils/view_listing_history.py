from sqlalchemy import text

from app.utils.db import get_engine


if __name__ == "__main__":
    engine = get_engine()

    query = text(
        """
        SELECT
            source,
            source_listing_id,
            address,
            city,
            list_price,
            status,
            days_on_market,
            snapshot_timestamp
        FROM listing_history
        ORDER BY snapshot_timestamp, source_listing_id;
        """
    )

    with engine.connect() as conn:
        results = conn.execute(query)
        for row in results:
            print(row)
        
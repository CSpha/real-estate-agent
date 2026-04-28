from sqlalchemy import text

from app.utils.db import get_engine


def detect_price_changes():
    query = text(
        """
        WITH ranked_history AS (
            SELECT
                source,
                source_listing_id,
                address,
                city,
                state,
                list_price,
                status,
                snapshot_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY source, source_listing_id
                    ORDER BY snapshot_timestamp DESC
                ) AS rn
            FROM listing_history
        ),
        latest AS (
            SELECT
                source,
                source_listing_id,
                address,
                city,
                state,
                list_price AS current_price,
                status AS current_status,
                snapshot_timestamp AS current_snapshot
            FROM ranked_history
            WHERE rn = 1
        ),
        previous AS (
            SELECT
                source,
                source_listing_id,
                list_price AS previous_price,
                status AS previous_status,
                snapshot_timestamp AS previous_snapshot
            FROM ranked_history
            WHERE rn = 2
        )
        SELECT
            l.source,
            l.source_listing_id,
            l.address,
            l.city,
            l.state,
            p.previous_price,
            l.current_price,
            (l.current_price - p.previous_price) AS price_change,
            ROUND(
                ((l.current_price - p.previous_price) / NULLIF(p.previous_price, 0)) * 100,
                2
            ) AS price_change_pct,
            p.previous_snapshot,
            l.current_snapshot
        FROM latest l
        JOIN previous p
            ON l.source = p.source
           AND l.source_listing_id = p.source_listing_id
        WHERE l.current_price IS DISTINCT FROM p.previous_price
        ORDER BY price_change ASC, l.source_listing_id;
        """
    )

    engine = get_engine()

    with engine.connect() as conn:
        results = conn.execute(query).fetchall()

    if not results:
        print("No price changes detected.")
        return

    print("Price changes detected:\n")

    for row in results:
        print(
            f"{row.address}, {row.city}, {row.state} | "
            f"{row.previous_price} -> {row.current_price} | "
            f"Change: {row.price_change} ({row.price_change_pct}%)"
        )


if __name__ == "__main__":
    detect_price_changes()
from __future__ import annotations

from sqlalchemy import Engine, text

from app.db import get_engine


def snapshot_current_listings(engine: Engine | None = None) -> int:
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
            lot_size_acres,
            property_type,
            status,
            days_on_market,
            first_seen_date,
            last_seen_date,
            price_per_sqft,
            alert_eligible,
            scoring_eligible
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
            lot_size_acres,
            property_type,
            status,
            days_on_market,
            first_seen_date,
            last_seen_date,
            price_per_sqft,
            alert_eligible,
            scoring_eligible
        FROM listings_current
        WHERE NOT EXISTS (
            SELECT 1
            FROM LATERAL (
                SELECT
                    h.address,
                    h.city,
                    h.state,
                    h.zip,
                    h.list_price,
                    h.beds,
                    h.baths,
                    h.sqft,
                    h.lot_size_acres,
                    h.property_type,
                    h.status,
                    h.days_on_market,
                    h.first_seen_date,
                    h.last_seen_date,
                    h.price_per_sqft,
                    h.alert_eligible,
                    h.scoring_eligible
                FROM listing_history h
                WHERE h.source = listings_current.source
                  AND h.source_listing_id = listings_current.source_listing_id
                ORDER BY h.snapshot_timestamp DESC, h.id DESC
                LIMIT 1
            ) latest
            WHERE ROW(
                latest.address,
                latest.city,
                latest.state,
                latest.zip,
                latest.list_price,
                latest.beds,
                latest.baths,
                latest.sqft,
                latest.lot_size_acres,
                latest.property_type,
                latest.status,
                latest.days_on_market,
                latest.first_seen_date,
                latest.last_seen_date,
                latest.price_per_sqft,
                latest.alert_eligible,
                latest.scoring_eligible
            ) IS NOT DISTINCT FROM ROW(
                listings_current.address,
                listings_current.city,
                listings_current.state,
                listings_current.zip,
                listings_current.list_price,
                listings_current.beds,
                listings_current.baths,
                listings_current.sqft,
                listings_current.lot_size_acres,
                listings_current.property_type,
                listings_current.status,
                listings_current.days_on_market,
                listings_current.first_seen_date,
                listings_current.last_seen_date,
                listings_current.price_per_sqft,
                listings_current.alert_eligible,
                listings_current.scoring_eligible
            )
        )
        """
    )

    engine = engine or get_engine()

    with engine.begin() as conn:
        result = conn.execute(snapshot_sql)

    inserted = max(result.rowcount, 0)
    print(f"Snapshot complete: {inserted} changed listing(s) added to history.")
    return inserted


if __name__ == "__main__":
    snapshot_current_listings()

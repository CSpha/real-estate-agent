from __future__ import annotations

from sqlalchemy import Engine, text

from app.db import get_engine


SCORE_LISTINGS_SQL = text(
    """
    INSERT INTO listing_market_scores (
        source,
        source_listing_id,
        county_name,
        address,
        city,
        state,
        list_price,
        county_median_sale_price,
        price_vs_county_median,
        pct_below_or_above_median,
        market_score,
        score_reason
    )
    SELECT
        lc.source,
        lc.source_listing_id,
        ccl.county_name,
        lc.address,
        lc.city,
        lc.state,
        lc.list_price,
        cs.median_sale_price,
        lc.list_price - cs.median_sale_price,
        ROUND(
            (
                (lc.list_price - cs.median_sale_price)
                / NULLIF(cs.median_sale_price, 0)
            )::numeric,
            4
        ),
        CASE
            WHEN lc.list_price <= cs.median_sale_price * 0.70 THEN 90
            WHEN lc.list_price <= cs.median_sale_price * 0.80 THEN 80
            WHEN lc.list_price <= cs.median_sale_price * 0.90 THEN 70
            WHEN lc.list_price <= cs.median_sale_price THEN 60
            WHEN lc.list_price <= cs.median_sale_price * 1.10 THEN 50
            ELSE 30
        END,
        CASE
            WHEN lc.list_price <= cs.median_sale_price * 0.70
                THEN 'Price is at least 30% below county median'
            WHEN lc.list_price <= cs.median_sale_price * 0.80
                THEN 'Price is at least 20% below county median'
            WHEN lc.list_price <= cs.median_sale_price * 0.90
                THEN 'Price is at least 10% below county median'
            WHEN lc.list_price <= cs.median_sale_price
                THEN 'Price is below county median'
            WHEN lc.list_price <= cs.median_sale_price * 1.10
                THEN 'Price is slightly above county median'
            ELSE 'Price is more than 10% above county median'
        END
    FROM listings_current lc
    JOIN city_county_lookup ccl
      ON LOWER(lc.city) = LOWER(ccl.city)
     AND UPPER(lc.state) = UPPER(ccl.state)
    JOIN county_sales cs
      ON LOWER(ccl.county_name) = LOWER(cs.county_name)
     AND UPPER(ccl.state) = UPPER(cs.state)
    WHERE lc.list_price IS NOT NULL
      AND lc.scoring_eligible
      AND cs.median_sale_price IS NOT NULL
      AND cs.period_date = (
          SELECT MAX(recent.period_date)
          FROM county_sales recent
          WHERE LOWER(recent.county_name) = LOWER(ccl.county_name)
            AND UPPER(recent.state) = UPPER(ccl.state)
      )
    ON CONFLICT (source, source_listing_id)
    DO UPDATE SET
        county_name = EXCLUDED.county_name,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        list_price = EXCLUDED.list_price,
        county_median_sale_price = EXCLUDED.county_median_sale_price,
        price_vs_county_median = EXCLUDED.price_vs_county_median,
        pct_below_or_above_median = EXCLUDED.pct_below_or_above_median,
        market_score = EXCLUDED.market_score,
        score_reason = EXCLUDED.score_reason,
        scored_at = CURRENT_TIMESTAMP
    WHERE ROW(
        listing_market_scores.county_name,
        listing_market_scores.address,
        listing_market_scores.city,
        listing_market_scores.state,
        listing_market_scores.list_price,
        listing_market_scores.county_median_sale_price,
        listing_market_scores.price_vs_county_median,
        listing_market_scores.pct_below_or_above_median,
        listing_market_scores.market_score,
        listing_market_scores.score_reason
    ) IS DISTINCT FROM ROW(
        EXCLUDED.county_name,
        EXCLUDED.address,
        EXCLUDED.city,
        EXCLUDED.state,
        EXCLUDED.list_price,
        EXCLUDED.county_median_sale_price,
        EXCLUDED.price_vs_county_median,
        EXCLUDED.pct_below_or_above_median,
        EXCLUDED.market_score,
        EXCLUDED.score_reason
    )
    """
)

DELETE_STALE_SCORES_SQL = text(
    """
    DELETE FROM listing_market_scores score
    USING listings_current listing
    WHERE score.source = listing.source
      AND score.source_listing_id = listing.source_listing_id
      AND NOT EXISTS (
          SELECT 1
          FROM city_county_lookup lookup
          JOIN county_sales sales
            ON LOWER(lookup.county_name) = LOWER(sales.county_name)
           AND UPPER(lookup.state) = UPPER(sales.state)
            WHERE LOWER(listing.city) = LOWER(lookup.city)
              AND UPPER(listing.state) = UPPER(lookup.state)
              AND listing.scoring_eligible
              AND listing.list_price IS NOT NULL
            AND sales.median_sale_price IS NOT NULL
            AND sales.period_date = (
                SELECT MAX(recent.period_date)
                FROM county_sales recent
                WHERE LOWER(recent.county_name) = LOWER(lookup.county_name)
                  AND UPPER(recent.state) = UPPER(lookup.state)
            )
      )
    """
)


def score_listings_against_market(engine: Engine | None = None) -> int:
    engine = engine or get_engine()
    with engine.begin() as conn:
        stale_result = conn.execute(DELETE_STALE_SCORES_SQL)
        result = conn.execute(SCORE_LISTINGS_SQL)

    stale_deleted = max(stale_result.rowcount, 0)
    inserted_or_changed = max(result.rowcount, 0)
    affected_rows = stale_deleted + inserted_or_changed
    print(
        "Market scoring complete: "
        f"{inserted_or_changed} listing(s) inserted or changed, "
        f"{stale_deleted} stale score(s) removed."
    )
    return affected_rows


if __name__ == "__main__":
    score_listings_against_market()

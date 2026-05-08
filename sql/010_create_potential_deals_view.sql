CREATE OR REPLACE VIEW potential_deals AS
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
WHERE market_score >= 80;
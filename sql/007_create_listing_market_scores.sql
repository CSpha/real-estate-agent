CREATE TABLE IF NOT EXISTS listing_market_scores (
    id SERIAL PRIMARY KEY,
    listing_id TEXT NOT NULL,
    county_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    price NUMERIC(12, 2),
    county_median_sale_price NUMERIC(12, 2),
    price_vs_county_median NUMERIC(12, 2),
    pct_below_or_above_median NUMERIC(8, 4),
    market_score INTEGER,
    score_reason TEXT,
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT listing_market_scores_unique_listing
        UNIQUE (listing_id)
);
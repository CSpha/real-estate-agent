import os

import psycopg2
from dotenv import load_dotenv


load_dotenv()


def get_required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realestate"),
        user=os.getenv("DB_USER", "realestate"),
        password=get_required_env("DB_PASSWORD"),
    )


def score_listings_against_market():
    sql = """
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
            cs.median_sale_price AS county_median_sale_price,
            lc.list_price - cs.median_sale_price AS price_vs_county_median,
            ROUND(
                ((lc.list_price - cs.median_sale_price) / NULLIF(cs.median_sale_price, 0))::numeric,
                4
            ) AS pct_below_or_above_median,
            CASE
                WHEN lc.list_price <= cs.median_sale_price * 0.70 THEN 90
                WHEN lc.list_price <= cs.median_sale_price * 0.80 THEN 80
                WHEN lc.list_price <= cs.median_sale_price * 0.90 THEN 70
                WHEN lc.list_price <= cs.median_sale_price THEN 60
                WHEN lc.list_price <= cs.median_sale_price * 1.10 THEN 50
                ELSE 30
            END AS market_score,
            CASE
                WHEN lc.list_price <= cs.median_sale_price * 0.70 THEN 'Price is at least 30% below county median'
                WHEN lc.list_price <= cs.median_sale_price * 0.80 THEN 'Price is at least 20% below county median'
                WHEN lc.list_price <= cs.median_sale_price * 0.90 THEN 'Price is at least 10% below county median'
                WHEN lc.list_price <= cs.median_sale_price THEN 'Price is below county median'
                WHEN lc.list_price <= cs.median_sale_price * 1.10 THEN 'Price is slightly above county median'
                ELSE 'Price is more than 10% above county median'
            END AS score_reason
        FROM listings_current lc
        JOIN city_county_lookup ccl
            ON LOWER(lc.city) = LOWER(ccl.city)
           AND LOWER(lc.state) = LOWER(ccl.state)
        JOIN county_sales cs
            ON LOWER(ccl.county_name) = LOWER(cs.county_name)
        WHERE lc.list_price IS NOT NULL
          AND cs.period_date = (
              SELECT MAX(period_date)
              FROM county_sales
              WHERE LOWER(county_name) = LOWER(ccl.county_name)
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
            scored_at = CURRENT_TIMESTAMP;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            affected_rows = cur.rowcount

    print(f"Scored {affected_rows} listings against county market data.")


if __name__ == "__main__":
    score_listings_against_market()
-- Data reconciliation only. Alembic remains authoritative for the new schema.
-- Run by app.utils.rehearse_legacy_migration inside its transaction, after
-- archiving public and applying Alembic head. Never run on a live database.
INSERT INTO public.city_county_lookup (id, city, state, county_name)
SELECT id, city, state, county_name FROM legacy_archive.city_county_lookup;

INSERT INTO public.county_sales
    (id, county_name, state, period_date, median_sale_price, homes_sold,
     new_listings, active_listings, median_days_on_market, source_file, loaded_at)
SELECT id, county_name,
       (SELECT CASE WHEN COUNT(DISTINCT state) = 1 THEN MIN(state) END
        FROM legacy_archive.city_county_lookup lookup
        WHERE lookup.county_name = sale.county_name),
       period_date, median_sale_price, homes_sold, new_listings, active_listings,
       median_days_on_market, source_file, loaded_at
FROM legacy_archive.county_sales sale;
-- An unknown or ambiguous county state fails the NOT NULL constraint.

INSERT INTO public.listings_current
    (id, source, source_listing_id, address, city, state, zip, list_price, beds,
     baths, sqft, property_type, status, days_on_market, first_seen_date,
     last_seen_date, price_per_sqft, created_at, updated_at, alert_eligible)
SELECT id, source, source_listing_id, address, city, state, zip, list_price, beds,
       baths, sqft, property_type, status, days_on_market, first_seen_date,
       last_seen_date, price_per_sqft, created_at, updated_at, false
FROM legacy_archive.listings_current;

INSERT INTO public.listing_history
    (id, source, source_listing_id, address, city, state, zip, list_price, beds,
     baths, sqft, property_type, status, days_on_market, first_seen_date,
     last_seen_date, price_per_sqft, snapshot_timestamp, alert_eligible)
SELECT id, source, source_listing_id, address, city, state, zip, list_price, beds,
       baths, sqft, property_type, status, days_on_market, first_seen_date,
       last_seen_date, price_per_sqft, snapshot_timestamp, false
FROM legacy_archive.listing_history;

INSERT INTO public.alerts_sent
    (id, alert_type, source, source_listing_id, event_timestamp, payload_json, sent_at)
SELECT id, alert_type, source, source_listing_id, event_timestamp,
       payload_json::jsonb, sent_at
FROM legacy_archive.alerts_sent;

INSERT INTO public.listing_market_scores
    (id, source, source_listing_id, county_name, address, city, state, list_price,
     county_median_sale_price, price_vs_county_median, pct_below_or_above_median,
     market_score, score_reason, scored_at)
SELECT id, source, source_listing_id, county_name, address, city, state, list_price,
       county_median_sale_price, price_vs_county_median, pct_below_or_above_median,
       market_score, score_reason, scored_at
FROM legacy_archive.listing_market_scores;

INSERT INTO public.provider_sync_state
SELECT * FROM legacy_archive.provider_sync_state;

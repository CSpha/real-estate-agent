CREATE TABLE IF NOT EXISTS county_sales (
    id SERIAL PRIMARY KEY,
    county_name TEXT NOT NULL,
    period_date DATE NOT NULL,
    median_sale_price NUMERIC(12, 2),
    homes_sold INTEGER,
    new_listings INTEGER,
    active_listings INTEGER,
    median_days_on_market INTEGER,
    source_file TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT county_sales_unique_period
        UNIQUE (county_name, period_date)
);
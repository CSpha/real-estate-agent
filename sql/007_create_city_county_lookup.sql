CREATE TABLE IF NOT EXISTS city_county_lookup (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    county_name TEXT NOT NULL,

    CONSTRAINT city_county_lookup_unique_city_state
        UNIQUE (city, state)
);
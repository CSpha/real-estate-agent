CREATE TABLE IF NOT EXISTS alerts_sent (
    id SERIAL PRIMARY KEY,
    alert_type TEXT NOT NULL,
    source TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    payload_json TEXT,
    sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (alert_type, source, source_listing_id, event_timestamp)
);
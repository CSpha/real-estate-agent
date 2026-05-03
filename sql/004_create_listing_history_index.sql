CREATE INDEX IF NOT EXISTS idx_listing_history_source_listing_snapshot
ON listing_history (source, source_listing_id, snapshot_timestamp DESC);

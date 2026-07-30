CREATE TABLE IF NOT EXISTS investment_analyses (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    evidence_json JSONB NOT NULL,
    analysis_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS investment_analyses_listing_idx
    ON investment_analyses (source, source_listing_id, created_at DESC);

CREATE TABLE IF NOT EXISTS investment_analysis_reviews (
    id SERIAL PRIMARY KEY,
    analysis_id INTEGER NOT NULL REFERENCES investment_analyses(id) ON DELETE CASCADE,
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected', 'corrected')),
    reviewer TEXT,
    notes TEXT,
    corrections_json JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS investment_analysis_reviews_analysis_idx
    ON investment_analysis_reviews (analysis_id, created_at DESC);

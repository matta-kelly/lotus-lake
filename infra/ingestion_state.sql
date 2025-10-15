-- Table to track last sync timestamps for incremental ingestion
CREATE TABLE IF NOT EXISTS ingestion_state (
    source VARCHAR(255) PRIMARY KEY,
    last_sync TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_ingestion_state_source ON ingestion_state(source);


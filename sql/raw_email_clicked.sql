CREATE NAMESPACE IF NOT EXISTS klaviyo;

CREATE TABLE IF NOT EXISTS klaviyo.email_clicked_raw (
    -- Core event identifiers
    event_id STRING,                     -- Unique Klaviyo event ID
    profile_id STRING,                   -- Klaviyo profile identifier
    email STRING,                        -- Recipient email address
    campaign_id STRING,                  -- Campaign ID (Klaviyo's $message field for campaigns)
    flow_id STRING,                      -- Flow ID (for flow emails)

    -- Timestamps
    timestamp_utc TIMESTAMP,             -- UTC event time
    datetime TIMESTAMP,                  -- Alias of timestamp_utc (used for incremental sync)
    _load_timestamp TIMESTAMP,           -- Ingestion timestamp (when flow ran)

    -- Metadata
    ingestion_date STRING                -- YYYY-MM-DD partition key
)
USING ICEBERG
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);
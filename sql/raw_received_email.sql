CREATE NAMESPACE IF NOT EXISTS klaviyo;

CREATE TABLE IF NOT EXISTS klaviyo.received_email_raw (
    -- Core event identifiers
    event_id STRING,                     -- Unique Klaviyo event ID
    profile_id STRING,                   -- Klaviyo profile identifier
    email STRING,                        -- Recipient email address
    campaign_id STRING,                  -- Campaign or flow ID
    message_id STRING,                   -- Message variation (A/B test) ID

    -- Timestamps
    timestamp_utc TIMESTAMP,             -- Official UTC event time from Klaviyo
    datetime TIMESTAMP,                  -- Alias of timestamp_utc (used for incremental sync)
    _load_timestamp TIMESTAMP,           -- Ingestion timestamp (when the flow ran)

    -- Metadata
    ingestion_date STRING                -- YYYY-MM-DD partition key
)
USING ICEBERG
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);
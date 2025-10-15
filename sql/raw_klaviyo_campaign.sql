CREATE NAMESPACE IF NOT EXISTS klaviyo;

CREATE TABLE IF NOT EXISTS klaviyo.campaign_raw (
    -- Core identifiers
    id STRING,                           -- Klaviyo campaign ID
    name STRING,                         -- Campaign name (subject or internal label)

    -- Metadata
    send_time TIMESTAMP,                 -- Scheduled or actual send time
    updated_at TIMESTAMP,                -- Last modified timestamp (used for incremental sync)
    _load_timestamp TIMESTAMP,           -- Ingestion timestamp from flow run

    -- Partitioning
    ingestion_date STRING                -- YYYY-MM-DD partition key
)
USING ICEBERG
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

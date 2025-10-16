CREATE NAMESPACE IF NOT EXISTS klaviyo;

CREATE TABLE IF NOT EXISTS klaviyo.campaign_raw (
    -- Campaign-level identifiers
    id STRING,                           -- Klaviyo campaign ID
    name STRING,                         -- Campaign name (subject or internal label)

    -- Campaign metadata
    send_time TIMESTAMP,                 -- Scheduled or actual send time
    updated_at TIMESTAMP,                -- Last modified timestamp (used for incremental sync)
    _load_timestamp TIMESTAMP,           -- Ingestion timestamp from flow run

    -- Nested messages array (STRUCT inside ARRAY)
    messages ARRAY<STRUCT<
        id: STRING,
        subject: STRING,
        preview_text: STRING,
        from_email: STRING,
        from_label: STRING,
        send_time: TIMESTAMP,
        created_at: TIMESTAMP,
        updated_at: TIMESTAMP
    >>,

    -- Partitioning
    ingestion_date STRING                -- YYYY-MM-DD partition key
)
USING ICEBERG
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_klaviyo__metrics (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id VARCHAR,
    type VARCHAR,
    links STRUCT(self VARCHAR),
    attributes STRUCT("name" VARCHAR, created TIMESTAMP WITH TIME ZONE, updated TIMESTAMP WITH TIME ZONE, integration STRUCT()),
    updated TIMESTAMP WITH TIME ZONE,
    relationships STRUCT(flow_triggers STRUCT("data" STRUCT(id VARCHAR, "type" VARCHAR), links STRUCT(self VARCHAR, related VARCHAR))),

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

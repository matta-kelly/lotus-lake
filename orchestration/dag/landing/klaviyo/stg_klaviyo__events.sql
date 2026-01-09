CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_klaviyo__events (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id VARCHAR,
    type VARCHAR,
    links STRUCT(self VARCHAR),
    datetime TIMESTAMP WITH TIME ZONE,
    attributes STRUCT(uuid VARCHAR, datetime TIMESTAMP WITH TIME ZONE, "timestamp" BIGINT, event_properties VARCHAR),
    relationships STRUCT(metric STRUCT("data" STRUCT(id VARCHAR, "type" VARCHAR), links STRUCT(self VARCHAR, related VARCHAR)), profile STRUCT("data" STRUCT(id VARCHAR, "type" VARCHAR), links STRUCT(self VARCHAR, related VARCHAR)), attributions STRUCT("data" STRUCT(id VARCHAR, "type" VARCHAR)[], links STRUCT(self VARCHAR, related VARCHAR))),

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

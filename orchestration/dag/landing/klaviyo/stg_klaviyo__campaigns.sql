CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_klaviyo__campaigns (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id VARCHAR,
    type VARCHAR,
    links STRUCT(self VARCHAR),
    attributes STRUCT("name" VARCHAR, status VARCHAR, channel VARCHAR, message VARCHAR, archived BOOLEAN, audiences STRUCT(excluded VARCHAR[], included VARCHAR[]), send_time TIMESTAMP WITH TIME ZONE, created_at TIMESTAMP WITH TIME ZONE, updated_at TIMESTAMP WITH TIME ZONE, scheduled_at TIMESTAMP WITH TIME ZONE, send_options STRUCT(use_smart_sending BOOLEAN, ignore_unsubscribes BOOLEAN), send_strategy STRUCT("method" VARCHAR, options_sto STRUCT(date DATE), options_static STRUCT(datetime TIMESTAMP WITH TIME ZONE, is_local BOOLEAN, send_past_recipients_immediately BOOLEAN), options_throttled STRUCT(datetime TIMESTAMP WITH TIME ZONE, throttle_percentage BIGINT)), tracking_options STRUCT(is_add_utm BOOLEAN, utm_params STRUCT("name" VARCHAR, "value" VARCHAR)[], is_tracking_opens BOOLEAN, is_tracking_clicks BOOLEAN, add_tracking_params BOOLEAN)),
    updated_at TIMESTAMP WITH TIME ZONE,
    relationships STRUCT(tags STRUCT("data" STRUCT(id VARCHAR, "type" VARCHAR)[], links STRUCT(self VARCHAR, related VARCHAR)), campaign_messages STRUCT("data" STRUCT(id VARCHAR, "type" VARCHAR)[], links STRUCT(self VARCHAR, related VARCHAR))),

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_shopify__customers (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id BIGINT,
    tags VARCHAR,
    email VARCHAR,
    state VARCHAR,
    last_name VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    first_name VARCHAR,
    updated_at TIMESTAMP WITH TIME ZONE,
    total_spent DOUBLE,
    orders_count BIGINT,

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

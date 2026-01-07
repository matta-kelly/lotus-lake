{{ config(tags=['core', 'klaviyo__metrics'], materialized='table') }}

select
    -- identifiers
    id as metric_id,

    -- attributes
    attributes::JSON->>'$.name' as metric_name,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at

from read_parquet('s3://landing/raw/klaviyo/metrics/**/*.parquet', hive_partitioning=true)
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

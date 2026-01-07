{{ config(tags=['core', 'klaviyo__lists'], materialized='table') }}

select
    -- identifiers
    id as list_id,

    -- attributes
    attributes::JSON->>'$.name' as list_name,
    attributes::JSON->>'$.opt_in_process' as opt_in_process,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at

from read_parquet('s3://landing/raw/klaviyo/lists/**/*', hive_partitioning=true)
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

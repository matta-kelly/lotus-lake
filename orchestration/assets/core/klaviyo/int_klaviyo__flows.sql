{{ config(tags=['core', 'klaviyo__flows'], materialized='table') }}

select
    -- identifiers
    id as flow_id,

    -- attributes
    attributes::JSON->>'$.name' as flow_name,
    attributes::JSON->>'$.status' as status,
    attributes::JSON->>'$.trigger_type' as trigger_type,
    cast(attributes::JSON->>'$.archived' as boolean) as is_archived,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at

from read_parquet('s3://landing/raw/klaviyo/flows*.parquet')
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

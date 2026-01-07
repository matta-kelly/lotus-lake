{{ config(tags=['core', 'klaviyo__campaigns'], materialized='table') }}

select
    -- identifiers
    id as campaign_id,

    -- attributes
    attributes::JSON->>'$.name' as campaign_name,
    attributes::JSON->>'$.status' as status,
    attributes::JSON->>'$.channel' as channel,
    cast(attributes::JSON->>'$.archived' as boolean) as is_archived,

    -- timestamps
    attributes::JSON->>'$.created_at' as created_at,
    attributes::JSON->>'$.updated_at' as updated_at,
    attributes::JSON->>'$.scheduled_at' as scheduled_at,
    attributes::JSON->>'$.send_time' as send_time

from read_parquet('s3://landing/raw/klaviyo/campaigns/*.parquet')
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

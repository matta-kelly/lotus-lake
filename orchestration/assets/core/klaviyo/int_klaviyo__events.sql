{{ config(tags=['core', 'klaviyo__events'], materialized='table') }}

select
    -- identifiers
    id as event_id,
    relationships::JSON->>'$.profile.data.id' as profile_id,
    relationships::JSON->>'$.metric.data.id' as metric_id,

    -- timestamps
    datetime as event_datetime,
    attributes::JSON->>'$.timestamp' as event_timestamp,

    -- event details
    attributes::JSON->>'$.uuid' as event_uuid,
    attributes::JSON->>'$.event_properties' as properties

from read_parquet('s3://landing/raw/klaviyo/events/*.parquet')
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

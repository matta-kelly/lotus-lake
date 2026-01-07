{{ config(tags=['core'], materialized='table') }}

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

from {{ source('klaviyo', 'events') }}

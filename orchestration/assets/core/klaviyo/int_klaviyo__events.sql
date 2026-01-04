{{ config(tags=['core'], materialized='table') }}

select
    -- identifiers
    id as event_id,
    json_extract_scalar(json_parse(relationships), '$.profile.data.id') as profile_id,
    json_extract_scalar(json_parse(relationships), '$.metric.data.id') as metric_id,

    -- timestamps
    datetime as event_datetime,
    json_extract_scalar(json_parse(attributes), '$.timestamp') as event_timestamp,

    -- event details
    json_extract_scalar(json_parse(attributes), '$.uuid') as event_uuid,
    cast(json_extract(json_parse(attributes), '$.event_properties') as varchar) as properties

from {{ source('klaviyo', 'events') }}

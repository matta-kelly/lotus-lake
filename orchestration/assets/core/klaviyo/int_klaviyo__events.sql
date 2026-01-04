{{ config(tags=['core']) }}

select
    -- identifiers
    id as event_id,
    relationships.profile.data.id as profile_id,
    relationships.metric.data.id as metric_id,

    -- timestamps
    datetime as event_datetime,
    attributes.timestamp as event_timestamp,

    -- event details
    attributes.uuid as event_uuid,
    attributes.event_properties as properties

from {{ source('klaviyo', 'events') }}

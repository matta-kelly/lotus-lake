{{ config(
    tags=['processed', 'klaviyo__events'],
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

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
    attributes::JSON->>'$.event_properties' as properties,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from lakehouse.staging.stg_klaviyo__events

{% if is_incremental() %}
where _airbyte_extracted_at > (select max(_airbyte_extracted_at) from {{ this }})
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

{{ config(
    tags=['core', 'klaviyo__campaigns'],
    materialized='incremental',
    unique_key='campaign_id',
    incremental_strategy='delete+insert'
) }}

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
    attributes::JSON->>'$.send_time' as send_time,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/campaigns/**/*', hive_partitioning=true)

{% if is_incremental() %}
where (year, month, day) >= (select (max(year), max(month), max(day)) from {{ this }})
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

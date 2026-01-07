{{ config(
    tags=['core', 'klaviyo__flows'],
    materialized='incremental',
    unique_key='flow_id',
    incremental_strategy='delete+insert'
) }}

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
    attributes::JSON->>'$.updated' as updated_at,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/flows/**/*', hive_partitioning=true)

{% if is_incremental() %}
where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (
    select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) from {{ this }}
)
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

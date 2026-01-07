{{ config(
    tags=['core', 'klaviyo__metrics'],
    materialized='incremental',
    unique_key='metric_id',
    incremental_strategy='delete+insert'
) }}

select
    -- identifiers
    id as metric_id,

    -- attributes
    attributes::JSON->>'$.name' as metric_name,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/metrics/**/*', hive_partitioning=true)

{% if is_incremental() %}
where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (
    select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) from {{ this }}
)
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

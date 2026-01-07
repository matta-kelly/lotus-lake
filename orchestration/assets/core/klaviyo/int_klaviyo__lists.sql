{{ config(
    tags=['core', 'klaviyo__lists'],
    materialized='incremental',
    unique_key='list_id',
    incremental_strategy='delete+insert'
) }}

select
    -- identifiers
    id as list_id,

    -- attributes
    attributes::JSON->>'$.name' as list_name,
    attributes::JSON->>'$.opt_in_process' as opt_in_process,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/lists/**/*', hive_partitioning=true)

{% if is_incremental() %}
where year * 10000 + month * 100 + day >= (
    select max(year * 10000 + month * 100 + day) from {{ this }}
)
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

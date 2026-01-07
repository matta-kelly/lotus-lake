{{ config(
    tags=['core', 'shopify__customers'],
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='delete+insert'
) }}

select
    -- identifiers
    id as customer_id,
    email,

    -- name
    first_name,
    last_name,

    -- timestamps
    created_at as customer_created_at,
    updated_at as customer_updated_at,

    -- metrics
    orders_count,
    cast(total_spent as decimal(10,2)) as total_spent,

    -- attributes
    tags,
    state as customer_state,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/shopify/customers/**/*', hive_partitioning=true)

{% if is_incremental() %}
where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (
    select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) from {{ this }}
)
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

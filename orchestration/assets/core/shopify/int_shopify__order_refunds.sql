{{ config(
    tags=['core', 'shopify__order_refunds'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

-- Dedupe then aggregate refunds per order
with deduped as (
    select
        *,
        year,
        month,
        day
    from read_parquet('s3://landing/raw/shopify/order_refunds/**/*', hive_partitioning=true)
    {% if is_incremental() %}
    where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (
        select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) from {{ this }}
    )
    {% endif %}
    qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
)

select
    order_id,
    sum(cast(t::JSON->>'$.amount' as decimal(10,2))) as returns,
    max(_airbyte_extracted_at) as _airbyte_extracted_at,
    max(year) as year,
    max(month) as month,
    max(day) as day

from deduped,
unnest(transactions) as u(t)
group by order_id

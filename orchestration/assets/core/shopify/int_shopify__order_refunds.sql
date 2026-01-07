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
    where (year, month, day) >= (select (max(year), max(month), max(day)) from {{ this }})
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

{{ config(tags=['core', 'shopify__order_refunds'], materialized='table') }}

-- Dedupe then aggregate refunds per order
with deduped as (
    select *
    from read_parquet('s3://landing/raw/shopify/order_refunds/**/*.parquet', hive_partitioning=true)
    qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
)

select
    order_id,
    sum(cast(t::JSON->>'$.amount' as decimal(10,2))) as returns

from deduped,
unnest(transactions) as u(t)
group by order_id

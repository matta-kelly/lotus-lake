{{ config(
    tags=['processed', 'shopify__order_refunds'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

with deduped as (
    select *
    from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)
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

{{ config(
    tags=['core', 'shopify__orders'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

{% if is_incremental() %}
with max_partition as (
    select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) as max_date
    from {{ this }}
)
{% endif %}

select
    -- identifiers
    id as order_id,
    cast(customer::JSON->>'$.id' as bigint) as customer_id,

    -- timestamps
    created_at as order_date,
    updated_at as order_updated_at,
    cancelled_at,

    -- status fields
    test as is_test_order,
    financial_status,
    fulfillment_status,
    cancel_reason,

    -- money fields
    cast(total_line_items_price as decimal(10,2)) as gross_sales,
    cast(total_discounts as decimal(10,2)) as discounts,
    cast(total_shipping_price_set::JSON->>'$.shop_money.amount' as decimal(10,2)) as shipping,
    cast(total_tax as decimal(10,2)) as taxes,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/shopify/orders/**/*', hive_partitioning=true)

{% if is_incremental() %}
where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (select max_date from max_partition)
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

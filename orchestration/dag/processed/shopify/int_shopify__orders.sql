{{ config(
    tags=['processed', 'shopify__orders'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    -- source file
    filename as _source_file,

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

from read_parquet('{{ var("file") }}', filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

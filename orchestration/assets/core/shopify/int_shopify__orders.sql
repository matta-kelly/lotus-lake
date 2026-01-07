{{ config(tags=['core', 'shopify__orders'], materialized='table') }}

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
    cast(total_tax as decimal(10,2)) as taxes

from {{ source('shopify', 'orders') }}
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

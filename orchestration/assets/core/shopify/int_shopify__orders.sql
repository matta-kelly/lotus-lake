{{ config(tags=['core'], materialized='table') }}

select
    -- identifiers
    id as order_id,

    -- timestamps
    created_at as order_date,
    updated_at as order_updated_at,
    cancelled_at,

    -- status fields
    test as is_test_order,
    financial_status,
    fulfillment_status,
    cancel_reason,

    -- money fields (already double, cast to decimal for precision)
    cast(total_line_items_price as decimal(10,2)) as gross_sales,
    cast(total_discounts as decimal(10,2)) as discounts,
    cast(json_extract_scalar(json_parse(total_shipping_price_set), '$.shop_money.amount') as decimal(10,2)) as shipping,
    cast(total_tax as decimal(10,2)) as taxes

from {{ source('shopify', 'orders') }}

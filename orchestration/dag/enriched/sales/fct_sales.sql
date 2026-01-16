{{ config(
    tags=['enriched'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

select
    -- identifiers
    o.order_id,

    -- timestamps
    o.order_date,
    o.cancelled_at,

    -- status (for filtering)
    o.is_test_order,
    o.financial_status,
    o.fulfillment_status,
    o.cancel_reason,

    -- metrics
    o.gross_sales,
    o.discounts,
    coalesce(r.returns, 0) as returns,
    o.gross_sales - o.discounts - coalesce(r.returns, 0) as net_sales,
    o.shipping,
    o.taxes,
    o.gross_sales - o.discounts - coalesce(r.returns, 0) + o.shipping + o.taxes as total_sales,

    -- incremental tracking
    greatest(o._airbyte_extracted_at, coalesce(r._airbyte_extracted_at, o._airbyte_extracted_at)) as _airbyte_extracted_at

from {{ ref('int_shopify__orders') }} o
left join {{ ref('int_shopify__order_refunds') }} r on o.order_id = r.order_id

{% if is_incremental() %}
where o._airbyte_extracted_at > (select max(_airbyte_extracted_at) from {{ this }})
   or r._airbyte_extracted_at > (select max(_airbyte_extracted_at) from {{ this }})
{% endif %}

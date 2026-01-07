{{ config(
    tags=['core', 'shopify__orders'],
    materialized='incremental',
    unique_key='order_line_id',
    incremental_strategy='delete+insert'
) }}

select
    -- identifiers
    line_item.id as order_line_id,
    id as order_id,
    line_item.product_id,
    line_item.variant_id,

    -- product info
    line_item.title as product_title,
    line_item.name as line_item_name,
    line_item.variant_title,
    line_item.sku,
    line_item.vendor,

    -- quantities
    line_item.quantity,
    line_item.fulfillable_quantity,
    line_item.grams as weight_grams,

    -- pricing
    cast(line_item.price as decimal(10,2)) as unit_price,
    cast(line_item.total_discount as decimal(10,2)) as total_discount,

    -- flags
    line_item.gift_card as is_gift_card,
    line_item.taxable as is_taxable,
    line_item.requires_shipping,
    line_item.product_exists,
    line_item.fulfillment_status,
    line_item.fulfillment_service,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/shopify/orders/**/*', hive_partitioning=true),
     unnest(from_json(line_items, '["JSON"]')) as t(line_item)

{% if is_incremental() %}
where year * 10000 + month * 100 + day >= (
    select max(year * 10000 + month * 100 + day) from {{ this }}
)
{% endif %}

qualify row_number() over (partition by line_item.id order by _airbyte_extracted_at desc) = 1

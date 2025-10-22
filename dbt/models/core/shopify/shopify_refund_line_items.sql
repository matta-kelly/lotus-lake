-- dbt/models/core/shopify/shopify_refund_line_items.sql

{{ config(
    materialized='incremental',
    unique_key='refund_line_item_id'
) }}

WITH deduped_refunds AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rn
    FROM {{ source('shopify', 'refunds_raw') }}
    {% if is_incremental() %}
    WHERE updatedAt > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
  )
  WHERE rn = 1
),

refund_line_items AS (
  SELECT
    r.id AS refund_id,
    r.order_id,
    r.order_name,
    r.createdAt AS created_at,
    r.updatedAt AS updated_at,
    rli.node.lineItem.id AS line_item_id,
    rli.node.quantity AS quantity,
    rli.node.lineItem.title AS title,
    rli.node.lineItem.sku AS sku,
    rli.node.lineItem.product.id AS product_id,
    rli.node.lineItem.product.productType AS product_type,
    rli.node.lineItem.variant.id AS variant_id,
    rli.node.lineItem.variant.sku AS variant_sku,
    CAST(rli.node.subtotalSet.shopMoney.amount AS DOUBLE) AS subtotal,
    CAST(rli.node.totalTaxSet.shopMoney.amount AS DOUBLE) AS tax
  FROM deduped_refunds r
  CROSS JOIN UNNEST(r.refundLineItems.edges) AS rli
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['refund_id', 'line_item_id']) }} AS refund_line_item_id,
  *
FROM refund_line_items
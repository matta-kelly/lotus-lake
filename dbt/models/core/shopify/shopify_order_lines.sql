{{ config(
    materialized='incremental',
    unique_key='line_item_id'
) }}

WITH deduped_orders AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rn
    FROM {{ source('shopify', 'orders_raw') }}
    {% if is_incremental() %}
    WHERE updatedAt > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
  )
  WHERE rn = 1
),

line_items AS (
  SELECT
    o.id AS order_id,
    o.createdAt AS created_at,
    o.updatedAt AS updated_at,
    li.node.id AS line_item_id,
    li.node.title AS title,
    li.node.sku AS sku,
    li.node.product.productType AS product_type,
    li.node.variant.price AS variant_price,
    li.node.quantity AS quantity,
    CAST(li.node.originalTotalSet.shopMoney.amount AS DOUBLE) AS original_total,
    CAST(li.node.discountedTotalSet.shopMoney.amount AS DOUBLE) AS discounted_total
  FROM deduped_orders o
  CROSS JOIN UNNEST(o.lineItems.edges) AS li
)

SELECT * FROM line_items
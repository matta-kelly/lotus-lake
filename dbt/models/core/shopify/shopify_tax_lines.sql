{{
  config(
    materialized='incremental',
    unique_key='tax_line_id'
  )
}}

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

-- Product tax lines
product_taxes AS (
  SELECT
    d.id AS order_id,
    d.updatedAt AS updated_at,
    'product' AS tax_type,
    tax.title AS title,
    CAST(tax.price AS DOUBLE) AS amount
  FROM deduped_orders d
  CROSS JOIN UNNEST(d.taxLines) AS tax
  WHERE tax.title IS NOT NULL
),

-- Shipping tax lines
shipping_taxes AS (
  SELECT
    d.id AS order_id,
    d.updatedAt AS updated_at,
    'shipping' AS tax_type,
    NULL AS title,
    CAST(ship_tax.price AS DOUBLE) AS amount
  FROM deduped_orders d
  CROSS JOIN UNNEST(d.shippingLine.taxLines) AS ship_tax
  WHERE ship_tax.price IS NOT NULL
),

combined AS (
  SELECT * FROM product_taxes
  UNION ALL
  SELECT * FROM shipping_taxes
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['order_id', 'tax_type', 'COALESCE(title, \'\')', 'CAST(amount AS VARCHAR)']) }} AS tax_line_id,
  order_id,
  updated_at,
  tax_type,
  title,
  amount
FROM combined
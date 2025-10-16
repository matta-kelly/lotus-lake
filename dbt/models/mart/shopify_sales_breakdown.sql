{{ config(
    materialized='table'
) }}

WITH discounts AS (
  SELECT
    order_id,
    SUM(discount_value) AS discounts_total
  FROM {{ ref('shopify_discounts') }}
  GROUP BY order_id
),

order_metrics AS (
  SELECT
    order_id,
    date_trunc('day', o.created_at) AS day,
    CAST(o.subtotal AS DOUBLE) AS gross_sales,
    COALESCE(d.discounts_total, 0.0) AS discounts,
    (CAST(o.subtotal AS DOUBLE) - COALESCE(d.discounts_total, 0.0)) AS net_sales,
    CAST(o.shipping_cost AS DOUBLE) AS shipping_charges,
    0.00 AS return_fees,
    CAST(o.total_tax AS DOUBLE) AS taxes,
    CAST(o.total_price AS DOUBLE) AS total_sales
  FROM {{ ref('shopify_orders') }} o
  LEFT JOIN discounts d USING (order_id)
)

SELECT
  day,
  SUM(gross_sales) AS gross_sales,
  SUM(discounts) AS discounts,
  SUM(net_sales) AS net_sales,
  SUM(shipping_charges) AS shipping_charges,
  SUM(return_fees) AS return_fees,
  SUM(taxes) AS taxes,
  SUM(total_sales) AS total_sales,
  COUNT(DISTINCT order_id) AS order_count
FROM order_metrics
GROUP BY day

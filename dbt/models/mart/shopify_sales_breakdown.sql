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
    o.created_at,
    date_trunc('hour', o.created_at) AS hour,
    date_trunc('day', o.created_at) AS day,
    date_trunc('week', o.created_at) AS week,
    date_trunc('month', o.created_at) AS month,
    date_trunc('year', o.created_at) AS year,
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
  hour,
  day,
  week,
  month,
  year,
  SUM(gross_sales) AS gross_sales,
  SUM(discounts) AS discounts,
  SUM(net_sales) AS net_sales,
  SUM(shipping_charges) AS shipping_charges,
  SUM(return_fees) AS return_fees,
  SUM(taxes) AS taxes,
  SUM(total_sales) AS total_sales,
  COUNT(DISTINCT order_id) AS order_count
FROM order_metrics
GROUP BY 1, 2, 3, 4, 5
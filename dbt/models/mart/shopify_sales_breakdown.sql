{{ config(
    materialized='table'
) }}

WITH orders_in_scope AS (
  SELECT *
  FROM {{ ref('shopify_orders') }}
  WHERE financial_status IN ('PAID', 'PARTIALLY_PAID', 'REFUNDED', 'PARTIALLY_REFUNDED')
),

gross_sales_calc AS (
  SELECT
    o.order_id,
    date_trunc('day', o.created_at AT TIME ZONE 'America/Los_Angeles') AS day,
    COALESCE(SUM(li.original_total), 0.0) AS gross_sales
  FROM orders_in_scope o
  JOIN {{ ref('shopify_order_lines') }} li ON o.order_id = li.order_id
  GROUP BY o.order_id, date_trunc('day', o.created_at AT TIME ZONE 'America/Los_Angeles')
),

order_metrics AS (
  SELECT
    gs.order_id,
    gs.day,
    gs.gross_sales,
    o.discounts_total AS discounts,
    (gs.gross_sales - o.discounts_total) AS net_sales,
    o.shipping_cost AS shipping_charges,
    0.00 AS return_fees,
    o.total_tax AS taxes,
    (gs.gross_sales - o.discounts_total + o.shipping_cost + o.total_tax) AS total_sales
  FROM gross_sales_calc gs
  JOIN orders_in_scope o ON gs.order_id = o.order_id
)

SELECT
  day,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(gross_sales) AS gross_sales,
  SUM(discounts) AS discounts,
  SUM(net_sales) AS net_sales,
  SUM(shipping_charges) AS shipping_charges,
  SUM(return_fees) AS return_fees,
  SUM(taxes) AS taxes,
  SUM(total_sales) AS total_sales
FROM order_metrics
GROUP BY day
ORDER BY day DESC
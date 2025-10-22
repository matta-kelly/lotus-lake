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

-- FIXED: Returns by transaction date (when refund was actually processed)
returns_by_day AS (
  SELECT
    date_trunc('day', r.refund_processed_at AT TIME ZONE 'America/Los_Angeles') AS day,
    COALESCE(SUM(r.total_returns), 0.0) AS returns
  FROM {{ ref('shopify_refunds') }} r
  GROUP BY date_trunc('day', r.refund_processed_at AT TIME ZONE 'America/Los_Angeles')
),

order_metrics AS (
  SELECT
    gs.order_id,
    gs.day,
    gs.gross_sales,
    o.discounts_total AS discounts,
    o.shipping_cost AS shipping_charges,
    0.00 AS return_fees,
    o.total_tax AS taxes
  FROM gross_sales_calc gs
  JOIN orders_in_scope o ON gs.order_id = o.order_id
),

daily_orders AS (
  SELECT
    day,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(gross_sales) AS gross_sales,
    SUM(discounts) AS discounts,
    SUM(shipping_charges) AS shipping_charges,
    SUM(return_fees) AS return_fees,
    SUM(taxes) AS taxes
  FROM order_metrics
  GROUP BY day
)

SELECT
  do.day,
  do.order_count,
  do.gross_sales,
  do.discounts,
  -1 * COALESCE(r.returns, 0.0) AS returns,
  (do.gross_sales - do.discounts - COALESCE(r.returns, 0.0)) AS net_sales,
  do.shipping_charges,
  do.return_fees,
  do.taxes,
  (do.gross_sales - do.discounts - COALESCE(r.returns, 0.0) + do.shipping_charges + do.taxes) AS total_sales
FROM daily_orders do
LEFT JOIN returns_by_day r ON do.day = r.day
ORDER BY do.day DESC
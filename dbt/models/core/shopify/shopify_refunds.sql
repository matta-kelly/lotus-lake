-- dbt/models/core/shopify/shopify_refunds.sql

{{ config(
    materialized='incremental',
    unique_key='refund_id'
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

refund_line_item_aggregates AS (
  SELECT
    r.id AS refund_id,
    COALESCE(SUM(CAST(rli.node.subtotalSet.shopMoney.amount AS DOUBLE)), 0.0) AS gross_returns,
    COALESCE(SUM(CAST(rli.node.totalTaxSet.shopMoney.amount AS DOUBLE)), 0.0) AS taxes_returned
  FROM deduped_refunds r
  CROSS JOIN UNNEST(r.refundLineItems.edges) AS rli
  GROUP BY r.id
),

shipping_refund_aggregates AS (
  SELECT
    r.id AS refund_id,
    COALESCE(SUM(CAST(t.node.amount AS DOUBLE)), 0.0) AS shipping_returned
  FROM deduped_refunds r
  CROSS JOIN UNNEST(r.transactions.edges) AS t
  WHERE t.node.kind = 'REFUND'
    AND t.node.status = 'SUCCESS'
  GROUP BY r.id
),

-- NEW: Get the transaction date (when refund was actually processed)
refund_transaction_dates AS (
  SELECT
    r.id AS refund_id,
    MAX(t.node.createdAt) AS refund_processed_at
  FROM deduped_refunds r
  CROSS JOIN UNNEST(r.transactions.edges) AS t
  WHERE t.node.kind = 'REFUND'
    AND t.node.status = 'SUCCESS'
  GROUP BY r.id
),

refunds AS (
  SELECT
    r.id AS refund_id,
    r.order_id,
    r.order_name,
    r.createdAt AS created_at,
    r.updatedAt AS updated_at,
    COALESCE(rtd.refund_processed_at, r.createdAt) AS refund_processed_at,  -- NEW FIELD
    CAST(r.totalRefundedSet.shopMoney.amount AS DOUBLE) AS total_refunded,
    COALESCE(rli.gross_returns, 0.0) AS gross_returns,
    COALESCE(rli.taxes_returned, 0.0) AS taxes_returned,
    COALESCE(sr.shipping_returned, 0.0) AS shipping_returned,
    0.0 AS discounts_returned,
    COALESCE(rli.gross_returns, 0.0) - 0.0 AS net_returns,
    CAST(r.totalRefundedSet.shopMoney.amount AS DOUBLE) AS total_returns
  FROM deduped_refunds r
  LEFT JOIN refund_line_item_aggregates rli ON r.id = rli.refund_id
  LEFT JOIN shipping_refund_aggregates sr ON r.id = sr.refund_id
  LEFT JOIN refund_transaction_dates rtd ON r.id = rtd.refund_id  -- NEW JOIN
)

SELECT * FROM refunds
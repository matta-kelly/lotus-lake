-- shopify_product_performance.sql

{{ config(
    materialized='table',
    schema='mart',
    alias='shopify_product_performance'
) }}

WITH order_data AS (
    -- This CTE is now simpler and more accurate.
    -- We can get the true sale date directly from the order lines table.
    SELECT
        line_item_id,
        sku,
        quantity,
        discounted_total,
        order_id,
        DATE(created_at AT TIME ZONE 'America/Los_Angeles') AS date
    FROM {{ ref('shopify_order_lines') }}
    {% if is_incremental() %}
    WHERE DATE(created_at AT TIME ZONE 'America/Los_Angeles') > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

product_daily AS (
    -- Roll up SKUs to product_id by date
    SELECT
        v.product_id,
        od.date,
        COUNT(DISTINCT od.order_id) AS orders,
        SUM(od.quantity) AS units_sold,
        SUM(od.discounted_total) AS revenue
    FROM order_data od
    INNER JOIN {{ ref('shopify_variants') }} v
        ON od.sku = v.sku
    WHERE v.product_id IS NOT NULL
    GROUP BY v.product_id, od.date
),

with_product_info AS (
    -- Add product details
    SELECT
        pd.*,
        p.title AS product_title,
        p.product_type,
        p.published_at AS release_date
    FROM product_daily pd
    LEFT JOIN {{ ref('shopify_products') }} p
        ON pd.product_id = p.product_id
)

SELECT
    product_id,
    date,
    product_title,
    product_type,
    release_date,
    orders,
    units_sold,
    revenue,
    CASE WHEN orders > 0 THEN revenue / orders ELSE 0 END AS aov,
    CASE WHEN units_sold > 0 THEN revenue / units_sold ELSE 0 END AS revenue_per_unit
FROM with_product_info
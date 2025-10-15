{{ config(
    materialized='table',
    schema='mart',
    alias='shopify_product_performance'
) }}

WITH order_data AS (
    -- Join order lines to orders to get the DATE
    SELECT
        ol.line_item_id,
        ol.sku,
        ol.quantity,
        ol.discounted_total,
        o.order_id,
        DATE(o.created_at) AS date  -- THIS is our date dimension
    FROM {{ ref('shopify_order_lines') }} ol
    INNER JOIN {{ ref('shopify_orders') }} o 
        ON ol.order_id = o.order_id
    {% if is_incremental() %}
    WHERE DATE(o.created_at) > (SELECT MAX(date) FROM {{ this }})
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
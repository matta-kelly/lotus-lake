{{ config(
    materialized='table',
    schema='mart',
    alias='tag_performance'
) }}

WITH product_perf AS (
    SELECT
        product_id,
        date,
        orders,
        units_sold,
        revenue
    FROM {{ ref('shopify_product_performance') }}
    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

product_tags AS (
    SELECT
        product_id,
        tag_name
    FROM {{ ref('shopify_product_tags') }}
),

tag_daily AS (
    SELECT
        pt.tag_name,
        pp.date,
        SUM(pp.orders) AS orders,
        SUM(pp.units_sold) AS units_sold,
        SUM(pp.revenue) AS revenue,
        COUNT(DISTINCT pp.product_id) AS num_products
    FROM product_perf pp
    INNER JOIN product_tags pt
        ON pp.product_id = pt.product_id
    GROUP BY pt.tag_name, pp.date
)

SELECT
    tag_name,
    date,
    num_products,
    orders,
    units_sold,
    revenue,
    CASE WHEN orders > 0 THEN revenue / orders ELSE 0 END AS aov,
    CASE WHEN units_sold > 0 THEN revenue / units_sold ELSE 0 END AS revenue_per_unit
FROM tag_daily
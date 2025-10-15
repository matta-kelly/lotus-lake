{{ config(
    materialized='incremental',
    unique_key='discount_id'
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

discounts AS (
  SELECT
    o.id AS order_id,
    o.updatedAt AS updated_at,
    d.node.index AS discount_index,
    d.node.allocationMethod AS allocation_method,
    d.node.targetType AS target_type,
    d.node.code AS discount_code,
    d.node.value.__typename AS value_type,
    COALESCE(CAST(d.node.value.amount AS DOUBLE), 0.0) AS discount_value,
    COALESCE(CAST(d.node.value.percentage AS DOUBLE), 0.0) AS discount_percentage
  FROM deduped_orders o
  CROSS JOIN UNNEST(o.discountApplications.edges) AS d
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['order_id', 'discount_index', 'discount_code']) }} AS discount_id,
  *
FROM discounts

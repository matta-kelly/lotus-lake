{{ config(
    materialized='incremental',
    unique_key='variant_id',
    on_schema_change='sync_all_columns'
) }}

WITH exploded AS (
  SELECT
    id AS product_id,
    v.node.id AS variant_id,
    v.node.sku,
    CAST(v.node.price AS DOUBLE) AS price,
    CAST(v.node.compareAtPrice AS DOUBLE) AS compare_at_price,
    v.node.createdAt AS created_at,
    v.node.updatedAt AS updated_at,
    pr.ingestion_date
  FROM {{ source('shopify', 'products_raw') }} pr
  CROSS JOIN UNNEST(pr.variants.edges) AS v
),

deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY variant_id ORDER BY updated_at DESC) AS rn
  FROM exploded
  {% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
  {% endif %}
)

SELECT
  variant_id,
  product_id,
  sku,
  price,
  compare_at_price,
  created_at,
  updated_at,
  ingestion_date
FROM deduped
WHERE rn = 1

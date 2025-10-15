{{ config(
    materialized='incremental',
    unique_key='product_id',
    on_schema_change='sync_all_columns'
) }}

WITH base AS (
  SELECT
    id AS product_id,
    handle,
    title,
    productType AS product_type,
    status,
    createdAt AS created_at,
    updatedAt AS updated_at,
    publishedAt AS published_at,
    ingestion_date
  FROM {{ source('shopify', 'products_raw') }}
),

deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS rn
  FROM base
  {% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
  {% endif %}
)

SELECT
  product_id,
  handle,
  title,
  product_type,
  status,
  created_at,
  updated_at,
  published_at,
  ingestion_date
FROM deduped
WHERE rn = 1

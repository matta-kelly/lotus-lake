{{ config(
    materialized='incremental',
    unique_key='tag_id',
    on_schema_change='sync_all_columns'
) }}

WITH exploded_tags AS (
  SELECT
    id AS product_id,
    updatedAt AS updated_at,
    t.tag AS tag_name,
    ingestion_date
  FROM {{ source('shopify', 'products_raw') }}
  CROSS JOIN UNNEST(tags) AS t(tag)
),

deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY product_id, tag_name ORDER BY updated_at DESC) AS rn
  FROM exploded_tags
  {% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
  {% endif %}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['product_id', 'tag_name']) }} AS tag_id,
  product_id,
  tag_name,
  updated_at,
  ingestion_date
FROM deduped
WHERE rn = 1

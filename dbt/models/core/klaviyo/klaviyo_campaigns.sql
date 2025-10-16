{{ config(
    materialized='incremental',
    unique_key='campaign_id'
) }}

WITH deduped AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM {{ source('klaviyo', 'campaign_raw') }}
    {% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
  )
  WHERE rn = 1
)

SELECT
  id AS campaign_id,
  name AS campaign_name,
  status AS campaign_status,
  send_time,
  created_at,
  updated_at,
  _load_timestamp,
  ingestion_date
FROM deduped
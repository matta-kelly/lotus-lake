{{ config(
    materialized='incremental',
    unique_key='campaign_id'
) }}

{% if is_incremental() %}
WITH max_timestamp AS (
  SELECT MAX(updated_at) as max_dt FROM {{ this }}
)
{% endif %}

, deduped AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM {{ source('klaviyo', 'campaign_raw') }}
    {% if is_incremental() %}
    CROSS JOIN max_timestamp
    WHERE updated_at > max_timestamp.max_dt
    {% endif %}
  )
  WHERE rn = 1
)

SELECT
  id AS campaign_id,
  name AS campaign_name,
  send_time,
  updated_at,
  _load_timestamp,
  ingestion_date
FROM deduped
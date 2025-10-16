{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH deduped AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY datetime DESC) AS rn
    FROM {{ source('klaviyo', 'email_open_raw') }}
    {% if is_incremental() %}
    WHERE datetime > (SELECT MAX(event_datetime) FROM {{ this }})
    {% endif %}
  )
  WHERE rn = 1
)

SELECT
  event_id,
  profile_id,
  email,
  campaign_id,
  flow_id,
  timestamp_utc AS event_timestamp,
  datetime AS event_datetime,
  _load_timestamp,
  ingestion_date
FROM deduped
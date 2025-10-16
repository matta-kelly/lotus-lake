{{ config(
    materialized='incremental',
    unique_key='message_id'
) }}

WITH deduped_campaigns AS (
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
),

messages AS (
  SELECT
    c.id AS campaign_id,
    c.updated_at AS campaign_updated_at,
    m.id AS message_id,
    m.subject,
    m.preview_text,
    m.from_email,
    m.from_label,
    m.send_time,
    m.created_at AS message_created_at,
    m.updated_at AS message_updated_at,
    c._load_timestamp,
    c.ingestion_date
  FROM deduped_campaigns c
  CROSS JOIN UNNEST(c.messages) AS m
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['campaign_id', 'message_id']) }} AS campaign_message_sk,
  *
FROM messages

{{ config(
    materialized='table',
    schema='mart',
    alias='campaign_performance'
) }}

WITH received AS (
    SELECT
        campaign_id,
        DATE(event_datetime) AS date,
        COUNT(*) AS received_count
    FROM {{ ref('received_email') }}
    {% if is_incremental() %}
    WHERE DATE(event_datetime) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY campaign_id, DATE(event_datetime)
),

opens AS (
    SELECT
        campaign_id,
        DATE(event_datetime) AS date,
        COUNT(*) AS open_count
    FROM {{ ref('email_open') }}
    {% if is_incremental() %}
    WHERE DATE(event_datetime) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY campaign_id, DATE(event_datetime)
),

clicks AS (
    SELECT
        campaign_id,
        DATE(event_datetime) AS date,
        COUNT(*) AS click_count
    FROM {{ ref('email_clicked') }}
    {% if is_incremental() %}
    WHERE DATE(event_datetime) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY campaign_id, DATE(event_datetime)
),

all_dates AS (
    -- Get all campaign-date combinations that exist
    SELECT campaign_id, date FROM received
    UNION
    SELECT campaign_id, date FROM opens
    UNION
    SELECT campaign_id, date FROM clicks
),

aggregated AS (
    SELECT
        ad.campaign_id,
        ad.date,
        COALESCE(r.received_count, 0) AS received_count,
        COALESCE(o.open_count, 0) AS open_count,
        COALESCE(cl.click_count, 0) AS click_count,
        CASE 
            WHEN COALESCE(r.received_count, 0) > 0 
            THEN ROUND(COALESCE(o.open_count, 0) * 1.0 / r.received_count, 4) 
            ELSE 0 
        END AS open_rate,
        CASE 
            WHEN COALESCE(r.received_count, 0) > 0 
            THEN ROUND(COALESCE(cl.click_count, 0) * 1.0 / r.received_count, 4) 
            ELSE 0 
        END AS click_rate
    FROM all_dates ad
    LEFT JOIN received r ON ad.campaign_id = r.campaign_id AND ad.date = r.date
    LEFT JOIN opens o ON ad.campaign_id = o.campaign_id AND ad.date = o.date
    LEFT JOIN clicks cl ON ad.campaign_id = cl.campaign_id AND ad.date = cl.date
),

with_campaign_info AS (
    SELECT
        a.*,
        c.campaign_name,
        c.send_time
    FROM aggregated a
    LEFT JOIN {{ ref('klaviyo_campaigns') }} c
        ON a.campaign_id = c.campaign_id
)

SELECT
    campaign_id,
    date,
    campaign_name,
    send_time,
    received_count,
    open_count,
    click_count,
    open_rate,
    click_rate
FROM with_campaign_info
-- campaign_performance.sql

{{ config(
    materialized='table',
    schema='mart',
    alias='campaign_performance'
) }}

WITH received AS (
    SELECT
        campaign_id,
        DATE(event_datetime AT TIME ZONE 'America/Los_Angeles') AS date,
        COUNT(DISTINCT profile_id) AS unique_received,
        COUNT(*) AS total_received
    FROM {{ ref('received_email') }}
    WHERE campaign_id IS NOT NULL  -- Exclude flow emails
    GROUP BY campaign_id, DATE(event_datetime AT TIME ZONE 'America/Los_Angeles')
),

opens AS (
    SELECT
        campaign_id,
        DATE(event_datetime AT TIME ZONE 'America/Los_Angeles') AS date,
        COUNT(DISTINCT profile_id) AS unique_opens,
        COUNT(*) AS total_opens
    FROM {{ ref('email_open') }}
    WHERE campaign_id IS NOT NULL  -- Exclude flow emails
    GROUP BY campaign_id, DATE(event_datetime AT TIME ZONE 'America/Los_Angeles')
),

clicks AS (
    SELECT
        campaign_id,
        DATE(event_datetime AT TIME ZONE 'America/Los_Angeles') AS date,
        COUNT(DISTINCT profile_id) AS unique_clicks,
        COUNT(*) AS total_clicks
    FROM {{ ref('email_clicked') }}
    WHERE campaign_id IS NOT NULL  -- Exclude flow emails
    GROUP BY campaign_id, DATE(event_datetime AT TIME ZONE 'America/Los_Angeles')
),

all_dates AS (
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
        COALESCE(r.unique_received, 0) AS unique_received,
        COALESCE(r.total_received, 0) AS total_received,
        COALESCE(o.unique_opens, 0) AS unique_opens,
        COALESCE(o.total_opens, 0) AS total_opens,
        COALESCE(cl.unique_clicks, 0) AS unique_clicks,
        COALESCE(cl.total_clicks, 0) AS total_clicks,
        -- Calculate rates based on unique users
        CASE 
            WHEN COALESCE(r.unique_received, 0) > 0 
            THEN ROUND(COALESCE(o.unique_opens, 0) * 100.0 / r.unique_received, 2) 
            ELSE 0 
        END AS open_rate,
        CASE 
            WHEN COALESCE(r.unique_received, 0) > 0 
            THEN ROUND(COALESCE(cl.unique_clicks, 0) * 100.0 / r.unique_received, 2) 
            ELSE 0 
        END AS click_rate,
        CASE 
            WHEN COALESCE(o.unique_opens, 0) > 0 
            THEN ROUND(COALESCE(cl.unique_clicks, 0) * 100.0 / o.unique_opens, 2) 
            ELSE 0 
        END AS click_to_open_rate
    FROM all_dates ad
    LEFT JOIN received r ON ad.campaign_id = r.campaign_id AND ad.date = r.date
    LEFT JOIN opens o ON ad.campaign_id = o.campaign_id AND ad.date = o.date
    LEFT JOIN clicks cl ON ad.campaign_id = cl.campaign_id AND ad.date = cl.date
)

-- Join with campaigns to get metadata
SELECT
    a.campaign_id,
    c.campaign_name,
    c.campaign_status,
    a.date,
    c.send_time,
    a.unique_received,
    a.total_received,
    a.unique_opens,
    a.total_opens,
    a.unique_clicks,
    a.total_clicks,
    a.open_rate,
    a.click_rate,
    a.click_to_open_rate
FROM aggregated a
LEFT JOIN {{ ref('klaviyo_campaigns') }} c
    ON a.campaign_id = c.campaign_id
ORDER BY a.date DESC, a.campaign_id
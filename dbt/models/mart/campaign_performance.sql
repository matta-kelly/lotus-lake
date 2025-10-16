{{ config(
    materialized='table',
    schema='mart',
    alias='campaign_performance'
) }}

WITH received AS (
    SELECT
        message_id,
        DATE(event_datetime) AS date,
        COUNT(*) AS received_count
    FROM {{ ref('received_email') }}
    {% if is_incremental() %}
    WHERE DATE(event_datetime) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY message_id, DATE(event_datetime)
),

opens AS (
    SELECT
        message_id,
        DATE(event_datetime) AS date,
        COUNT(*) AS open_count
    FROM {{ ref('email_open') }}
    {% if is_incremental() %}
    WHERE DATE(event_datetime) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY message_id, DATE(event_datetime)
),

clicks AS (
    SELECT
        message_id,
        DATE(event_datetime) AS date,
        COUNT(*) AS click_count
    FROM {{ ref('email_clicked') }}
    {% if is_incremental() %}
    WHERE DATE(event_datetime) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY message_id, DATE(event_datetime)
),

all_dates AS (
    SELECT message_id, date FROM received
    UNION
    SELECT message_id, date FROM opens
    UNION
    SELECT message_id, date FROM clicks
),

aggregated AS (
    SELECT
        ad.message_id,
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
    LEFT JOIN received r ON ad.message_id = r.message_id AND ad.date = r.date
    LEFT JOIN opens o ON ad.message_id = o.message_id AND ad.date = o.date
    LEFT JOIN clicks cl ON ad.message_id = cl.message_id AND ad.date = cl.date
),

with_message_info AS (
    SELECT
        a.*,
        m.campaign_id,
        m.subject,
        m.preview_text,
        m.from_email,
        m.from_label,
        m.send_time
    FROM aggregated a
    LEFT JOIN {{ ref('campaign_message') }} m
        ON a.message_id = m.message_id
)

SELECT
    message_id,
    campaign_id,
    date,
    subject,
    preview_text,
    from_email,
    from_label,
    send_time,
    received_count,
    open_count,
    click_count,
    open_rate,
    click_rate
FROM with_message_info

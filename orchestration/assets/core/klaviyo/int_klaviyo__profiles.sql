{{ config(tags=['core'], materialized='table') }}

select
    -- identifiers
    id as profile_id,

    -- timestamps
    json_extract_scalar(json_parse(attributes), '$.created') as created_at,
    json_extract_scalar(json_parse(attributes), '$.updated') as updated_at,

    -- contact info
    json_extract_scalar(json_parse(attributes), '$.email') as email,
    json_extract_scalar(json_parse(attributes), '$.phone_number') as phone_number,
    json_extract_scalar(json_parse(attributes), '$.first_name') as first_name,
    json_extract_scalar(json_parse(attributes), '$.last_name') as last_name,

    -- location
    json_extract_scalar(json_parse(attributes), '$.location.city') as city,
    json_extract_scalar(json_parse(attributes), '$.location.region') as region,
    json_extract_scalar(json_parse(attributes), '$.location.country') as country,
    json_extract_scalar(json_parse(attributes), '$.location.zip') as postal_code,

    -- engagement
    json_extract_scalar(json_parse(attributes), '$.last_event_date') as last_event_date,

    -- predictive analytics
    cast(json_extract_scalar(json_parse(attributes), '$.predictive_analytics.total_clv') as double) as total_clv,
    cast(json_extract_scalar(json_parse(attributes), '$.predictive_analytics.historic_clv') as double) as historic_clv,
    cast(json_extract_scalar(json_parse(attributes), '$.predictive_analytics.predicted_clv') as double) as predicted_clv,
    cast(json_extract_scalar(json_parse(attributes), '$.predictive_analytics.churn_probability') as double) as churn_probability,
    cast(json_extract_scalar(json_parse(attributes), '$.predictive_analytics.average_order_value') as double) as average_order_value,
    cast(json_extract_scalar(json_parse(attributes), '$.predictive_analytics.historic_number_of_orders') as bigint) as historic_orders

from {{ source('klaviyo', 'profiles') }}

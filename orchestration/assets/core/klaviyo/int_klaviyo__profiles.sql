{{ config(tags=['core']) }}

select
    -- identifiers
    id as profile_id,

    -- timestamps
    attributes.created as created_at,
    attributes.updated as updated_at,

    -- contact info
    attributes.email,
    attributes.phone_number,
    attributes.first_name,
    attributes.last_name,

    -- location
    attributes.location.city as city,
    attributes.location.region as region,
    attributes.location.country as country,
    attributes.location.zip as postal_code,

    -- engagement
    attributes.last_event_date,

    -- predictive analytics
    attributes.predictive_analytics.total_clv as total_clv,
    attributes.predictive_analytics.historic_clv as historic_clv,
    attributes.predictive_analytics.predicted_clv as predicted_clv,
    attributes.predictive_analytics.churn_probability as churn_probability,
    attributes.predictive_analytics.average_order_value as average_order_value,
    attributes.predictive_analytics.historic_number_of_orders as historic_orders

from {{ source('klaviyo', 'profiles') }}

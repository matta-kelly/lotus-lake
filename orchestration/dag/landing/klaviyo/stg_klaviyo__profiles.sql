CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_klaviyo__profiles (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id VARCHAR,
    type VARCHAR,
    links STRUCT(self VARCHAR),
    updated TIMESTAMP WITH TIME ZONE,
    segments VARCHAR,
    attributes STRUCT(email VARCHAR, image VARCHAR, title VARCHAR, locale VARCHAR, created TIMESTAMP WITH TIME ZONE, updated TIMESTAMP WITH TIME ZONE, "location" STRUCT(ip VARCHAR, zip VARCHAR, city VARCHAR, region VARCHAR, country VARCHAR, address1 VARCHAR, address2 VARCHAR, timezone VARCHAR), last_name VARCHAR, first_name VARCHAR, properties VARCHAR, external_id VARCHAR, anonymous_id VARCHAR, organization VARCHAR, phone_number VARCHAR, subscriptions STRUCT(sms STRUCT(marketing STRUCT("method" VARCHAR, consent VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE, last_updated TIMESTAMP WITH TIME ZONE, method_detail VARCHAR, consent_timestamp TIMESTAMP WITH TIME ZONE, can_receive_sms_marketing BOOLEAN), transactional STRUCT("method" VARCHAR, consent VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE, last_updated TIMESTAMP WITH TIME ZONE, method_detail VARCHAR, consent_timestamp TIMESTAMP WITH TIME ZONE, can_receive_sms_marketing BOOLEAN)), email STRUCT(marketing STRUCT("method" VARCHAR, consent VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE, double_optin BOOLEAN, last_updated TIMESTAMP WITH TIME ZONE, suppressions STRUCT(reason VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE)[], method_detail VARCHAR, consent_timestamp TIMESTAMP WITH TIME ZONE, list_suppressions STRUCT(reason VARCHAR, list_id VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE)[], custom_method_detail VARCHAR, can_receive_email_marketing BOOLEAN)), mobile_push STRUCT(marketing STRUCT(consent VARCHAR, consent_timestamp TIMESTAMP WITH TIME ZONE, can_receive_sms_marketing BOOLEAN))), last_event_date TIMESTAMP WITH TIME ZONE, predictive_analytics STRUCT(total_clv DOUBLE, historic_clv DOUBLE, predicted_clv DOUBLE, churn_probability DOUBLE, average_order_value DOUBLE, historic_number_of_orders BIGINT, predicted_number_of_orders DOUBLE, average_days_between_orders DOUBLE, expected_date_of_next_order VARCHAR)),
    relationships STRUCT(lists STRUCT(links STRUCT(self VARCHAR, related VARCHAR)), segments STRUCT(links STRUCT(self VARCHAR, related VARCHAR))),

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

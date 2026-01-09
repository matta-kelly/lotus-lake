CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_shopify__orders (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id BIGINT,
    test BOOLEAN,
    customer STRUCT(id BIGINT, note VARCHAR, tags VARCHAR, email VARCHAR, phone VARCHAR, state VARCHAR, currency VARCHAR, last_name VARCHAR, created_at TIMESTAMP WITH TIME ZONE, first_name VARCHAR, tax_exempt BOOLEAN, updated_at TIMESTAMP WITH TIME ZONE, total_spent DOUBLE, orders_count BIGINT, last_order_id BIGINT, tax_exemptions VARCHAR[], verified_email BOOLEAN, default_address STRUCT(id BIGINT, zip VARCHAR, city VARCHAR, "name" VARCHAR, phone VARCHAR, company VARCHAR, country VARCHAR, "default" BOOLEAN, address1 VARCHAR, address2 VARCHAR, province VARCHAR, last_name VARCHAR, first_name VARCHAR, customer_id BIGINT, country_code VARCHAR, country_name VARCHAR, province_code VARCHAR), last_order_name VARCHAR, accepts_marketing BOOLEAN, admin_graphql_api_id VARCHAR, multipass_identifier VARCHAR, sms_marketing_consent STRUCT(state VARCHAR, opt_in_level VARCHAR, consent_updated_at TIMESTAMP WITH TIME ZONE, consent_collected_from VARCHAR), marketing_opt_in_level VARCHAR, email_marketing_consent STRUCT(state VARCHAR, opt_in_level VARCHAR, consent_updated_at TIMESTAMP WITH TIME ZONE, consent_collected_from VARCHAR), accepts_marketing_updated_at VARCHAR),
    total_tax DOUBLE,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    cancel_reason VARCHAR,
    total_discounts DOUBLE,
    financial_status VARCHAR,
    fulfillment_status VARCHAR,
    total_line_items_price DOUBLE,
    total_shipping_price_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)),

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

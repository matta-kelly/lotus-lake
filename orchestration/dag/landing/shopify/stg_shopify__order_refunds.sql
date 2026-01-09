CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_shopify__order_refunds (
    -- Airbyte metadata
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns
    id BIGINT,
    order_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE,
    transactions STRUCT(id BIGINT, kind VARCHAR, test BOOLEAN, amount VARCHAR, status VARCHAR, gateway VARCHAR, message VARCHAR, receipt STRUCT(paid_amount VARCHAR), user_id BIGINT, currency VARCHAR, order_id BIGINT, device_id BIGINT, parent_id BIGINT, created_at VARCHAR, error_code VARCHAR, payment_id VARCHAR, location_id BIGINT, source_name VARCHAR, processed_at VARCHAR, "authorization" VARCHAR, payment_details STRUCT(avs_result_code VARCHAR, credit_card_bin VARCHAR, cvv_result_code VARCHAR, credit_card_name VARCHAR, buyer_action_info VARCHAR, credit_card_number VARCHAR, credit_card_wallet VARCHAR, credit_card_company VARCHAR, credit_card_expiration_year BIGINT, credit_card_expiration_month BIGINT), admin_graphql_api_id VARCHAR)[],
    refund_line_items STRUCT(id BIGINT, quantity BIGINT, subtotal DOUBLE, line_item STRUCT(id BIGINT, sku VARCHAR, "name" VARCHAR, grams BIGINT, price DOUBLE, title VARCHAR, duties STRUCT(duty_id BIGINT, amount_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)))[], vendor VARCHAR, taxable BOOLEAN, quantity BIGINT, gift_card BOOLEAN, price_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)), tax_lines STRUCT(rate DOUBLE, price DOUBLE, title VARCHAR, price_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)), channel_liable BOOLEAN)[], product_id BIGINT, properties VARCHAR[], variant_id BIGINT, pre_tax_price DOUBLE, variant_title VARCHAR, product_exists BOOLEAN, total_discount DOUBLE, pre_tax_price_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)), requires_shipping BOOLEAN, fulfillment_status VARCHAR, total_discount_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)), fulfillment_service VARCHAR, admin_graphql_api_id VARCHAR, discount_allocations STRUCT(amount DOUBLE, amount_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)), discount_application_index BIGINT)[], fulfillable_quantity BIGINT, variant_inventory_management VARCHAR), total_tax DOUBLE, location_id BIGINT, line_item_id BIGINT, restock_type VARCHAR, subtotal_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)), total_tax_set STRUCT(shop_money STRUCT(amount DOUBLE, currency_code VARCHAR), presentment_money STRUCT(amount DOUBLE, currency_code VARCHAR)))[],

    -- Hive partition columns
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);

{{ config(
    tags=['processed', 'faire__order_lines'],
    materialized='incremental',
    unique_key='line_id',
    incremental_strategy='delete+insert'
) }}

select
    -- source file
    filename as _source_file,

    -- identifiers
    id as line_id,
    order_id,
    product_id,
    variant_id,
    sku,

    -- quantities & amounts
    quantity,
    price_amount_minor,
    price_currency,

    -- line item status
    state as line_state,
    includes_tester,

    -- order context
    order_state,
    order_created_at,
    order_updated_at,

    -- retailer/customer context
    retailer_name,
    retailer_city,
    retailer_state,
    retailer_country,

    -- line timestamps
    created_at as line_created_at,
    updated_at as line_updated_at,

    -- metadata (dlt)
    _dlt_load_id,
    _dlt_id,
    year,
    month,
    day

from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _dlt_load_id desc) = 1

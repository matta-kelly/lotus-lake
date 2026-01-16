{{ config(
    tags=['processed', 'shopify__customers'],
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

select
    -- source file
    filename as _source_file,

    -- identifiers
    id as customer_id,
    email,

    -- name
    first_name,
    last_name,

    -- timestamps
    created_at as customer_created_at,
    updated_at as customer_updated_at,

    -- metrics
    orders_count,
    cast(total_spent as decimal(10,2)) as total_spent,

    -- attributes
    tags,
    state as customer_state,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

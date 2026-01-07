{{ config(tags=['core', 'shopify__customers'], materialized='table') }}

select
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
    state as customer_state

from {{ source('shopify', 'customers') }}
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

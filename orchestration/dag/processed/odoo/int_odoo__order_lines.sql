{{ config(
    tags=['processed', 'odoo__order_lines'],
    materialized='incremental',
    unique_key='line_id',
    incremental_strategy='delete+insert'
) }}

select
    -- source file
    filename as _source_file,

    -- identifiers
    id as line_id,
    order_reference,
    sku,

    -- product
    product_category,

    -- quantities & amounts
    cast(quantity as decimal(10,2)) as quantity,
    cast(unit_price as decimal(10,2)) as unit_price,
    cast(unit_cost as decimal(10,2)) as unit_cost,
    cast(subtotal as decimal(10,2)) as subtotal,

    -- sales attribution
    sales_team,
    salesperson,

    -- timestamps
    write_date as line_updated_at,
    sales_date,

    -- metadata (dlt)
    _dlt_load_id,
    _dlt_id,
    year,
    month,
    day

from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _dlt_load_id desc) = 1

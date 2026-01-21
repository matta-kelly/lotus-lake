{{ config(
    tags=['processed', 'odoo__orders'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

select
    -- source file
    filename as _source_file,

    -- identifiers
    id as order_id,
    order_reference,
    f_order_id as external_order_id,
    customer,

    -- timestamps
    create_date as order_created_at,
    write_date as order_updated_at,
    sales_date,
    order_date,
    delivery_date,

    -- status fields
    status,
    invoice_status,
    delivery_status,

    -- money fields
    cast(amount_untaxed as decimal(10,2)) as amount_untaxed,
    cast(amount_tax as decimal(10,2)) as amount_tax,
    cast(amount_total as decimal(10,2)) as amount_total,

    -- sales attribution
    sales_team,
    salesperson,
    payment_terms,
    shipping_policy,

    -- delivery location
    delivery_country,
    delivery_state,
    delivery_city,
    delivery_zip,

    -- metadata (dlt)
    _dlt_load_id,
    _dlt_id,
    year,
    month,
    day

from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _dlt_load_id desc) = 1

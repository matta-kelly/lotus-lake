{{ config(
    tags=['core', 'shopify__orders'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

{% if is_incremental() %}
  {% set partition_query %}
    select max(year) as max_year, max(month) as max_month, max(day) as max_day from {{ this }}
  {% endset %}
  {% set results = run_query(partition_query) %}
  {% set max_year = results.columns[0][0] %}
  {% set max_month = results.columns[1][0] %}
  {% set max_day = results.columns[2][0] %}
{% endif %}

select
    -- identifiers
    id as order_id,
    cast(customer::JSON->>'$.id' as bigint) as customer_id,

    -- timestamps
    created_at as order_date,
    updated_at as order_updated_at,
    cancelled_at,

    -- status fields
    test as is_test_order,
    financial_status,
    fulfillment_status,
    cancel_reason,

    -- money fields
    cast(total_line_items_price as decimal(10,2)) as gross_sales,
    cast(total_discounts as decimal(10,2)) as discounts,
    cast(total_shipping_price_set::JSON->>'$.shop_money.amount' as decimal(10,2)) as shipping,
    cast(total_tax as decimal(10,2)) as taxes,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/shopify/orders/**/*', hive_partitioning=true)

{% if is_incremental() %}
where year = '{{ max_year }}' and month = '{{ max_month }}' and day >= '{{ max_day }}'
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

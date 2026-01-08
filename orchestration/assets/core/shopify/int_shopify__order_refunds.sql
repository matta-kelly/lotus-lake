{{ config(
    tags=['core', 'shopify__order_refunds'],
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

-- Dedupe then aggregate refunds per order
with deduped as (
    select
        *,
        year,
        month,
        day
    from read_parquet('s3://landing/raw/shopify/order_refunds/**/*', hive_partitioning=true)
    {% if is_incremental() %}
    where year = '{{ max_year }}' and month = '{{ max_month }}' and day >= '{{ max_day }}'
    {% endif %}
    qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
)

select
    order_id,
    sum(cast(t::JSON->>'$.amount' as decimal(10,2))) as returns,
    max(_airbyte_extracted_at) as _airbyte_extracted_at,
    max(year) as year,
    max(month) as month,
    max(day) as day

from deduped,
unnest(transactions) as u(t)
group by order_id

order by order_id

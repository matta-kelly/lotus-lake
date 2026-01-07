{{ config(
    tags=['core', 'klaviyo__profiles'],
    materialized='incremental',
    unique_key='profile_id',
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
    id as profile_id,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at,

    -- contact info
    attributes::JSON->>'$.email' as email,
    attributes::JSON->>'$.phone_number' as phone_number,
    attributes::JSON->>'$.first_name' as first_name,
    attributes::JSON->>'$.last_name' as last_name,

    -- location
    attributes::JSON->>'$.location.city' as city,
    attributes::JSON->>'$.location.region' as region,
    attributes::JSON->>'$.location.country' as country,
    attributes::JSON->>'$.location.zip' as postal_code,

    -- engagement
    attributes::JSON->>'$.last_event_date' as last_event_date,

    -- predictive analytics
    cast(attributes::JSON->>'$.predictive_analytics.total_clv' as double) as total_clv,
    cast(attributes::JSON->>'$.predictive_analytics.historic_clv' as double) as historic_clv,
    cast(attributes::JSON->>'$.predictive_analytics.predicted_clv' as double) as predicted_clv,
    cast(attributes::JSON->>'$.predictive_analytics.churn_probability' as double) as churn_probability,
    cast(attributes::JSON->>'$.predictive_analytics.average_order_value' as double) as average_order_value,
    cast(attributes::JSON->>'$.predictive_analytics.historic_number_of_orders' as bigint) as historic_orders,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/profiles/**/*', hive_partitioning=true)

{% if is_incremental() %}
where year = '{{ max_year }}' and month = '{{ max_month }}' and day >= '{{ max_day }}'
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

{% if var('batch_size', none) %}
limit {{ var('batch_size') }}
{% endif %}

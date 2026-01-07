{{ config(
    tags=['core', 'klaviyo__events'],
    materialized='incremental',
    unique_key='event_id',
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
    id as event_id,
    relationships::JSON->>'$.profile.data.id' as profile_id,
    relationships::JSON->>'$.metric.data.id' as metric_id,

    -- timestamps
    datetime as event_datetime,
    attributes::JSON->>'$.timestamp' as event_timestamp,

    -- event details
    attributes::JSON->>'$.uuid' as event_uuid,
    attributes::JSON->>'$.event_properties' as properties,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/events/**/*', hive_partitioning=true)

{% if is_incremental() %}
where year = '{{ max_year }}' and month = '{{ max_month }}' and day >= '{{ max_day }}'
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

{% if var('batch_size', none) %}
limit {{ var('batch_size') }}
{% endif %}

{{ config(
    tags=['core', 'klaviyo__flows'],
    materialized='incremental',
    unique_key='flow_id',
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
    id as flow_id,

    -- attributes
    attributes::JSON->>'$.name' as flow_name,
    attributes::JSON->>'$.status' as status,
    attributes::JSON->>'$.trigger_type' as trigger_type,
    cast(attributes::JSON->>'$.archived' as boolean) as is_archived,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet('s3://landing/raw/klaviyo/flows/**/*', hive_partitioning=true)

{% if is_incremental() %}
where year = '{{ max_year }}' and month = '{{ max_month }}' and day >= '{{ max_day }}'
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

order by created_at

{{ config(
    tags=['processed', 'klaviyo__metrics'],
    materialized='incremental',
    unique_key='metric_id',
    incremental_strategy='merge'
) }}

select
    -- source file
    filename as _source_file,

    -- identifiers
    id as metric_id,

    -- attributes
    attributes::JSON->>'$.name' as metric_name,

    -- timestamps
    attributes::JSON->>'$.created' as created_at,
    attributes::JSON->>'$.updated' as updated_at,

    -- metadata
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

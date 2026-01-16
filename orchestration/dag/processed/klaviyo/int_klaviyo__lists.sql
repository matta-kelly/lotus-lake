{{ config(
    tags=['processed', 'klaviyo__lists'],
    materialized='incremental',
    unique_key='list_id',
    incremental_strategy='merge'
) }}

select
    -- source file
    filename as _source_file,

    -- identifiers
    id as list_id,

    -- attributes
    attributes::JSON->>'$.name' as list_name,
    attributes::JSON->>'$.opt_in_process' as opt_in_process,

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

{{ config(
    tags=['processed', 'klaviyo__lists'],
    materialized='incremental',
    unique_key='list_id',
    incremental_strategy='merge'
) }}

select
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

from lakehouse.staging.stg_klaviyo__lists

{% if is_incremental() %}
where _airbyte_extracted_at > (select max(_airbyte_extracted_at) from {{ this }})
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

order by created_at

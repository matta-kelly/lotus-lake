{{ config(
    tags=['processed', 'klaviyo__flows'],
    materialized='incremental',
    unique_key='flow_id',
    incremental_strategy='merge'
) }}

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

from lakehouse.staging.stg_klaviyo__flows

{% if is_incremental() %}
where _airbyte_extracted_at > (select max(_airbyte_extracted_at) from {{ this }})
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

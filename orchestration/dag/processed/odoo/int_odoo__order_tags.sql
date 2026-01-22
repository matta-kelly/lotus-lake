{{ config(
    tags=['processed', 'odoo__orders'],
    materialized='incremental',
    unique_key=['order_id', 'tag'],
    incremental_strategy='delete+insert'
) }}

-- Junction table: one row per (order_id, tag) pair
-- Unnests the tags JSON array from orders parquet

with source as (
    select
        id as order_id,
        tags,
        _dlt_load_id
    from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)
    where tags is not null
      and tags != '[]'
      and tags != ''
    qualify row_number() over (partition by id order by _dlt_load_id desc) = 1
),

unnested as (
    select
        order_id,
        unnest(from_json(tags, '["VARCHAR"]')) as tag,
        _dlt_load_id
    from source
)

select
    order_id,
    tag,
    _dlt_load_id
from unnested
where tag is not null and tag != ''

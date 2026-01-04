{{ config(tags=['core'], materialized='table') }}

-- Aggregate refunds per order
select
    order_id,
    sum(cast(json_extract_scalar(json_parse(t), '$.amount') as decimal(10,2))) as returns

from {{ source('shopify', 'order_refunds') }}
cross join unnest(transactions) as t(t)
group by order_id

{{ config(tags=['core']) }}

-- Aggregate refunds per order
select
    order_id,
    sum(cast(json_extract_scalar(t.value, '$.amount') as decimal(10,2))) as returns

from {{ source('shopify', 'order_refunds') }}
cross join unnest(cast(json_parse(transactions) as array(json))) as t(value)
group by order_id

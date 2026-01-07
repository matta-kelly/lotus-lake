{{ config(tags=['core'], materialized='table') }}

-- Aggregate refunds per order
select
    order_id,
    sum(cast(t::JSON->>'$.amount' as decimal(10,2))) as returns

from {{ source('shopify', 'order_refunds') }},
unnest(transactions) as u(t)
group by order_id

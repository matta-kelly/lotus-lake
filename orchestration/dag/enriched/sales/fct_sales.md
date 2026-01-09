# fct_sales

Sales metrics at order grain. Mirrors Shopify's "Total sales breakdown" report.

## Why

Track revenue across all dimensions - by day, hour, product, customer. Filter by order status to get accurate sales figures.

## Lineage

```
streams/shopify/orders.json          →  shopify.orders (Airbyte)
streams/shopify/order_refunds.json   →  shopify.order_refunds (Airbyte)
                                              ↓
                                     core/shopify/int_shopify__orders.sql
                                     core/shopify/int_shopify__refunds.sql
                                              ↓
                                     marts/fct_sales.sql
```

## Grain

One row per order.

## Status Fields (for filtering)

| Field | Values | Use |
|-------|--------|-----|
| is_test_order | true/false | Exclude test transactions |
| financial_status | paid, pending, refunded, voided, etc. | Filter by payment status |
| fulfillment_status | fulfilled, partial, null | Filter by shipping status |
| cancelled_at | timestamp or null | Exclude cancelled orders |
| cancel_reason | text or null | Understand why cancelled |

## Metrics

| Metric | Formula |
|--------|---------|
| gross_sales | Line item prices before discounts |
| discounts | Total discounts applied |
| returns | Sum of refund amounts |
| net_sales | gross_sales - discounts - returns |
| shipping | Shipping charges |
| taxes | Tax collected |
| total_sales | net_sales + shipping + taxes |

## Example Queries

Exclude test orders and cancelled orders:
```sql
select * from fct_sales
where is_test_order = false
  and cancelled_at is null
```

Only paid orders:
```sql
select * from fct_sales
where financial_status = 'paid'
```

Daily totals:
```sql
select 
    date_trunc('day', order_date) as day,
    sum(total_sales) as total_sales
from fct_sales
where is_test_order = false
group by 1
```

# fct_sales

Sales metrics at order grain. Mirrors Shopify's "Total sales breakdown" report.

## Why

Track revenue across all dimensions - by day, hour, product, customer. Filter by order status to get accurate sales figures.

## Lineage

```
S3 parquet (Airbyte)     →  processed/shopify/int_shopify__orders.sql
                             processed/shopify/int_shopify__refunds.sql
                                              ↓
                             enriched/sales/fct_sales.sql
```

## Grain

One row per order.

## Incremental Strategy

Uses `delete+insert` on `order_id` - memory efficient upsert:

| Scenario | What Happens |
|----------|--------------|
| New order | No match → inserts new row |
| Order updated (e.g., refund added) | Deletes old row → inserts updated row |

Incremental filter uses `_airbyte_extracted_at` from both upstream tables - catches new orders AND orders with new refunds.

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

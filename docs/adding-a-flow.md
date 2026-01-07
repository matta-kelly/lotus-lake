# Adding a Flow (Core + Mart Models)

How to add dbt models for a new or existing source.

## Overview

```
Airbyte sync (Incremental Append)
    ↓
S3: raw/{source}/{stream}/YYYY/MM/DD/  ← Hive-partitioned Parquet
    ↓
{source}_{stream}_sensor        ← Auto-created per stream
    ↓
Core Model                      ← YOU CREATE (dedupe + flatten)
    ↓
Mart Model (optional)           ← YOU CREATE (business logic)
```

## Core Layer: What It Does

Core models transform raw data into clean, queryable tables:

| Step | What | Why |
|------|------|-----|
| **Dedupe** | `qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1` | Airbyte Append mode keeps appending - take latest |
| **Flatten** | `attributes::JSON->>'$.email'` | Extract nested JSON into columns |
| **Type** | `cast(amount as decimal(10,2))` | Proper SQL types |
| **Rename** | `id as customer_id` | Clear, consistent names |

**Core does NOT:**
- Filter (test orders, cancelled, etc.)
- Join across entities
- Calculate metrics

## Adding a Core Model

### 1. Verify Prerequisites

```bash
# Stream config exists (triggers sensor creation)
ls orchestration/assets/streams/{source}/{stream}.json

# Source definition has the table
cat orchestration/assets/core/{source}/_{source}__sources.yml
```

### 2. Create the Model

Create `orchestration/assets/core/{source}/int_{source}__{stream}.sql`:

```sql
{{ config(tags=['core', '{source}__{stream}'], materialized='table') }}

select
    -- identifiers (rename for clarity)
    id as entity_id,

    -- flatten nested JSON
    attributes::JSON->>'$.email' as email,

    -- type cast money fields
    cast(amount as decimal(10,2)) as amount,

    -- timestamps
    created_at,
    updated_at

from {{ source('{source}', '{stream}') }}
qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
```

**Critical:** Tag must be `'{source}__{stream}'` (double underscore) to match the sensor.

### 3. Regenerate Manifest

```bash
cd orchestration && dbt parse
```

### 4. Push

```bash
git add orchestration/assets/core/{source}/
git commit -m "Add int_{source}__{stream} core model"
git push
```

## Adding a Mart Model

Marts contain business logic and join core models.

### Create the Model

Create `orchestration/assets/marts/{domain}/fct_{metric}.sql`:

```sql
{{ config(tags=['mart'], materialized='table') }}

select
    o.order_id,
    o.order_date,
    o.gross_sales,
    o.discounts,
    coalesce(r.returns, 0) as returns,
    o.gross_sales - o.discounts - coalesce(r.returns, 0) as net_sales

from {{ ref('int_shopify__orders') }} o
left join {{ ref('int_shopify__refunds') }} r on o.order_id = r.order_id
```

**Key differences from core:**
- Tag is just `['mart']` - no stream tag
- Uses `ref()` not `source()` - references core models
- Contains business logic (net_sales calculation)

**Marts auto-trigger** via `.downstream()` in the sensor - no extra config needed.

## How Sensors Work

```
shopify_orders_sensor fires
    ↓
AssetSelection.tag("shopify__orders") | .downstream()
    ↓
int_shopify__orders runs (has tag "shopify__orders")
    ↓
fct_sales runs (downstream via ref())
```

Each stream gets its own sensor. Only that stream's models run.

## Quick Reference

| What | Where | Tag |
|------|-------|-----|
| Stream config | `streams/{source}/{stream}.json` | - |
| Source definition | `core/{source}/_{source}__sources.yml` | - |
| Core model | `core/{source}/int_{source}__{stream}.sql` | `{source}__{stream}` |
| Mart model | `marts/{domain}/fct_{metric}.sql` | `mart` |

## Current Models

### Shopify
| Stream | Core Model | Mart |
|--------|-----------|------|
| orders | int_shopify__orders | fct_sales |
| customers | int_shopify__customers | - |
| order_refunds | int_shopify__refunds | (via fct_sales) |

### Klaviyo
| Stream | Core Model | Mart |
|--------|-----------|------|
| profiles | int_klaviyo__profiles | - |
| events | int_klaviyo__events | - |
| campaigns | int_klaviyo__campaigns | - |
| flows | int_klaviyo__flows | - |
| metrics | int_klaviyo__metrics | - |
| lists | int_klaviyo__lists | - |

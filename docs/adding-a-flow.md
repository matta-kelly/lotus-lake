# Adding a Flow (Core + Mart Models)

How to add dbt models for a new or existing source.

## Overview

```
Airbyte sync (Incremental Append)
    ↓
S3: raw/{source}/{stream}/year=YYYY/month=MM/day=DD/  ← Hive partition format
    ↓
{source}_{stream}_sensor        ← Auto-created per stream
    ↓
Core Model (incremental)        ← YOU CREATE (dedupe + flatten + partition filter)
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
{{ config(
    tags=['core', '{source}__{stream}'],
    materialized='incremental',
    unique_key='entity_id',
    incremental_strategy='delete+insert'
) }}

select
    -- identifiers (rename for clarity)
    id as entity_id,

    -- flatten nested JSON
    attributes::JSON->>'$.email' as email,

    -- type cast money fields
    cast(amount as decimal(10,2)) as amount,

    -- timestamps
    created_at,
    updated_at,
    _airbyte_extracted_at,

    -- partition columns (from Hive path, used for incremental filter)
    year,
    month,
    day

from read_parquet('s3://landing/raw/{source}/{stream}/**/*', hive_partitioning=true)

{% if is_incremental() %}
where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (
    select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) from {{ this }}
)
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
```

**Critical points:**
- Tag must be `'{source}__{stream}'` (double underscore) to match the sensor
- Multiple models can share the same tag (e.g., `orders` + `order_lines` both use `shopify__orders`)
- `unique_key` enables delete+insert to find existing records
- Partition columns `year`, `month`, `day` come from Hive path automatically
- `is_incremental()` filter enables partition pruning (DuckDB skips old folders)
- Use integer comparison for partitions (DuckDB doesn't support tuple comparison with aggregates)
- `qualify` dedupe still runs on both full refresh and incremental

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

## Incremental Behavior

### How It Works

| Run Type | Trigger | What Happens |
|----------|---------|--------------|
| **Full refresh** | First run, or `dbt run --full-refresh` | Reads ALL partitions, rebuilds entire table |
| **Incremental** | Sensor trigger (normal operation) | Reads recent partitions only, delete+insert changed records |

### Why delete+insert?

Airbyte appends data. If order #123 is updated in Shopify:
1. Airbyte syncs a NEW row with updated `_airbyte_extracted_at`
2. Incremental run reads this new row
3. `delete+insert` removes old order #123, inserts new version
4. `qualify` dedupe ensures only latest version remains

Simple `append` would create duplicates. `delete+insert` maintains dedupe.

### Partition Pruning

The `is_incremental()` WHERE clause:
```sql
where cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer) >= (
    select max(cast(year as integer) * 10000 + cast(month as integer) * 100 + cast(day as integer)) from {{ this }}
)
```

DuckDB sees this filter on partition columns and **skips reading older folders entirely**. This is the memory savings - we don't load historical data.

**Note:** We use integer comparison (`year * 10000 + month * 100 + day`) because DuckDB doesn't support tuple comparison with aggregate subqueries.

### When to Full Refresh

Run `dbt run --full-refresh -s int_{source}__{stream}` when:
- Adding new columns (backfill historical values)
- Fixing bugs in transformation logic
- After schema changes in source

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
left join {{ ref('int_shopify__order_refunds') }} r on o.order_id = r.order_id
```

**Key differences from core:**
- Tag is just `['mart']` - no stream tag
- Uses `ref()` not `source()` - references core models
- Contains business logic (net_sales calculation)

**Auto-materialize:** Marts automatically trigger when upstream core models materialize. This is configured via `AutoMaterializePolicy.eager()` in `orchestration/assets/marts/assets.py`.

## How Sensors Work

```
shopify_orders_sensor fires
    ↓
asset_selection=[AssetKey(["main", "int_shopify__orders"])]
    ↓
int_shopify__orders runs (ONLY this model)
```

Each stream gets its own sensor. Each sensor triggers exactly ONE core model.

For full sensor/asset wiring details, see `CLAUDE.md` → "Dagster Sensor & Asset Wiring".

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
| order_refunds | int_shopify__order_refunds | (via fct_sales) |

### Klaviyo
| Stream | Core Model | Mart |
|--------|-----------|------|
| profiles | int_klaviyo__profiles | - |
| events | int_klaviyo__events | - |
| campaigns | int_klaviyo__campaigns | - |
| flows | int_klaviyo__flows | - |
| metrics | int_klaviyo__metrics | - |
| lists | int_klaviyo__lists | - |

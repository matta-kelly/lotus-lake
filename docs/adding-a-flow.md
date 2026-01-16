# Adding a Flow (Processed + Enriched)

How to add the complete data flow for a new stream.

## Overview

```
streams → processed → enriched
```

When you add a stream, data flows through two layers:

| Layer | Folder | File Pattern | Purpose |
|-------|--------|--------------|---------|
| **Processed** | `dag/processed/{source}/` | `int_{source}__{stream}.sql` | Read parquet, dedupe, flatten JSON, type cast |
| **Enriched** | `dag/enriched/{domain}/` | `fct_*.sql` | Business logic, joins |

## Adding a Complete Flow

### Step 1: Create Stream Config

Create `orchestration/dag/streams/{source}/{stream}.json`:

```json
{}
```

This registers the stream with Airbyte and generates the feeder asset.

### Step 2: Create Processed Model

Create `orchestration/dag/processed/{source}/int_{source}__{stream}.sql`:

```sql
{{ config(
    tags=['processed', '{source}__{stream}'],
    materialized='incremental',
    unique_key='{id_column}',
    incremental_strategy='delete+insert'
) }}

select
    -- source file tracking
    filename as _source_file,

    -- identifiers (rename for clarity)
    id as {id_column},

    -- flatten nested JSON
    attributes::JSON->>'$.field' as field_name,
    cast(attributes::JSON->>'$.numeric_field' as double) as numeric_field,

    -- metadata (for incremental)
    _airbyte_extracted_at,
    year,
    month,
    day

from read_parquet({{ var("files") }}, filename=true, hive_partitioning=true)

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
```

**Critical points:**

| Element | Why |
|---------|-----|
| `tags=['processed', '{source}__{stream}']` | Feeder runs `dbt run --select tag:{source}__{stream}` |
| `incremental_strategy='delete+insert'` | Memory-efficient upsert, deletes then inserts |
| `read_parquet({{ var("files") }}, ...)` | Reads S3 parquet directly, no staging table |
| `filename=true` | Adds `filename` column for source tracking |
| `hive_partitioning=true` | Exposes year/month/day partition columns |
| `qualify row_number()` | Dedupe - take latest version of each record |

**Processed does NOT:**
- Filter data (test orders, cancelled, etc.)
- Join across entities
- Calculate business metrics

### Step 3: Create Enriched Model (Optional)

Create `orchestration/dag/enriched/{domain}/fct_{metric}.sql`:

```sql
{{ config(tags=['enriched'], materialized='table') }}

select
    o.order_id,
    o.order_date,
    o.gross_sales,
    o.discounts,
    coalesce(r.returns, 0) as returns,
    o.gross_sales - o.discounts - coalesce(r.returns, 0) as net_sales

from {{ ref('int_shopify__orders') }} o
left join {{ ref('int_shopify__order_refunds') }} r on o.order_id = r.order_id

order by order_date
```

**Key differences from processed:**
- Tag is just `['enriched']`
- Uses `ref()` to reference processed models
- Contains business logic (calculations, joins, filters)
- Auto-materializes when upstream processed models update

### Step 4: Regenerate dbt Manifest

```bash
cd orchestration && dbt parse
```

### Step 5: Push

```bash
git add orchestration/dag/
git commit -m "Add {source}/{stream} flow"
git push
```

## How It All Connects

When Airbyte syncs new data:

```
1. Airbyte writes Parquet to S3
       ↓
2. Sensor detects new files (checks cursor)
       ↓
3. Feeder asset runs:
   a. get_files_after_cursor() → partition-aware discovery
   b. batch files (10 per dbt run)
   c. dbt run --select tag:{source}__{stream} --vars '{"files": [...]}'
   d. set_cursor() → update progress
       ↓
4. dbt model reads parquet directly, merges into DuckLake
       ↓
5. Enriched auto-materializes (via automation sensor)
```

## Incremental Behavior

| Run Type | What Happens |
|----------|--------------|
| **First run** | Full table scan, create table |
| **Incremental** | Only new files (cursor-based) |
| **Full refresh** | `dbt run --full-refresh -s model_name` rebuilds from scratch |

### When to Full Refresh

- Adding new columns (backfill historical values)
- Fixing bugs in transformation logic
- After schema changes in source

## The Delete+Insert Strategy

Processed models use `incremental_strategy='delete+insert'`:

1. Rows matching `unique_key` are deleted
2. New rows are inserted

This uses less memory than `merge` because DuckDB doesn't need to hold both datasets for comparison.

## Quick Reference

| What | Where | Tag |
|------|-------|-----|
| Stream config | `dag/streams/{source}/{stream}.json` | - |
| Processed model | `dag/processed/{source}/int_{source}__{stream}.sql` | `{source}__{stream}` |
| Enriched model | `dag/enriched/{domain}/fct_{metric}.sql` | `enriched` |

## Current Models

### Shopify

| Stream | Processed | Enriched |
|--------|-----------|----------|
| orders | int_shopify__orders | fct_sales |
| customers | int_shopify__customers | - |
| order_refunds | int_shopify__order_refunds | (via fct_sales) |

### Klaviyo

| Stream | Processed | Enriched |
|--------|-----------|----------|
| profiles | int_klaviyo__profiles | - |
| events | int_klaviyo__events | - |
| campaigns | int_klaviyo__campaigns | - |
| flows | int_klaviyo__flows | - |
| metrics | int_klaviyo__metrics | - |
| lists | int_klaviyo__lists | - |

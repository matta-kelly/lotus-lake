# Adding a Flow (Landing + Processed + Enriched)

How to add the complete data flow for a new stream.

## Overview

```
sources → streams → landing → processed → enriched
```

When you add a stream, data flows through three layers:

| Layer | Folder | File Pattern | Purpose |
|-------|--------|--------------|---------|
| **Landing** | `dag/landing/{source}/` | `stg_{source}__{stream}.sql` | DDL defining raw schema |
| **Processed** | `dag/processed/{source}/` | `int_{source}__{stream}.sql` | Dedupe, flatten JSON, type cast |
| **Enriched** | `dag/enriched/{domain}/` | `fct_*.sql` | Business logic, joins |

## Adding a Complete Flow

### Step 1: Create Landing DDL

Create `orchestration/dag/landing/{source}/stg_{source}__{stream}.sql`:

```sql
CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_shopify__orders (
    -- Airbyte metadata (always include these)
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    _airbyte_meta STRUCT(sync_id BIGINT, changes STRUCT(field VARCHAR, change VARCHAR, reason VARCHAR)[]),
    _airbyte_generation_id BIGINT,

    -- Source columns (from Airbyte schema)
    id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    customer STRUCT(id BIGINT, email VARCHAR, ...),
    total_price DOUBLE,
    ...

    -- Hive partition columns (always include these)
    day VARCHAR,
    month VARCHAR,
    year BIGINT
);
```

**Tips:**
- Reference `dag/sources/{source}/{stream}.json` for available fields and types
- Use STRUCT for nested objects
- Include all Airbyte metadata columns
- Include Hive partition columns (year, month, day)

### Step 2: Create Processed Model

Create `orchestration/dag/processed/{source}/int_{source}__{stream}.sql`:

```sql
{{ config(
    tags=['processed', '{source}__{stream}'],
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    -- identifiers (rename for clarity)
    id as order_id,

    -- flatten nested JSON
    cast(customer::JSON->>'$.id' as bigint) as customer_id,
    customer::JSON->>'$.email' as customer_email,

    -- type cast
    cast(total_price as decimal(10,2)) as total_price,

    -- timestamps
    created_at,
    updated_at,

    -- metadata (for incremental)
    _airbyte_extracted_at,
    year,
    month,
    day

from lakehouse.staging.stg_{source}__{stream}

{% if is_incremental() %}
where _airbyte_extracted_at > (select max(_airbyte_extracted_at) from {{ this }})
{% endif %}

qualify row_number() over (partition by id order by _airbyte_extracted_at desc) = 1

order by created_at
```

**Critical points:**

| Element | Why |
|---------|-----|
| `tags=['processed', '{source}__{stream}']` | Feeder runs `dbt run --select tag:{source}__{stream}` |
| `incremental_strategy='merge'` | DuckLake native upsert, efficient for updates |
| `from lakehouse.staging.stg_*` | Reads from DuckLake landing table, not raw S3 |
| `_airbyte_extracted_at` filter | Only process new records on incremental runs |
| `qualify row_number()` | Dedupe - take latest version of each record |
| `order by` | Enables DuckDB zonemap optimization |

**Processed does NOT:**
- Filter data (test orders, cancelled, etc.)
- Join across entities
- Calculate business metrics

### Step 3: Add to dbt Sources (if new source)

Create/update `orchestration/dag/processed/{source}/_{source}__sources.yml`:

```yaml
version: 2

sources:
  - name: {source}
    schema: staging
    database: lakehouse
    tables:
      - name: stg_{source}__{stream}
```

### Step 4: Create Enriched Model (Optional)

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

### Step 5: Regenerate dbt Manifest

```bash
cd orchestration && dbt parse
```

### Step 6: Push

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
   b. batch_by_size() → ~500MB chunks
   c. register_batch() → ducklake_add_data_files()
   d. dbt run --select tag:{source}__{stream}
   e. set_cursor() → update progress
   f. cleanup_old_files() → delete old S3 files
       ↓
4. Processed model merges new data into DuckLake
       ↓
5. Enriched auto-materializes (via automation sensor)
```

## Incremental Behavior

| Run Type | What Happens |
|----------|--------------|
| **First run** | Full table scan, create table |
| **Incremental** | Only rows where `_airbyte_extracted_at > max(existing)` |
| **Full refresh** | `dbt run --full-refresh -s model_name` rebuilds from scratch |

### When to Full Refresh

- Adding new columns (backfill historical values)
- Fixing bugs in transformation logic
- After schema changes in source

## The Merge Strategy

Processed models use `incremental_strategy='merge'` (DuckLake native):

1. New rows are inserted
2. Existing rows (by `unique_key`) are updated
3. No need to delete first

This is more efficient than `delete+insert` for DuckLake tables.

## Quick Reference

| What | Where | Tag |
|------|-------|-----|
| Landing DDL | `dag/landing/{source}/stg_{source}__{stream}.sql` | - |
| Processed model | `dag/processed/{source}/int_{source}__{stream}.sql` | `{source}__{stream}` |
| Enriched model | `dag/enriched/{domain}/fct_{metric}.sql` | `enriched` |

## Current Models

### Shopify

| Stream | Landing | Processed | Enriched |
|--------|---------|-----------|----------|
| orders | stg_shopify__orders | int_shopify__orders | fct_sales |
| customers | stg_shopify__customers | int_shopify__customers | - |
| order_refunds | stg_shopify__order_refunds | int_shopify__order_refunds | (via fct_sales) |

### Klaviyo

| Stream | Landing | Processed | Enriched |
|--------|---------|-----------|----------|
| profiles | stg_klaviyo__profiles | int_klaviyo__profiles | - |
| events | stg_klaviyo__events | int_klaviyo__events | - |
| campaigns | stg_klaviyo__campaigns | int_klaviyo__campaigns | - |
| flows | stg_klaviyo__flows | int_klaviyo__flows | - |
| metrics | stg_klaviyo__metrics | int_klaviyo__metrics | - |
| lists | stg_klaviyo__lists | int_klaviyo__lists | - |

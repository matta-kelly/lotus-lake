# Architecture

## Stack

| Layer | Tool | Why |
|-------|------|-----|
| Ingestion | Airbyte | Built-in Shopify/Klaviyo connectors, incremental sync |
| Storage | MinIO → Iceberg → Nessie | S3-compatible, ACID tables, git-like catalog |
| Transform | dbt-trino | SQL transforms with testing and lineage |
| Orchestration | Dagster | Asset-based, sensors for Airbyte completion |
| Query | Trino | Distributed SQL, native Iceberg support |
| Semantic | Cube.js | Metrics, caching, API generation |

## Data Flow

```
Sources (Shopify, Klaviyo)
    ↓
Airbyte (append_dedup to Iceberg)
    ↓
Bronze (raw tables in source schemas)
    ↓
Streams (stg_* views - rename, cast)
    ↓
Core (int_* incremental - flatten, filter, join)
    ↓
Marts (fct_*, dim_* tables - aggregations)
    ↓
Cube.js → API/Dashboards
```

## Project Organization

SQL models and dagster Python assets live together per source:

```
orchestration/assets/
├── streams/shopify/
│   ├── orders.json              # Schema reference
│   ├── stg_shopify__orders.sql  # Staging model
│   └── _shopify__sources.yml    # dbt source definition
├── core/shopify/
│   ├── int_shopify__orders.sql  # Core model
│   └── shopify.py               # Dagster asset (@dbt_assets)
└── marts/
    └── fct_daily_sales.sql
```

## Key Decisions

**Iceberg over raw Parquet** - ACID transactions, schema evolution, time travel.

**Nessie over Glue/Hive** - Git-like versioning, self-hosted, multi-table transactions.

**Trino over DuckDB** - DuckDB can't write Iceberg yet. Revisit when it can.

**Dagster over Airflow** - Asset-based model fits dbt. Sensors react to Airbyte syncs.

**Combined orchestration/** - dbt and dagster configs together reduces context switching.

## Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| Staging | `stg_<source>__<entity>` | `stg_shopify__orders` |
| Core | `int_<source>__<entity>` | `int_shopify__order_lines` |
| Mart | `fct_<domain>`, `dim_<entity>` | `fct_daily_sales`, `dim_products` |

## Dev Resources

| Component | CPU | Memory |
|-----------|-----|--------|
| Airbyte Server | 250m-1 | 512Mi-2Gi |
| Airbyte Worker | 500m-2 | 1-4Gi |
| Trino | 1 | 10-12Gi |

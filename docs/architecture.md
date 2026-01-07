# Architecture

## Stack

| Layer | Tool | Why |
|-------|------|-----|
| Ingestion | Airbyte | Built-in Shopify/Klaviyo connectors, incremental sync |
| Storage | SeaweedFS (S3) + Parquet | S3-compatible object storage, columnar format |
| Catalog | DuckLake + Postgres (CNPG) | SQL-based metadata catalog, ACID, time travel |
| Transform | dbt-duckdb | SQL transforms with DuckLake integration |
| Orchestration | Dagster | Asset-based, sensors for Airbyte completion |
| Query | DuckDB | Embedded OLAP, reads Parquet from S3 via DuckLake |

## Data Flow

```
Sources (Shopify, Klaviyo)
    ↓
Airbyte (append to S3 Parquet)
    ↓
SeaweedFS: s3://landing/raw/{namespace}/{stream}/
    ↓
DuckLake (Postgres catalog tracks files)
    ↓
dbt-duckdb transforms (core, marts)
    ↓
Dagster orchestration
```

## DuckLake Setup

DuckLake uses Postgres as the metadata catalog and SeaweedFS for Parquet storage:

```sql
-- Attach DuckLake with Postgres catalog and S3 storage
ATTACH 'ducklake:postgres:' AS lakehouse (DATA_PATH 's3://landing/raw/');
```

**Components:**
- **Postgres (CNPG)**: `ducklake-db` cluster in h-kube stores table metadata
- **SeaweedFS**: Stores actual Parquet files at `s3://landing/raw/{namespace}/{stream}/`
- **DuckDB**: Queries catalog, reads Parquet directly from S3

## Project Organization

SQL models and dagster Python assets live together per source:

```
orchestration/
├── airbyte/terraform/       # Airbyte sources, destinations, connections
├── assets/
│   ├── streams/shopify/
│   │   ├── orders.json      # Stream config (fields to sync)
│   │   └── _catalog.json    # Generated catalog for Airbyte
│   ├── core/shopify/
│   │   ├── int_shopify__orders.sql
│   │   ├── _shopify__sources.yml
│   │   └── assets.py        # Dagster assets
│   └── marts/
│       └── fct_daily_sales.sql
├── profiles.yml             # dbt-duckdb connection config
└── dbt_project.yml
```

## Key Decisions

**DuckLake over Iceberg/Nessie** - Simpler SQL-based catalog using Postgres. No complex file-based metadata. Same features: ACID, time travel, schema evolution.

**Postgres (CNPG) for catalog** - Managed by h-kube, already running for other services. Low overhead.

**SeaweedFS over MinIO** - Already deployed in h-kube, S3-compatible, distributed.

**DuckDB over Trino** - Embedded, no cluster to manage. DuckLake enables full lakehouse features.

**Dagster over Airflow** - Asset-based model fits dbt. Sensors react to Airbyte syncs.

## Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| Raw (Airbyte) | `{namespace}/{stream}` | `shopify/orders` |
| Core | `int_<source>__<entity>` | `int_shopify__orders` |
| Mart | `fct_<domain>`, `dim_<entity>` | `fct_daily_sales`, `dim_customers` |

## Infrastructure (h-kube)

| Component | Namespace | Purpose |
|-----------|-----------|---------|
| SeaweedFS | `seaweedfs` | S3-compatible storage for Parquet |
| CNPG `ducklake-db` | `lotus-lake` | DuckLake metadata catalog |
| Airbyte | `airbyte` | Data ingestion |
| tofu-controller | `flux-system` | GitOps for Terraform |

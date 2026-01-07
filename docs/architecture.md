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
SeaweedFS: s3://landing/raw/{namespace}/{stream}/year=YYYY/month=MM/day=DD/
    ↓                                          └── Hive partition format
DuckDB read_parquet() with hive_partitioning=true
    ↓                  └── Auto-detects year/month/day columns, enables partition pruning
dbt-duckdb transforms (core models = incremental, marts = table)
    ↓
DuckLake tables (Postgres catalog)
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
- **SeaweedFS**: Stores Parquet files at `s3://landing/raw/{namespace}/{stream}/year=YYYY/month=MM/day=DD/`
- **DuckDB**: Queries catalog, reads Parquet directly from S3 with partition pruning

## Partition Pruning & Incremental Models

**Why Hive format matters:**

Airbyte writes Parquet with Hive-style partitions: `year=2026/month=01/day=07/`

When dbt reads with `hive_partitioning=true`, DuckDB:
1. Auto-detects `year`, `month`, `day` as partition columns
2. Skips entire folders that don't match WHERE filters
3. Only reads data from relevant partitions

**Incremental model behavior:**

| Run Type | What Happens |
|----------|--------------|
| Full refresh | Reads ALL partitions, rebuilds entire table |
| Incremental | Reads only recent partitions (e.g., last 2 days), delete+insert changed records |

This reduces memory usage from "all historical data" to "just recent data" per run.

## Project Organization

SQL models and dagster Python assets live together per source:

```
orchestration/
├── airbyte/terraform/       # Airbyte sources, destinations, connections
├── assets/
│   ├── streams/shopify/
│   │   ├── orders.json      # Stream config (fields to sync)
│   │   └── _catalog.json    # Generated catalog for Airbyte
│   ├── core/
│   │   ├── assets.py        # Dagster asset factory (auto-generates per stream)
│   │   └── shopify/
│   │       ├── int_shopify__orders.sql
│   │       └── _shopify__sources.yml
│   └── marts/
│       └── sales/
│           └── fct_sales.sql
├── sensors.py               # S3 sensors (auto-generated per stream)
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

## Dagster Architecture

Dagster runs as a Helm release in lotus-lake namespace with three components:

```
┌─────────────────────────────────────────────────────────────┐
│                    Dagster Helm Release                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐    ┌──────────────────┐               │
│  │ dagster-webserver│    │  dagster-daemon  │               │
│  │ (Dagster image)  │    │ (Dagster image)  │               │
│  │                  │    │                  │               │
│  │ - UI             │    │ - Schedules      │               │
│  │ - GraphQL API    │    │ - Sensors        │               │
│  └────────┬─────────┘    │ - Run queue      │               │
│           │              └────────┬─────────┘               │
│           │    gRPC               │                          │
│           └───────────┬───────────┘                          │
│                       ▼                                      │
│  ┌─────────────────────────────────────────┐                │
│  │     dagster-user-deployments            │                │
│  │     (lotus-lake image)                  │                │
│  │                                         │                │
│  │  - orchestration/definitions.py         │                │
│  │  - dbt models + profiles.yml            │                │
│  │  - Python assets                        │                │
│  │                                         │                │
│  │  Runs gRPC server on port 3030          │                │
│  └─────────────────────────────────────────┘                │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Key points:**
- Webserver and daemon use Dagster's official images (generic infrastructure)
- User deployment uses `ghcr.io/lotusandluna/lotus-lake` (your code)
- User deployment container runs gRPC server that webserver/daemon call to discover assets, trigger runs
- Logs stored in SeaweedFS via S3ComputeLogManager

## Infrastructure (h-kube)

| Component | Namespace | Purpose |
|-----------|-----------|---------|
| SeaweedFS | `seaweedfs` | S3-compatible storage for Parquet + logs |
| CNPG `ducklake-db` | `lotus-lake` | DuckLake metadata catalog |
| CNPG `dagster-db` | `lotus-lake` | Dagster run/event storage |
| Airbyte | `airbyte` | Data ingestion |
| tofu-controller | `flux-system` | GitOps for Terraform |

# Architecture

## Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | Airbyte | Incremental sync from Shopify, Klaviyo → S3 Parquet |
| Storage | SeaweedFS (S3) | S3-compatible object storage for raw Parquet files |
| Catalog | DuckLake + Postgres | SQL-based metadata catalog with ACID, time travel |
| Transform | dbt-duckdb | SQL transforms reading from DuckLake tables |
| Orchestration | Dagster | Asset-based orchestration with sensors |
| Semantic | Cube.js | PostgreSQL protocol + REST API for BI tools |
| Query | DuckDB | Embedded OLAP, queries DuckLake tables |

## Data Flow

```
sources → streams → landing → processed → enriched
```

```
Sources (Shopify, Klaviyo)
    ↓
Airbyte (incremental append to S3)
    ↓
S3: raw/{source}/{stream}/year=YYYY/month=MM/day=DD/*.parquet
    ↓                      └── Hive partition format
Sensor detects new files
    ↓
Feeder asset:
    ├── 1. Get files after cursor (partition-aware)
    ├── 2. Batch by size (~500MB)
    ├── 3. Register batch → ducklake_add_data_files()
    ├── 4. Run processed dbt model (merge into DuckLake)
    ├── 5. Update cursor
    └── 6. Cleanup old S3 files (trails cursor by 7 days)
    ↓
DuckLake landing table (stg_shopify__orders)
    ↓
DuckLake processed table (int_shopify__orders)
    ↓
Enriched auto-materializes (fct_sales)
```

## The Duck Feeder Pattern

The feeder solves OOM issues from large Airbyte backfills by processing files incrementally.

### Key Concepts

| Concept | What | Why |
|---------|------|-----|
| **Cursor** | Last processed S3 file path | Resume from where we left off |
| **Partition-aware discovery** | Only glob from cursor date forward | Avoid scanning all historical partitions |
| **Batch by size** | Group files into ~500MB chunks | Stay within memory limits |
| **Register, don't copy** | `ducklake_add_data_files()` adds metadata only | Files stay in S3, no duplication |
| **Trailing cleanup** | Delete S3 files older than cursor - 7 days | Prevent unbounded storage growth |

### Processing Loop

```
┌─────────────────────────────────────────────────────────────┐
│                     FEEDER ASSET                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  cursor = get_cursor(source, stream)                        │
│       ↓                                                     │
│  files = get_files_after_cursor()  ← partition-aware        │
│       ↓                                                     │
│  for batch in batch_by_size(files, 500MB):                  │
│       │                                                     │
│       ├── register_batch(landing_table, batch)              │
│       │        └── ducklake_add_data_files() for each file  │
│       │                                                     │
│       ├── dbt run --select tag:{source}__{stream}           │
│       │        └── merge into processed table               │
│       │                                                     │
│       └── set_cursor(last_file_in_batch)                    │
│                                                             │
│  cleanup_old_files(buffer_days=7)                           │
│       └── delete S3 files older than cursor - 7 days        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Cursor Table

Stored in DuckLake at `lakehouse.meta.feeder_cursors`:

```sql
CREATE TABLE lakehouse.meta.feeder_cursors (
    source VARCHAR,
    stream VARCHAR,
    cursor_path VARCHAR,  -- e.g., s3://landing/raw/shopify/orders/year=2026/month=01/day=09/file.parquet
    updated_at TIMESTAMP
    -- Note: DuckLake doesn't support PRIMARY KEY syntax, composite key is (source, stream)
)
```

## Three Asset Layers

Assets defined in `orchestration/assets.py`, data in `orchestration/dag/`

### 1. Landing (DDL Reconciliation)

**Asset:** `landing_tables`

Reconciles SQL DDL files to DuckLake tables:
- Creates missing tables
- Adds missing columns to existing tables
- Idempotent - safe to run on every deploy

**Files:** `orchestration/dag/landing/{source}/stg_{source}__{stream}.sql`

```sql
CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_shopify__orders (
    _airbyte_raw_id VARCHAR,
    _airbyte_extracted_at TIMESTAMP WITH TIME ZONE,
    id BIGINT,
    ...
);
```

### 2. Feeders (One Per Stream)

**Assets:** `feeder_{source}_{stream}` (auto-generated from stream configs)

Each feeder:
1. Discovers new S3 files after cursor
2. Registers them into landing table via DuckLake
3. Runs the corresponding processed dbt model
4. Updates cursor
5. Cleans up old files

**Sensors:** `{source}_{stream}_sensor` triggers feeder when new files arrive

### 3. Enriched (Auto-Materialize)

**Asset:** `enriched_dbt_models`

All dbt models tagged `enriched`. Auto-materializes when upstream processed models update.

**Files:** `orchestration/dag/enriched/{domain}/fct_*.sql`

## Project Structure

```
orchestration/
├── definitions.py              # Dagster entry point
├── resources.py                # dbt resource config
├── assets.py                   # ALL asset definitions (unified)
├── dag/
│   ├── sources/                # Discovered schemas (reference)
│   │   ├── shopify/*.json
│   │   └── klaviyo/*.json
│   ├── streams/                # Stream configs (source of truth)
│   │   ├── shopify/
│   │   │   ├── orders.json
│   │   │   └── _catalog.json   # Generated for Airbyte
│   │   └── klaviyo/
│   ├── landing/                # DDL files for DuckLake tables
│   │   ├── lib.py              # Duck Feeder library
│   │   ├── shopify/stg_*.sql
│   │   └── klaviyo/stg_*.sql
│   ├── processed/              # dbt intermediate models
│   │   ├── shopify/int_*.sql
│   │   └── klaviyo/int_*.sql
│   └── enriched/               # dbt fact/dimension models
│       └── sales/fct_*.sql
├── dbt_project.yml
└── profiles.yml
```

## Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| S3 Raw | `raw/{source}/{stream}/` | `raw/shopify/orders/` |
| Landing DDL | `stg_{source}__{stream}` | `stg_shopify__orders` |
| Processed Model | `int_{source}__{stream}` | `int_shopify__orders` |
| Enriched Model | `fct_{domain}` / `dim_{entity}` | `fct_sales` |
| Feeder Asset | `feeder_{source}_{stream}` | `feeder_shopify_orders` |
| Sensor | `{source}_{stream}_sensor` | `shopify_orders_sensor` |

## DuckLake Setup

DuckLake uses Postgres as catalog and S3 for storage:

```sql
ATTACH 'ducklake:postgres:host=... dbname=ducklake ...'
    AS lakehouse (DATA_PATH 's3://landing/raw/')
```

**Schemas:**
- `lakehouse.staging` - Raw data registered from S3
- `lakehouse.main` - Transformed processed/enriched tables
- `lakehouse.meta` - Metadata (cursors, etc.)

## Dagster Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Dagster Helm Release                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐    ┌──────────────────┐              │
│  │ dagster-webserver│    │  dagster-daemon  │              │
│  │ (Dagster image)  │    │ (Dagster image)  │              │
│  │ - UI             │    │ - Sensors        │              │
│  │ - GraphQL API    │    │ - Run queue      │              │
│  └────────┬─────────┘    └────────┬─────────┘              │
│           │         gRPC          │                         │
│           └───────────┬───────────┘                         │
│                       ▼                                     │
│  ┌─────────────────────────────────────────┐               │
│  │     dagster-user-deployments            │               │
│  │     (lotus-lake image)                  │               │
│  │                                         │               │
│  │  - orchestration/definitions.py         │               │
│  │  - dbt models + Duck Feeder library     │               │
│  │                                         │               │
│  └─────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

## Infrastructure (h-kube)

| Component | Namespace | Purpose |
|-----------|-----------|---------|
| SeaweedFS | `seaweedfs` | S3 storage for Parquet + logs |
| CNPG `ducklake-db` | `lotus-lake` | DuckLake metadata catalog |
| CNPG `dagster-db` | `lotus-lake` | Dagster run/event storage |
| Airbyte | `airbyte` | Data ingestion |
| Cube.js | `lotus-lake` | Semantic layer (PostgreSQL protocol + REST API) |
| tofu-controller | `flux-system` | GitOps for Terraform |

## Cube.js Semantic Layer

Cube.js exposes DuckLake tables via PostgreSQL protocol (port 5432) and REST API (port 4000) for BI tools.

See [cube.md](cube.md) for detailed documentation.

### Production Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 CUBE.JS PRODUCTION ARCHITECTURE              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐                                           │
│  │  Cube API    │   ← Deployment                            │
│  │  :4000 REST  │     Handles API requests                  │
│  │  :5432 SQL   │     Connects to Cube Store                │
│  └──────┬───────┘                                           │
│         │                                                    │
│         ▼                                                    │
│  ┌──────────────────────────┐                               │
│  │  Cube Store Router       │   ← StatefulSet               │
│  │  Coordinates queries     │     Stable DNS required       │
│  └──────────┬───────────────┘                               │
│             │                                                │
│             ▼                                                │
│  ┌──────────────────────────┐                               │
│  │  Cube Store Worker       │   ← StatefulSet               │
│  │  Executes queries        │     Scalable horizontally     │
│  └──────────────────────────┘                               │
│                                                              │
│  ┌──────────────────────────┐                               │
│  │  Refresh Worker          │   ← Deployment                │
│  │  Builds pre-aggregations │                               │
│  └──────────────────────────┘                               │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Connection

```bash
# PostgreSQL protocol (Power BI, DBeaver, psql, etc.)
Host: 100.64.0.5 (Tailscale/Traefik node)
Port: 5432
Database: cube
User: cube
Password: <CUBEJS_API_SECRET from lotus-lake-terraform-vars>
```

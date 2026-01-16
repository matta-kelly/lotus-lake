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
sources → streams → processed → enriched
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
    ├── 2. Batch files (10 per dbt run)
    ├── 3. Run dbt with file paths as var
    ├── 4. dbt reads parquet directly via read_parquet()
    └── 5. Update cursor
    ↓
DuckLake processed table (int_shopify__orders)
    ↓
Enriched auto-materializes (fct_sales)
```

**Key insight**: No staging layer. dbt reads S3 parquet directly. Memory stays bounded to ~1 file (DuckDB streams through the file list).

## The Feeder Pattern

The feeder solves OOM issues from large Airbyte backfills by processing files incrementally.

### Key Concepts

| Concept | What | Why |
|---------|------|-----|
| **Cursor** | Last processed S3 file path | Resume from where we left off |
| **Partition-aware discovery** | Only glob from cursor date forward | Avoid scanning all historical partitions |
| **Batch by count** | Group files (10 per dbt run) | Reduce dbt startup overhead |
| **Direct parquet read** | `read_parquet()` in dbt models | No staging tables, memory bounded |

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
│  for batch in chunks(files, 10):                            │
│       │                                                     │
│       ├── dbt run --select tag:{source}__{stream}           │
│       │        --vars '{"files": [...]}'                    │
│       │        └── read_parquet() streams through files     │
│       │        └── delete+insert into DuckLake table                │
│       │                                                     │
│       └── set_cursor(last_file_in_batch)                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

DuckDB streams through the file list one at a time, so memory is bounded to ~1 file regardless of batch size.

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

## Two Asset Layers

Assets defined in `orchestration/assets.py`, data in `orchestration/dag/`

### 1. Feeders (One Per Stream)

**Assets:** `{source}_{stream}` (auto-generated from stream configs)

Each feeder:
1. Discovers new S3 files after cursor
2. Batches files (10 per dbt run)
3. Runs dbt with file paths as `--vars '{"files": [...]}'`
4. dbt model reads parquet directly and upserts into DuckLake
5. Updates cursor

**Sensors:** `{source}_{stream}_sensor` triggers feeder when new files arrive

### 2. Enriched (Auto-Materialize)

**Asset:** `enriched_dbt_models`

All dbt models tagged `enriched`. Auto-materializes when upstream processed models update.

**Files:** `orchestration/dag/enriched/{domain}/fct_*.sql`

## Project Structure

```
orchestration/
├── definitions.py              # Dagster entry point
├── resources.py                # dbt resource config
├── assets.py                   # ALL asset definitions (unified)
├── lib.py                      # Core functions (cursor, file discovery)
├── dag/
│   ├── sources/                # Discovered schemas (reference)
│   │   ├── shopify/*.json
│   │   └── klaviyo/*.json
│   ├── streams/                # Stream configs (source of truth)
│   │   ├── shopify/
│   │   │   ├── orders.json
│   │   │   └── _catalog.json   # Generated for Airbyte
│   │   └── klaviyo/
│   ├── processed/              # dbt models - read parquet directly
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
| Processed Model | `int_{source}__{stream}` | `int_shopify__orders` |
| Enriched Model | `fct_{domain}` / `dim_{entity}` | `fct_sales` |
| Feeder Asset | `{source}_{stream}` | `shopify_orders` |
| Sensor | `{source}_{stream}_sensor` | `shopify_orders_sensor` |

## DuckLake Setup

DuckLake uses Postgres as catalog and S3 for storage:

```sql
ATTACH 'ducklake:postgres:host=... dbname=ducklake ...'
    AS lakehouse (DATA_PATH 's3://landing/raw/')
```

**Schemas:**
- `lakehouse.main` - All tables (processed int_*, enriched fct_*)
- `lakehouse.meta` - Metadata (feeder_cursors)

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

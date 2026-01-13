# Lotus Lake

Data platform for Lotus & Luna e-commerce. Lakehouse architecture on k3s with GitOps deployment.

## Stack

```
Shopify/Klaviyo → Airbyte → SeaweedFS (S3) → Dagster → DuckLake/dbt-duckdb
```

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | Airbyte | Incremental sync from sources to S3 Parquet |
| Storage | SeaweedFS | S3-compatible object storage |
| Catalog | DuckLake + Postgres | SQL metadata catalog with ACID, time travel |
| Transform | dbt-duckdb | SQL transforms on DuckLake tables |
| Orchestration | Dagster | Asset-based orchestration with sensors |

## Data Flow

```
Sources (Shopify, Klaviyo)
    ↓
Airbyte syncs to S3 Parquet (Hive partitioned)
    ↓
Dagster sensor detects new files
    ↓
Feeder asset: batch → register → dbt run → update cursor
    ↓
Landing (stg_*) → Processed (int_*) → Enriched (fct_*)
```

## Directory Structure

```
lotus-lake/
├── deploy/                      # Kubernetes manifests (Flux deploys these)
│   └── dagster/                 # Dagster Helm release + CNPG database
├── docs/                        # Detailed documentation
├── orchestration/               # Dagster + dbt unified workspace
│   ├── definitions.py           # Dagster entry point
│   ├── assets.py                # All asset definitions
│   ├── dbt_project.yml          # dbt configuration
│   ├── airbyte/
│   │   └── terraform/           # Airbyte sources/destinations/connections
│   └── dag/
│       ├── streams/             # Stream configs (source of truth)
│       ├── landing/             # DDL + Duck Feeder library
│       ├── processed/           # dbt int_* models
│       └── enriched/            # dbt fct_* models
├── .github/workflows/           # CI/CD pipeline
├── CLAUDE.md                    # AI assistant quick reference
└── Dockerfile                   # Dagster user code image
```

## Deployment

This repo is deployed via **GitOps** through the [h-kube](../h-kube) cluster:

- **Dagster**: Flux HelmRelease from `deploy/dagster/`
- **Airbyte config**: tofu-controller applies `orchestration/airbyte/terraform/`

**All changes go through git push.** Do not run terraform locally.

## Documentation

| Doc | Description |
|-----|-------------|
| [Architecture](docs/architecture.md) | System design, Duck Feeder pattern, data flow |
| [Adding Sources](docs/adding-a-source.md) | Add new Airbyte sources and streams |
| [Adding Flows](docs/adding-a-flow.md) | Add landing DDL and dbt models |
| [Adding Destinations](docs/adding-a-destination.md) | Configure S3 or other destinations |
| [Deployment](docs/deployment.md) | CI/CD pipeline and Kubernetes deployment |
| [Secrets](docs/secrets.md) | SOPS encryption and credential management |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and debugging |
| [Known Issues](docs/tickets.md) | Tracked issues and workarounds |

For AI assistants, see [CLAUDE.md](CLAUDE.md).

## Quick Start

### View Dagster UI

```bash
# Port forward (or use ingress if configured)
kubectl port-forward -n lotus-lake svc/dagster-webserver 3000:80
# Open http://localhost:3000
```

### Add a New Stream

```bash
# 1. Create stream config
vim orchestration/dag/streams/SOURCE/STREAM.json

# 2. Regenerate Airbyte catalog
python orchestration/airbyte/generate-catalog.py

# 3. Create landing DDL
vim orchestration/dag/landing/SOURCE/stg_SOURCE__STREAM.sql

# 4. Create processed model
vim orchestration/dag/processed/SOURCE/int_SOURCE__STREAM.sql

# 5. Regenerate dbt manifest
cd orchestration && dbt parse

# 6. Push
git add . && git commit -m "Add SOURCE/STREAM" && git push
```

See [docs/adding-a-source.md](docs/adding-a-source.md) and [docs/adding-a-flow.md](docs/adding-a-flow.md) for details.

## Data Sources

| Source | Streams |
|--------|---------|
| Shopify | orders, customers, order_refunds |
| Klaviyo | profiles, events, campaigns, flows, metrics, lists |

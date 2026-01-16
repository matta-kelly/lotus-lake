# Getting Started (AI Context)

Quick reference for understanding the lotus-lake data platform.

---

## What This Project Does

**lotus-lake** is a modern data lakehouse that:
- Ingests data from Shopify and Klaviyo via Airbyte
- Stores data in S3-compatible storage (SeaweedFS) as Parquet files
- Uses DuckLake (DuckDB + PostgreSQL catalog) for ACID transactions and time travel
- Transforms data with dbt-duckdb
- Orchestrates everything with Dagster
- Exposes data via Cube.js for BI tools (PostgreSQL protocol + REST API)

---

## Key Concepts

| Concept | What It Is |
|---------|------------|
| **DuckLake** | SQL-based lakehouse - PostgreSQL stores metadata, S3 stores Parquet files |
| **Feeder** | Pattern that incrementally processes S3 files into DuckLake tables |
| **Cursor** | Tracks last-processed file per stream, stored in `lakehouse.meta.feeder_cursors` |
| **Two-repo architecture** | `lotus-lake` (app code) + `h-kube` (infrastructure) |

---

## Repository Structure

```
lotus-lake/
├── orchestration/
│   ├── definitions.py          # Dagster entry point
│   ├── assets.py               # All asset definitions
│   ├── dag/
│   │   ├── landing/            # DDL files + lib.py (feeder library)
│   │   │   ├── lib.py          # get_ducklake_connection(), feeder functions
│   │   │   ├── shopify/stg_*.sql
│   │   │   └── klaviyo/stg_*.sql
│   │   ├── processed/          # dbt models (int_*)
│   │   └── enriched/           # dbt models (fct_*)
│   ├── airbyte/terraform/      # Airbyte sources, destinations, connections
│   ├── cube/                   # Cube.js semantic layer
│   │   ├── cube.js             # DuckLake connection config
│   │   ├── model/processed.js  # Auto-generates cubes for int_* models
│   │   └── model/enriched.js   # Auto-generates cubes for fct_* models
│   └── dbt_project.yml
├── deploy/dagster/             # Kubernetes deployment (HelmRelease)
├── docs/                       # Documentation
└── Dockerfile                  # Main Dagster image
```

---

## Data Flow

```
Shopify/Klaviyo → Airbyte → S3 Parquet → Sensor detects → Feeder processes
                                                              ↓
                                         DuckLake staging (stg_*) → processed (int_*) → enriched (fct_*)
                                                                                              ↓
                                                                              Cube.js exposes via PostgreSQL
```

---

## Key Files to Understand

| File | Purpose |
|------|---------|
| `orchestration/dag/landing/lib.py` | **Core library** - `get_ducklake_connection()`, feeder functions |
| `orchestration/assets.py` | All Dagster assets - feeders, sensors, dbt integration |
| `orchestration/airbyte/terraform/sources.tf` | Airbyte source configurations |
| `orchestration/cube/cube.js` | Cube.js DuckLake connection |
| `docs/ducklake-metadata.md` | DuckLake catalog tables, runbook commands |

---

## DuckLake Connection

Always use `get_ducklake_connection()` from `orchestration/dag/landing/lib.py`:

```python
from orchestration.dag.landing.lib import get_ducklake_connection

conn = get_ducklake_connection()
result = conn.execute("SELECT * FROM lakehouse.main.int_shopify__orders LIMIT 5").fetchall()
conn.close()
```

This properly configures:
- S3 credentials via `CREATE SECRET` (not `SET s3_*` commands)
- DuckLake attachment to PostgreSQL catalog
- httpfs extension for S3 access

---

## DuckLake Schemas

| Schema | Purpose |
|--------|---------|
| `lakehouse.staging` | Raw data from Airbyte (stg_* tables) |
| `lakehouse.main` | Transformed data (int_*, fct_* tables) |
| `lakehouse.meta` | Metadata (feeder_cursors table) |

---

## Common Operations

### Run Commands from Dagster Pod

The Dagster pod has all credentials configured. Use it for DuckDB/DuckLake operations:

```bash
export KUBECONFIG=/home/mkultra/bode/h-kube/generated/kubeconfig.yaml

kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- python3 -c "
from orchestration.dag.landing.lib import get_ducklake_connection

conn = get_ducklake_connection()
# Your DuckDB/DuckLake commands here
conn.close()
"
```

### Check DuckLake Catalog (PostgreSQL)

```bash
PGPASSWORD=$(kubectl get secret -n lotus-lake ducklake-db-app -o jsonpath='{.data.password}' | base64 -d)

kubectl exec -n lotus-lake ducklake-db-1 -c postgres -- bash -c "
PGPASSWORD='$PGPASSWORD' psql -h localhost -U ducklake -d ducklake -c \"
SELECT s.schema_name, t.table_name
FROM ducklake_table t
JOIN ducklake_schema s ON t.schema_id = s.schema_id
WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL
ORDER BY s.schema_name, t.table_name;
\""
```

---

## Infrastructure (h-kube)

The companion repo `~/bode/h-kube` contains:

| Component | Namespace | Files |
|-----------|-----------|-------|
| SeaweedFS | `seaweedfs` | S3-compatible storage |
| Airbyte | `airbyte` | Data ingestion platform |
| DuckLake DB | `lotus-lake` | PostgreSQL catalog (CNPG) |
| Dagster | `lotus-lake` | Orchestration |
| Cube.js | `lotus-lake` | Semantic layer |
| Traefik | `networking` | Ingress/load balancing |
| tofu-controller | `flux-system` | GitOps Terraform |

**Key paths:**
- Kubeconfig: `/home/mkultra/bode/h-kube/generated/kubeconfig.yaml`
- Secrets: `cluster/namespaces/lotus-lake/` (SOPS-encrypted)

---

## Quick Troubleshooting

| Symptom | Check |
|---------|-------|
| DuckLake errors | See `docs/ducklake-metadata.md` runbook |
| Feeder not processing | Check cursor in `lakehouse.meta.feeder_cursors` |
| S3 403 errors | Verify credentials match in SeaweedFS config |
| Pod won't start | `kubectl logs -n lotus-lake -l component=user-deployments --previous` |

---

## Related Docs

- [architecture.md](architecture.md) - Detailed system design
- [ducklake-metadata.md](ducklake-metadata.md) - DuckLake catalog tables and runbook
- [deployment.md](deployment.md) - CI/CD pipeline
- [troubleshooting.md](troubleshooting.md) - Debugging workflows
- [adding-a-flow.md](adding-a-flow.md) - Adding new data streams

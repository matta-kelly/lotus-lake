# Lotus Lake

Data platform for Lotus & Luna e-commerce. Lakehouse architecture on k3s.

## Stack

```
Shopify/Klaviyo → Airbyte → Iceberg/Nessie/MinIO → dbt-trino → Trino → Cube.js
```

## Directory Structure

```
lotus-lake/
├── infrastructure/           # Helm values + service configs
│   ├── airbyte/
│   │   ├── values.yaml
│   │   ├── terraform/        # Sources, destinations, connections
│   │   ├── streams/          # Airbyte stream configs
│   │   └── export-streams.py
│   ├── minio/
│   ├── nessie/
│   └── trino/
│
├── orchestration/            # dbt + dagster (unified)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── definitions.py        # Dagster entry point
│   ├── assets/
│   │   ├── streams/          # JSON schemas + staging SQL
│   │   │   ├── shopify/      # orders.json, stg_shopify__orders.sql
│   │   │   └── klaviyo/
│   │   ├── core/             # Transform SQL + dagster assets
│   │   │   ├── shopify/      # int_shopify__orders.sql, shopify.py
│   │   │   └── klaviyo/
│   │   └── marts/            # Business aggregations
│   ├── resources/
│   └── sensors/
│
├── helmfile.yaml.gotmpl
└── .env                      # Secrets (not committed)
```

## Data Layers

| Layer | Location | Prefix | Purpose |
|-------|----------|--------|---------|
| Bronze | Airbyte → Iceberg | - | Raw ingested data |
| Streams | `assets/streams/` | `stg_` | 1:1 views, rename/cast |
| Core | `assets/core/` | `int_` | Flatten, filter, join |
| Marts | `assets/marts/` | `fct_`, `dim_` | Business aggregations |

## Quick Start

```bash
# 1. Create cluster
k3d cluster create lotus-dev

# 2. Deploy infrastructure
set -a && source .env && set +a
helmfile sync

# 3. Wait for pods
kubectl get pods -A -w

# 4. Port forwards
kubectl port-forward -n airbyte deployment/airbyte-server 8080:8001 &
kubectl port-forward -n nessie svc/nessie 19120:19120 &

# 5. Get workspace ID (update .env with TF_VAR_workspace_id)
curl -s http://localhost:8080/api/public/v1/workspaces | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['workspaceId'])"

# 6. Apply Airbyte config
cd infrastructure/airbyte/terraform
set -a && source ../../../.env && set +a
terraform init && terraform apply
```

## Data Sources

**Shopify:** orders, customers, products, product_variants, order_refunds, discount_codes

**Klaviyo:** profiles, events, campaigns, flows, metrics, lists

## Services

| Service | Port | Access |
|---------|------|--------|
| Airbyte | 8080 | `kubectl port-forward -n airbyte deployment/airbyte-server 8080:8001` |
| Trino | 8081 | `kubectl port-forward -n trino svc/trino 8081:8080` |
| Nessie | 19120 | `kubectl port-forward -n nessie svc/nessie 19120:19120` |
| MinIO | 9000 | `kubectl port-forward -n minio svc/minio 9000:9000` |

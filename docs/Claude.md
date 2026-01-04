# Claude Code Workflow

Quick reference for working on Lotus Lake.

## Environment Setup

```bash
# Start cluster
k3d cluster create lotus-dev
set -a && source .env && set +a && helmfile sync
kubectl get pods -A -w

# Port forwards
kubectl port-forward -n airbyte deployment/airbyte-server 8080:8001 &
kubectl port-forward -n nessie svc/nessie 19120:19120 &
kubectl port-forward -n minio svc/minio 9000:9000 &
kubectl port-forward -n trino svc/trino 8081:8080 &
```

## Airbyte Config

```bash
cd infrastructure/airbyte/terraform
set -a && source ../../../.env && set +a
terraform init && terraform apply
```

## Common Issues

**Workspace ID stale after cluster recreate:**
```bash
curl -s http://localhost:8080/api/public/v1/workspaces | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['workspaceId'])"
# Update TF_VAR_workspace_id in .env
```

**Terraform state stale after cluster recreate:**
```bash
cd infrastructure/airbyte/terraform
rm -f terraform.tfstate* .terraform.lock.hcl
rm -rf .terraform
terraform init && terraform apply
```

**Airbyte database race condition:**
```bash
kubectl delete pod -n airbyte -l app.kubernetes.io/name=server
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=server -n airbyte --timeout=120s
```

## Key Paths

| What | Where |
|------|-------|
| Helm values | `infrastructure/<service>/values.yaml` |
| Airbyte terraform | `infrastructure/airbyte/terraform/` |
| Schema JSON files | `orchestration/assets/streams/<source>/*.json` |
| Staging SQL | `orchestration/assets/streams/<source>/stg_*.sql` |
| Core SQL | `orchestration/assets/core/<source>/int_*.sql` |
| Dagster assets | `orchestration/assets/core/<source>/<source>.py` |
| dbt config | `orchestration/dbt_project.yml` |
| Dagster entry | `orchestration/definitions.py` |

## Naming

- Staging: `stg_<source>__<entity>.sql`
- Core: `int_<source>__<entity>.sql`
- Marts: `fct_<domain>.sql`, `dim_<entity>.sql`

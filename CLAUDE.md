# Claude Code Workflow

Quick reference for working on Lotus Lake.

---

## After Context Compaction: Re-read the Docs

**If this session was resumed from a compacted/summarized conversation, immediately read the full docs directory to restore context:**

```bash
# Read all docs to restore full context
cat docs/*.md
```

| Doc | Purpose |
|-----|---------|
| `docs/architecture.md` | System design, Duck Feeder pattern, data flow |
| `docs/adding-a-source.md` | Adding Airbyte sources and streams |
| `docs/adding-a-flow.md` | Adding landing DDL + dbt models |
| `docs/deployment.md` | CI/CD pipeline and Kubernetes deployment |
| `docs/troubleshooting.md` | Debugging, health checks, common issues |
| `docs/secrets.md` | SOPS encryption, credential management |
| `docs/tickets.md` | Known issues and workarounds |

**Compaction loses details. The docs have the full picture.**

---

## Kubectl Setup

**All kubectl commands require the h-kube kubeconfig:**

```bash
export KUBECONFIG=/home/mkultra/bode/h-kube/generated/kubeconfig.yaml
```

Without this, you'll get connection refused errors. The default `~/.kube/config` is empty.

---

## CRITICAL: Never Run Terraform Locally

```
╔═══════════════════════════════════════════════════════════════════════════╗
║  DO NOT RUN `terraform apply` OR `terraform plan` LOCALLY                ║
║                                                                           ║
║  tofu-controller manages Terraform state in K8s.                         ║
║  Running locally creates a SEPARATE state file that drifts.              ║
║  This causes syncs to fail with "secret not found" errors.               ║
║                                                                           ║
║  See TICKET-003 in docs/tickets.md for full explanation.                 ║
╚═══════════════════════════════════════════════════════════════════════════╝
```

**If you see `terraform.tfstate` files locally, DELETE THEM:**
```bash
rm -f orchestration/airbyte/terraform/terraform.tfstate*
rm -rf orchestration/airbyte/terraform/.terraform
```

**The ONLY way to apply Terraform changes:** Edit files → `git push` → tofu-controller applies

---

## Key Paths

| What | Where |
|------|-------|
| Airbyte terraform | `orchestration/airbyte/terraform/` |
| **Dagster assets** | `orchestration/assets.py` |
| Duck Feeder library | `orchestration/dag/landing/lib.py` |
| Stream configs | `orchestration/dag/streams/{source}/*.json` |
| Landing DDL | `orchestration/dag/landing/{source}/stg_*.sql` |
| Processed dbt models | `orchestration/dag/processed/{source}/int_*.sql` |
| Enriched dbt models | `orchestration/dag/enriched/{domain}/fct_*.sql` |

---

## Data Flow

```
Airbyte → S3 Parquet → Sensor → Feeder → Landing → Processed → Enriched
                                  │
                                  ├── get_files_after_cursor
                                  ├── batch_by_size (~500MB)
                                  ├── register_batch (ducklake_add_data_files)
                                  ├── dbt run (merge into processed)
                                  ├── set_cursor
                                  └── cleanup_old_files (trails by 7 days)
```

**Three asset layers** (`assets.py`):
1. `landing_tables` - Reconciles DDL files to DuckLake tables
2. `feeder_{source}_{stream}` - Auto-generated per stream
3. `enriched_dbt_models` - All enriched models

---

## Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| Landing DDL | `stg_{source}__{stream}` | `stg_shopify__orders` |
| Processed model | `int_{source}__{stream}` | `int_shopify__orders` |
| Enriched model | `fct_{domain}` | `fct_sales` |
| Feeder asset | `feeder_{source}_{stream}` | `feeder_shopify_orders` |
| Sensor | `{source}_{stream}_sensor` | `shopify_orders_sensor` |

---

## Common Tasks

### Add a Stream

```bash
# 1. Create stream config
vim orchestration/dag/streams/SOURCE/STREAM.json

# 2. Regenerate Airbyte catalog
python orchestration/airbyte/generate-catalog.py

# 3. Create landing DDL
vim orchestration/dag/landing/SOURCE/stg_SOURCE__STREAM.sql

# 4. Create processed dbt model
vim orchestration/dag/processed/SOURCE/int_SOURCE__STREAM.sql

# 5. Regenerate dbt manifest + push
cd orchestration && dbt parse && cd ..
git add . && git commit -m "Add SOURCE/STREAM" && git push
```

### Trigger Backfill

```bash
# 1. Set "backfill": true in stream config
vim orchestration/dag/streams/SOURCE/STREAM.json

# 2. Push, wait for sync

# 3. Set "backfill": false and push again
```

### Verify Deployment

```bash
# Check deployed code
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake \
  -- head -50 /app/orchestration/assets.py

# Force image pull
kubectl rollout restart deployment -n lotus-lake -l component=user-deployments
```

---

## Stream Config → Auto-Generation

The stream config filename drives all auto-generation:

```
dag/streams/{source}/{stream}.json
       ↓
┌──────┴────────────────────────────────────────────┐
│                                                    │
▼                                                    ▼
S3 Path                                      Auto-generated
raw/{source}/{stream}/year=.../                     │
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                                 Sensor          Feeder          Landing DDL
                            {source}_{stream}  feeder_{source}  stg_{source}__{stream}
                               _sensor            _{stream}
```

---

## Quick Commands

```bash
# Force terraform reconcile
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite

# Force git fetch
kubectl annotate gitrepository -n lotus-lake lotus-lake \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite

# Check feeder cursors (in DuckLake)
SELECT * FROM lakehouse.meta.feeder_cursors;

# Force feeder re-run (delete cursor)
DELETE FROM lakehouse.meta.feeder_cursors
WHERE source = 'shopify' AND stream = 'orders';
```

For debugging and health checks, see `docs/troubleshooting.md`.

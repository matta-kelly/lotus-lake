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
| `docs/architecture.md` | System architecture, Duck Feeder pattern, data flow |
| `docs/adding-a-source.md` | Adding Airbyte sources and streams |
| `docs/adding-a-flow.md` | Adding landing DDL + dbt processed/enriched models |
| `docs/github-actions.md` | CI/CD pipeline |
| `docs/tickets.md` | Known issues |

**Compaction loses details. The docs have the full picture.**

---

## Kubectl Setup

**All kubectl commands require the h-kube kubeconfig:**

```bash
export KUBECONFIG=/home/mkultra/bode/h-kube/generated/kubeconfig.yaml
```

Without this, you'll get connection refused errors. The default `~/.kube/config` is empty.

**Verify connection:**
```bash
kubectl get nodes
```

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

**The ONLY way to apply Terraform changes:**
1. Edit files
2. `git commit && git push`
3. tofu-controller applies automatically

---

## Key Paths

| What | Where |
|------|-------|
| Airbyte terraform | `orchestration/airbyte/terraform/` |
| **Dagster assets** | `orchestration/assets.py` |
| Duck Feeder library | `orchestration/dag/landing/lib.py` |
| Stream configs | `orchestration/dag/streams/{source}/*.json` |
| Source schemas | `orchestration/dag/sources/{source}/*.json` |
| Landing DDL | `orchestration/dag/landing/{source}/stg_*.sql` |
| Processed dbt models | `orchestration/dag/processed/{source}/int_*.sql` |
| Enriched dbt models | `orchestration/dag/enriched/{domain}/fct_*.sql` |
| Tickets/issues | `docs/tickets.md` |

---

## Data Flow

```
sources → streams → landing → processed → enriched
```

```
Airbyte → S3 Parquet → Sensor → Feeder → Landing → Processed → Enriched
                                  │
                                  ├── get_files_after_cursor (partition-aware)
                                  ├── batch_by_size (~500MB)
                                  ├── register_batch (ducklake_add_data_files)
                                  ├── dbt run (merge into processed)
                                  ├── set_cursor
                                  └── cleanup_old_files (trails by 7 days)
```

**Three asset layers in one file** (`assets.py`):
1. `landing_tables` - Reconciles DDL files to DuckLake tables
2. `feeder_{source}_{stream}` - Auto-generated per stream, registers files + runs dbt
3. `enriched_dbt_models` - All enriched models, auto-materializes

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

### Add a Stream (Complete Flow)

```bash
# 1. Create stream config (for Airbyte)
vim orchestration/dag/streams/SOURCE/STREAM.json

# 2. Regenerate Airbyte catalog
python orchestration/airbyte/generate-catalog.py

# 3. Create landing DDL (defines raw schema)
vim orchestration/dag/landing/SOURCE/stg_SOURCE__STREAM.sql

# 4. Create processed dbt model (transforms + dedupe)
vim orchestration/dag/processed/SOURCE/int_SOURCE__STREAM.sql

# 5. Regenerate dbt manifest
cd orchestration && dbt parse

# 6. Commit and push
git add . && git commit -m "Add SOURCE/STREAM" && git push
```

### Trigger Backfill

```bash
# 1. Set backfill flag
vim orchestration/dag/streams/SOURCE/STREAM.json
# Set "backfill": true

# 2. Push, wait for sync to complete

# 3. Set "backfill": false and push again
```

---

## Dagster Asset & Sensor Wiring

### Stream Config is Source of Truth

The stream config filename drives ALL auto-generation:

```
dag/streams/{source}/{stream}.json
       ↓
┌──────┴────────────────────────────────────────────┐
│                                                    │
▼                                                    ▼
S3 Path (Hive format)                       Auto-generated
raw/{source}/{stream}/year=.../                     │
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                                 Sensor          Feeder          Landing DDL
                            {source}_{stream}  feeder_{source}  stg_{source}__{stream}
                               _sensor            _{stream}
```

### Feeder Factory

Each stream gets its own feeder asset (auto-generated in `assets.py`):

```python
def make_feeder_asset(source: str, stream: str):
    @asset(name=f"feeder_{source}_{stream}", deps=[AssetKey("landing_tables")])
    def _feeder(context, dbt):
        # 1. Get files after cursor
        # 2. Batch by size
        # 3. Register batch into landing
        # 4. Run processed dbt model
        # 5. Update cursor
        # 6. Cleanup old files
```

---

## Verification After Code Push

```bash
# Check deployed code
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake \
  -- head -50 /app/orchestration/assets.py

# Force image pull if stale
kubectl delete pod -n lotus-lake -l component=user-deployments
```

**CI takes ~2 minutes. Don't test until new image is deployed.**

---

## Troubleshooting

### Terraform Stuck/Failing

```bash
# Check status
kubectl get terraform -n lotus-lake -o yaml | grep -A20 status

# Force new reconciliation
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### Check Feeder Cursors

```sql
-- In DuckDB connected to DuckLake
SELECT * FROM lakehouse.meta.feeder_cursors;
```

### Force Feeder Re-run

Delete the cursor to reprocess all files:

```sql
DELETE FROM lakehouse.meta.feeder_cursors
WHERE source = 'shopify' AND stream = 'orders';
```

---

## Reference Docs

| Doc | Purpose |
|-----|---------|
| `docs/architecture.md` | System architecture, Duck Feeder pattern |
| `docs/adding-a-source.md` | Adding Airbyte sources and streams |
| `docs/adding-a-flow.md` | Adding landing DDL + dbt models |
| `docs/github-actions.md` | CI/CD pipeline and image builds |
| `docs/tickets.md` | Known issues and TODOs |

# Deployment

How code changes get from git push to running in the cluster.

---

## Overview

```
Push to main
    ↓
GitHub Actions: validate → build → push image
    ↓
ghcr.io/matta-kelly/lotus-lake:latest
    ↓
Flux GitRepository detects changes (5m interval)
    ↓
Flux Kustomization applies deploy/
    ↓
HelmRelease reconciles (pullPolicy: Always)
    ↓
Dagster pods restart with new image
```

---

## Components

| Component | Source | Image |
|-----------|--------|-------|
| Webserver | Dagster Helm chart | `dagster/dagster` (official) |
| Daemon | Dagster Helm chart | `dagster/dagster` (official) |
| User Deployment | `lotus-lake/deploy/dagster/` | `ghcr.io/matta-kelly/lotus-lake` |
| Cube.js | `lotus-lake/orchestration/cube/` | `ghcr.io/matta-kelly/lotus-lake-cube` |
| Dagster DB | `deploy/dagster/database.yaml` | CNPG Postgres |
| Airbyte Config | `orchestration/airbyte/terraform/` | tofu-controller (no image) |

---

## GitHub Actions Pipeline

### Triggers

```yaml
on:
  push:
    branches: [main]    # Build + push image
  pull_request:
    branches: [main]    # Build only (validate, no push)
```

### Stages

#### 1. Validate (PR + Push)

Catches errors before they reach the cluster:

| Check | Command | Prevents |
|-------|---------|----------|
| State file check | `find -name '*.tfstate*'` | TICKET-003 (state drift) |
| Terraform validate | `terraform validate` | Broken HCL syntax |
| dbt parse | `dbt parse` | Invalid SQL, broken refs |

#### 2. Build Dagster Image (PR + Push)

- Builds `Dockerfile`
- Runs `dbt parse` to bake manifest into image
- Tags with SHA and `latest` (main only)

#### 3. Build Cube.js Image (PR + Push)

- Builds `Dockerfile.cube`
- Copies `orchestration/cube/` + `target/manifest.json`
- Tags with SHA and `latest` (main only)
- Cube factories auto-generate cubes from dbt manifest

#### 5. Push to Registry (Main Only)

- Pushes both images to `ghcr.io/matta-kelly/`:
  - `lotus-lake` (Dagster)
  - `lotus-lake-cube` (Cube.js)
- Tags: `latest` + `<commit-sha>`
- PR builds verify but don't push

### Image Tags

| Tag | When | Use |
|-----|------|-----|
| `latest` | Every push to main | Default for Dagster deployment |
| `<sha>` | Every push to main | Rollback, pinned deployments |

---

## Flux Deployment

### Resources in h-kube

```
cluster/namespaces/lotus-lake/
├── namespace.yaml           # lotus-lake namespace
├── lotus-lake-source.yaml   # GitRepository watching this repo
├── lotus-lake-deploy.yaml   # Kustomization applying ./deploy
├── lotus-lake-terraform.yaml # Terraform for Airbyte config
└── lotus-lake-secrets.yaml  # SOPS-encrypted credentials
```

### Resources in lotus-lake (this repo)

```
deploy/dagster/
├── kustomization.yaml      # Includes all resources
├── repository.yaml         # HelmRepository for dagster charts
├── database.yaml           # CNPG cluster for dagster metadata
├── backup-secret.yaml      # SOPS-encrypted S3 backup creds
└── release.yaml            # HelmRelease with full config
```

### Deployment Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        GitHub Actions                           │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────┐  │
│  │ Validate │ →  │  Build   │ →  │  Push to ghcr.io         │  │
│  │ tf, dbt  │    │ (docker) │    │  (main branch only)      │  │
│  └──────────┘    └──────────┘    └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                           Flux CD                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ GitRepository: lotus-lake                                 │  │
│  │   watches: github.com/matta-kelly/lotus-lake             │  │
│  │   interval: 5m                                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│              ┌───────────────┴───────────────┐                 │
│              ▼                               ▼                  │
│  ┌─────────────────────────┐    ┌─────────────────────────┐   │
│  │ Kustomization:          │    │ Terraform:              │   │
│  │   lotus-lake-deploy     │    │   lotus-lake-airbyte    │   │
│  │   path: ./deploy        │    │   path: ./orchestration │   │
│  └─────────────────────────┘    │         /airbyte/tf     │   │
│              │                   └─────────────────────────┘   │
│              ▼                               │                  │
│  ┌─────────────────────────┐                ▼                  │
│  │ HelmRelease: dagster    │    ┌─────────────────────────┐   │
│  │   image: latest         │    │ Airbyte sources,        │   │
│  │   pullPolicy: Always    │    │ destinations,           │   │
│  └─────────────────────────┘    │ connections             │   │
│                                  └─────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Environment Variables

The Dagster user deployment receives these from secrets:

| Variable | Source | Purpose |
|----------|--------|---------|
| `DUCKLAKE_DB_HOST` | release.yaml value | DuckLake Postgres host |
| `DUCKLAKE_DB_PASSWORD` | `ducklake-db-app` secret | DuckLake Postgres password |
| `S3_ENDPOINT` | release.yaml value | SeaweedFS endpoint |
| `S3_ACCESS_KEY_ID` | `lotus-lake-terraform-vars` | S3 access key |
| `S3_SECRET_ACCESS_KEY` | `lotus-lake-terraform-vars` | S3 secret key |

---

## Making Changes

### Code Changes (Python/SQL)

Just push to main. CI builds a new image, Flux picks it up.

```bash
git add . && git commit -m "fix: update dbt model" && git push
```

### Helm Values Changes

Edit `deploy/dagster/release.yaml`, push. Flux reconciles automatically.

### Adding Environment Variables

1. **If secret**: Add to `lotus-lake-secrets.yaml` in h-kube, reference in `release.yaml`
2. **If plain value**: Add directly to `release.yaml` env section

### Airbyte Configuration

Edit files in `orchestration/airbyte/terraform/`, push. tofu-controller applies.

**Never run terraform locally** - see [CLAUDE.md](../CLAUDE.md) for why.

---

## Force Reconciliation

**Kubeconfig path:** `/home/mkultra/bode/h-kube/generated/kubeconfig.yaml`

```bash
export KUBECONFIG=/home/mkultra/bode/h-kube/generated/kubeconfig.yaml
```

### Force GitRepository Fetch

```bash
kubectl annotate gitrepository -n lotus-lake lotus-lake \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### Force Terraform Apply

```bash
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### Force Pod Restart

```bash
kubectl rollout restart deployment -n lotus-lake -l component=user-deployments
```

---

## Local Testing

Test the build locally before pushing:

```bash
# Build image
docker build -t lotus-lake:local .

# Verify dbt manifest was generated
docker run --rm lotus-lake:local ls -la /app/orchestration/target/

# Test Dagster can load definitions
docker run --rm lotus-lake:local python -c "from orchestration.definitions import defs; print(defs)"
```

---

## Optional CI Extensions

### Add Python Linting

```yaml
- name: Lint Python
  run: |
    pip install ruff
    ruff check orchestration/
```

### Add Security Scanning

```yaml
- name: Run Trivy vulnerability scanner
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ github.sha }}
```

### Add Dependabot

Create `.github/dependabot.yml`:

```yaml
version: 2
updates:
  - package-ecosystem: "pip"
    directory: "/"
    schedule:
      interval: "weekly"
```

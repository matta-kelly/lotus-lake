# GitHub Actions CI/CD

Automated build, validation, and deployment pipeline for lotus-lake.

## Current Workflow

```yaml
# .github/workflows/build.yaml
on:
  push:
    branches: [main]    # Build + push image
  pull_request:
    branches: [main]    # Build only (no push)
```

### What Happens

```
Push to main
    ↓
GitHub Actions builds Docker image
    ↓
Push to ghcr.io/lotusandluna/lotus-lake:latest
Push to ghcr.io/lotusandluna/lotus-lake:<sha>
    ↓
Flux detects new image (ImagePolicy) or new deploy/ changes
    ↓
Dagster pods restart with new image
```

## Pipeline Stages

### 1. Validate (PR + Push)

Catches errors before they reach the cluster:

| Check | What | Prevents |
|-------|------|----------|
| `.tfstate*` check | Blocks commits with local state | TICKET-003 (state drift) |
| `terraform validate` | Validates HCL syntax | TICKET-001 (broken terraform) |
| `dbt parse` | Validates SQL + refs | Bad SQL hitting prod |

### 2. Build Image (PR + Push)

- Only runs if validate passes
- Builds `Dockerfile`
- Bakes `dbt parse` manifest into image
- Tags with SHA and `latest` (main only)

### 3. Push to Registry (Main Only)

- Pushes to `ghcr.io/lotusandluna/lotus-lake`
- PR builds verify but don't push

## Image Tags

| Tag | When | Use |
|-----|------|-----|
| `latest` | Every push to main | Default for Dagster deployment |
| `<sha>` | Every push to main | Rollback, pinned deployments |

## Secrets Required

| Secret | Source | Used For |
|--------|--------|----------|
| `GITHUB_TOKEN` | Auto-provided | Push to ghcr.io |

No additional secrets needed - GitHub provides `GITHUB_TOKEN` automatically.

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

## Optional Extensions

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
    format: 'sarif'
    output: 'trivy-results.sarif'
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

  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
```

## Deployment Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        GitHub Actions                            │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────┐  │
│  │   Lint   │ →  │  Build   │ →  │  Push to ghcr.io         │  │
│  │  (ruff)  │    │ (docker) │    │  (main branch only)      │  │
│  └──────────┘    └──────────┘    └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                           Flux CD                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ GitRepository: lotus-lake                                 │   │
│  │   watches: github.com/matta-kelly/lotus-lake             │   │
│  │   interval: 5m                                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Kustomization: lotus-lake-deploy                          │   │
│  │   path: ./deploy                                          │   │
│  │   dependsOn: infrastructure-services                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ HelmRelease: dagster                                      │   │
│  │   image: ghcr.io/lotusandluna/lotus-lake:latest          │   │
│  │   pullPolicy: Always                                      │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### Build Fails on dbt parse

```bash
# Check locally
cd orchestration
dbt parse --profiles-dir . --project-dir .

# Common issues:
# - Missing source in _sources.yml
# - Typo in ref() or source()
# - Invalid SQL syntax
```

### Image Not Updating in Cluster

```bash
# Check Flux is seeing the repo
kubectl get gitrepository -n lotus-lake

# Force reconciliation
kubectl annotate gitrepository -n lotus-lake lotus-lake \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite

# Check pod image
kubectl get pods -n lotus-lake -o jsonpath='{.items[*].spec.containers[*].image}'
```

### Dagster Won't Start

```bash
# Check pod logs
kubectl logs -n lotus-lake -l app=dagster-user-deployments

# Common issues:
# - Missing env vars (secrets not mounted)
# - Python import errors
# - dbt manifest not found
```

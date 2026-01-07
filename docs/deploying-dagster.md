# Deploying Dagster

## Overview

Dagster deployment is automated via GitOps:

1. Push to `main` branch
2. GitHub Actions builds `ghcr.io/lotusandluna/lotus-lake` image
3. Flux detects new image and updates the user-deployment

## Components

| Component | Source | Image |
|-----------|--------|-------|
| Webserver | Dagster Helm chart | `dagster/dagster` (official) |
| Daemon | Dagster Helm chart | `dagster/dagster` (official) |
| User Deployment | `lotus-lake/deploy/dagster/` | `ghcr.io/lotusandluna/lotus-lake` |
| Database | `lotus-lake/deploy/dagster/database.yaml` | CNPG Postgres |

## Files

```
deploy/dagster/
├── kustomization.yaml      # Includes all resources
├── repository.yaml         # HelmRepository for dagster charts
├── database.yaml           # CNPG cluster for dagster metadata
├── backup-secret.yaml      # SOPS-encrypted S3 creds for backups
└── release.yaml            # HelmRelease with full config
```

## Making Changes

### Code changes (Python/SQL)

Just push to main. CI builds a new image, Flux picks it up.

```bash
git add . && git commit -m "fix: update dbt model" && git push
```

### Helm values changes

Edit `deploy/dagster/release.yaml`, push. Flux reconciles.

### Adding environment variables

1. If secret: Add to `lotus-lake-secrets` in h-kube, reference in `release.yaml`
2. If plain value: Add directly to `release.yaml` env section

## Environment Variables

The user deployment container receives:

| Variable | Source | Purpose |
|----------|--------|---------|
| `DUCKLAKE_DB_HOST` | Value | DuckLake Postgres host |
| `DUCKLAKE_DB_PASSWORD` | Secret | DuckLake Postgres password |
| `S3_ENDPOINT` | Value | SeaweedFS endpoint |
| `S3_ACCESS_KEY_ID` | Secret | S3 access key |
| `S3_SECRET_ACCESS_KEY` | Secret | S3 secret key |

Note: Airbyte runs independently on its own schedule. Dagster only handles dbt transforms.

## Troubleshooting

### Check pod status

```bash
kubectl get pods -n lotus-lake -l app.kubernetes.io/name=dagster
```

### View user deployment logs

```bash
kubectl logs -n lotus-lake -l component=user-deployments -f
```

### Force image pull

```bash
kubectl rollout restart deployment -n lotus-lake dagster-user-deployments
```

### Check HelmRelease status

```bash
kubectl get helmrelease -n lotus-lake dagster -o yaml | grep -A20 status
```

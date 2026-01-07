# Secrets Management

All secrets for lotus-lake use SOPS encryption with age keys.

---

## Secret Inventory

### CNPG Auto-Created (Do Not Edit)

| Secret | Created By | Used By |
|--------|------------|---------|
| `dagster-db-app` | CNPG operator | Dagster webserver/daemon |
| `ducklake-db-app` | CNPG operator | Dagster user deployment (DuckLake) |

These are auto-generated when CNPG creates the PostgreSQL clusters. Never edit manually.

---

### SOPS-Encrypted Secrets

#### In `h-kube` repo

| Secret | File | Purpose |
|--------|------|---------|
| `seaweedfs-s3-config` | `cluster/infrastructure/services/seaweedfs/s3-auth-secret.yaml` | Master S3 auth - defines all identities |
| `lotus-lake-terraform-vars` | `cluster/namespaces/lotus-lake/lotus-lake-secrets.yaml` | Airbyte terraform vars + S3 creds |
| `ducklake-db-backup-creds` | `cluster/namespaces/lotus-lake/ducklake-db-backup-secret.yaml` | DuckLake CNPG backup to S3 |

#### In `lotus-lake` repo

| Secret | File | Purpose |
|--------|------|---------|
| `dagster-db-backup-creds` | `deploy/dagster/backup-secret.yaml` | Dagster CNPG backup to S3 |

---

## S3 Identities

SeaweedFS uses identity-based access. Each service has its own identity.

| Identity | Used By | Bucket Access |
|----------|---------|---------------|
| `lotus-lake-s3` | Airbyte destinations, Dagster sensors | `airbyte-landing/*` |
| `rubberducky` | DuckLake CNPG backups | `cnpg-backups/*` |
| `daggy` | Dagster CNPG backups | `cnpg-backups/*` |

All identities are defined in `seaweedfs-s3-config`. When adding a new identity:
1. Add to SeaweedFS config
2. Create corresponding secret for the service
3. Both must use identical credentials

---

## Credential Sync Requirements

These credentials **must match** across files:

```
seaweedfs-s3-config (master)
    │
    ├── lotus-lake-s3 ──→ lotus-lake-terraform-vars (minio_user/minio_password)
    │
    ├── rubberducky ────→ ducklake-db-backup-creds (ACCESS_KEY_ID/SECRET_ACCESS_KEY)
    │
    └── daggy ──────────→ dagster-db-backup-creds (ACCESS_KEY_ID/SECRET_ACCESS_KEY)
```

If credentials don't match, services will get S3 403 errors.

---

## Rotation Procedure

### 1. Generate New Credentials

```bash
# Generate a secure password
openssl rand -base64 18 | tr -d '/+=' | head -c 14
```

### 2. Update SeaweedFS Config (Source of Truth)

```bash
cd /path/to/h-kube
sops cluster/infrastructure/services/seaweedfs/s3-auth-secret.yaml
# Edit the identity's credentials
```

### 3. Update Dependent Secrets

For each identity changed, update its corresponding secret:

```bash
# lotus-lake-s3
sops cluster/namespaces/lotus-lake/lotus-lake-secrets.yaml

# rubberducky
sops cluster/namespaces/lotus-lake/ducklake-db-backup-secret.yaml

# daggy (in lotus-lake repo)
cd /path/to/lotus-lake
sops deploy/dagster/backup-secret.yaml
```

### 4. Commit and Push

```bash
# Push h-kube first (infra)
cd /path/to/h-kube
git add -A && git commit -m "chore: rotate S3 credentials" && git push

# Then lotus-lake
cd /path/to/lotus-lake
git add -A && git commit -m "chore: rotate S3 credentials" && git push
```

### 5. Restart Services

Pods don't auto-reload secrets. Force restart:

```bash
kubectl rollout restart deploy/airbyte-server -n airbyte
kubectl rollout restart deploy/dagster-webserver -n lotus-lake
```

---

## SOPS Commands

```bash
# Decrypt and view
sops -d secret.yaml

# Edit in-place (decrypts, opens editor, re-encrypts)
sops secret.yaml

# Encrypt a plaintext file
sops -e -i secret.yaml

# Encrypt with specific key
sops -e --age age165ul5vra7q09yse7k0nharmt2jp0usw5tj2qwv2an9glyvxrr5ss5snxgj secret.yaml
```

---

## Troubleshooting

### S3 403 Forbidden

Credentials mismatch. Check:
1. SeaweedFS config has the identity
2. Service secret matches SeaweedFS exactly
3. Pods were restarted after secret update

### CNPG Backup Failing

```bash
kubectl logs -n lotus-lake -l cnpg.io/cluster=dagster-db --tail=50
```

Usually credential mismatch or bucket doesn't exist.

### Decryption Fails

```bash
# Check you have the age key
cat ~/.config/sops/age/keys.txt

# Verify the secret's recipient matches your key
grep recipient secret.yaml
```

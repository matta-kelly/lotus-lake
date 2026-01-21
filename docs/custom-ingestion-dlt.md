# Custom Ingestion with DLT

Build custom data pipelines using [DLT (Data Load Tool)](https://dlthub.com) for sources not supported by Airbyte.

---

## Why DLT?

| Use Case | Why DLT over Airbyte |
|----------|---------------------|
| **Custom APIs** | Source not in Airbyte catalog |
| **Complex auth** | OAuth flows, custom headers, token refresh |
| **Rate limiting** | Fine-grained control over request pacing |
| **Cost** | No Airbyte cloud costs, runs in existing infra |

---

## Directory Structure

```
orchestration/dlt/
├── discover-source.py          # Schema discovery tool
├── pipeline.py                 # Main runner with DAG integration
└── sources/
    └── {source}/
        ├── __init__.py         # Exports streams
        ├── client.py           # API client (auth, session, base requests)
        └── streams.py          # dlt resources (one per endpoint)

orchestration/dag/
├── sources/{source}/           # Discovered schemas (auto-generated)
│   └── {stream}.json
└── streams/{source}/           # Stream configs (manually curated)
    └── {stream}.json
```

**Separation of concerns:**

| File | Responsibility |
|------|---------------|
| `client.py` | Source-level: auth, base URL, session, rate limits |
| `streams.py` | Stream-level: endpoint, pagination, cursor field |
| `dag/sources/` | Available schemas (what the source can provide) |
| `dag/streams/` | Configured streams (what we actually sync) |

---

## DAG Directory Integration

DLT pipelines follow the same `{source}/{stream}` pattern as Airbyte for consistency.

### Schema Discovery

Run discovery to introspect a dlt source and write schemas:

```bash
cd orchestration/dlt
python discover-source.py odoo              # Discover all streams
python discover-source.py odoo orders       # Discover single stream
python discover-source.py odoo --sample-limit=50  # More samples for better inference
```

This writes JSON schemas to `dag/sources/{source}/{stream}.json`:

```json
{
  "stream": "orders",
  "options": {
    "sync_modes": ["incremental"],
    "destination_sync_modes": ["append", "merge"]
  },
  "defaults": {
    "primary_key": [["id"]],
    "cursor_field": ["write_date"]
  },
  "fields": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "write_date": {"type": "string", "format": "date-time"}
  }
}
```

### Stream Configuration

After discovery, create stream configs in `dag/streams/{source}/{stream}.json`:

```json
{
  "stream": "orders",
  "sync_mode": "incremental",
  "destination_sync_mode": "append",
  "primary_key": [["id"]],
  "cursor_field": ["write_date"],
  "selected": true,
  "fields": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "write_date": {"type": "string", "format": "date-time"}
  }
}
```

**Field filtering:** Only fields listed in `fields` are extracted. Remove fields you don't need to reduce data volume and improve performance.

### Running Pipelines

```bash
cd orchestration/dlt
python pipeline.py odoo orders              # Run single stream
python pipeline.py odoo orders order_lines  # Run multiple streams
python pipeline.py odoo --all               # Run all configured streams
python pipeline.py odoo orders --dry-run    # Preview without executing
```

The pipeline:
1. Loads stream config from `dag/streams/{source}/{stream}.json`
2. Applies field filter if config exists
3. Sets `max_table_nesting=0` to keep arrays inline (no child tables)
4. Writes parquet to S3 with Hive partitioning

### S3 Path Format

Output matches Airbyte's path structure for downstream compatibility:

```
s3://landing/raw/{namespace}/{stream}/year={YYYY}/month={MM}/day={DD}/*.parquet
```

Example:
```
s3://landing/raw/odoo/orders/year=2026/month=01/day=21/load_id_abc123.parquet
```

**Environment variables** (matches `lib.py` and `profiles.yml`):

| Variable | Purpose | Example |
|----------|---------|---------|
| `DLT_BUCKET_URL` | S3 bucket URL | `s3://landing/raw` |
| `S3_ENDPOINT` | S3 endpoint (SeaweedFS/MinIO) | `seaweedfs-s3.seaweedfs:8333` |
| `S3_ACCESS_KEY_ID` | S3 access key | From `lotus-lake-s3` identity |
| `S3_SECRET_ACCESS_KEY` | S3 secret key | From `lotus-lake-s3` identity |

For local dev, add to `.env`:
```bash
S3_ENDPOINT=minio.minio.svc.cluster.local:9000
S3_ACCESS_KEY_ID=<from lotus-lake-terraform-vars>
S3_SECRET_ACCESS_KEY=<from lotus-lake-terraform-vars>
```

In production, these are injected from the `lotus-lake-terraform-vars` secret (same `lotus-lake-s3` identity used by Airbyte).

---

## Adding a New Source

### 1. Create the client

```python
# sources/{source}/client.py
import os
import requests

class MyAPI:
    def __init__(self):
        self.base_url = os.environ["MY_API_URL"]
        self.api_key = os.environ["MY_API_KEY"]
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {self.api_key}"

    def get(self, endpoint: str, params: dict = None) -> list:
        response = self.session.get(f"{self.base_url}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()["data"]
```

### 2. Create streams

```python
# sources/{source}/streams.py
import dlt
from dlt.sources.incremental import Incremental
from .client import MyAPI

@dlt.resource(write_disposition="merge", primary_key="id")
def orders(
    updated_at: Incremental[str] = dlt.sources.incremental(
        "updated_at", initial_value="2020-01-01 00:00:00"
    ),
    limit: int = 100,
):
    """Orders stream with incremental loading."""
    api = MyAPI()
    offset = 0

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "since": updated_at.last_value,
        }
        batch = api.get("orders", params=params)

        if not batch:
            break

        yield batch

        if len(batch) < limit:
            break
        offset += limit
```

### 3. Export streams

```python
# sources/{source}/__init__.py
from .streams import orders

__all__ = ["orders"]
```

### 4. Discover and configure

```bash
# Discover available schemas
python discover-source.py my_source

# Review discovered schemas
cat ../dag/sources/my_source/orders.json

# Create stream config (copy from sources, edit fields as needed)
mkdir -p ../dag/streams/my_source
cp ../dag/sources/my_source/orders.json ../dag/streams/my_source/orders.json
# Edit to remove unwanted fields, set selected=true
```

### 5. Run the pipeline

```bash
python pipeline.py my_source orders
```

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| `@dlt.resource` | Decorator that makes a generator into a dlt stream |
| `write_disposition="merge"` | Upsert behavior based on primary key |
| `dlt.sources.incremental` | Automatic cursor/state tracking |
| `yield batch` | Yield dicts/lists; dlt handles parquet chunking |
| `dag/sources/` | Discovered schemas (what's available) |
| `dag/streams/` | Stream configs (what's selected) |

---

## Secrets

Secrets are read from environment variables. In production, use SOPS-encrypted secrets injected into the k8s CronJob environment.

Required env vars per source are documented in each `client.py`.

---

## Verification

After setting up a new dlt source:

1. **Discover schemas:**
   ```bash
   python discover-source.py my_source
   ls ../dag/sources/my_source/
   ```

2. **Create stream config:**
   ```bash
   mkdir -p ../dag/streams/my_source
   # Copy and edit from sources
   ```

3. **Test with dry-run:**
   ```bash
   python pipeline.py my_source orders --dry-run
   ```

4. **Run pipeline:**
   ```bash
   python pipeline.py my_source orders
   ```

5. **Verify S3 output:**
   ```bash
   aws s3 ls s3://landing/raw/my_source/orders/
   ```

6. **Check downstream compatibility:** Existing feeders/sensors should pick up the data since it uses the same path format as Airbyte.

---

## References

- [DLT Documentation](https://dlthub.com/docs/intro)
- [DLT Incremental Loading](https://dlthub.com/docs/general-usage/incremental-loading)
- [DLT + Dagster Integration](https://dlthub.com/docs/dlt-ecosystem/orchestrators/dagster)
- [DLT Filesystem Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem)

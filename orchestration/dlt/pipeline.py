#!/usr/bin/env python3
"""Main dlt pipeline runner with DAG directory integration.

Usage:
    python pipeline.py odoo orders           # Run single stream
    python pipeline.py odoo orders order_lines  # Run multiple streams
    python pipeline.py odoo --all            # Run all configured streams
"""
import argparse
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterator

env_file = Path(__file__).parent.parent.parent / ".env"
if env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(env_file)

import dlt


def get_dag_streams_path(source_name: str) -> Path:
    """Get the dag/streams/{source}/ directory path."""
    return Path(__file__).parent.parent / "dag" / "streams" / source_name


def load_stream_config(source_name: str, stream_name: str) -> dict | None:
    """Load stream configuration from dag/streams/{source}/{stream}.json."""
    config_path = get_dag_streams_path(source_name) / f"{stream_name}.json"
    if not config_path.exists():
        return None
    with open(config_path) as f:
        return json.load(f)


def get_configured_streams(source_name: str) -> list[str]:
    """Get all configured streams for a source from dag/streams/{source}/."""
    streams_dir = get_dag_streams_path(source_name)
    if not streams_dir.exists():
        return []
    streams = []
    for config_file in streams_dir.glob("*.json"):
        if config_file.name.startswith("_"):
            continue  # Skip _catalog.json and similar
        with open(config_file) as f:
            config = json.load(f)
            if config.get("selected", True):
                streams.append(config_file.stem)
    return streams


def field_filter(selected_fields: list[str]):
    """Create a dlt transformer that filters record fields."""

    @dlt.transformer
    def _filter(records: Iterator[dict]) -> Iterator[dict]:
        for record in records:
            if isinstance(record, dict):
                yield {k: v for k, v in record.items() if k in selected_fields}
            else:
                yield record

    return _filter


def get_resource(source_name: str, stream_name: str) -> Any:
    """Dynamically import and get a resource function from a source module."""
    try:
        source_module = importlib.import_module(f"orchestration.dlt.sources.{source_name}")
    except ImportError as e:
        raise ImportError(f"Could not import source 'orchestration.dlt.sources.{source_name}': {e}")

    if not hasattr(source_module, stream_name):
        available = [
            name
            for name in dir(source_module)
            if not name.startswith("_") and callable(getattr(source_module, name))
        ]
        raise ValueError(
            f"Stream '{stream_name}' not found in source '{source_name}'. "
            f"Available: {', '.join(available)}"
        )

    return getattr(source_module, stream_name)


def create_pipeline(source_name: str) -> dlt.Pipeline:
    """Create a dlt pipeline with S3 filesystem destination.

    Environment variables (matches lib.py/profiles.yml):
        DLT_BUCKET_URL: S3 bucket URL (default: s3://landing/raw)
        S3_ENDPOINT: S3 endpoint for non-AWS (SeaweedFS, MinIO)
        S3_ACCESS_KEY_ID: S3 access key
        S3_SECRET_ACCESS_KEY: S3 secret key

    File size config (matches Airbyte destinations.tf block_size_mb=128):
        - file_max_bytes: 128MB per parquet file before rotation
    """
    # Configure file rotation to match Airbyte's 128MB block_size_mb
    dlt.config["normalize.data_writer.file_max_bytes"] = 134217728  # 128MB

    bucket_url = os.environ.get("DLT_BUCKET_URL", "s3://landing/raw")

    # Build credentials config for non-AWS S3 (SeaweedFS, MinIO)
    credentials = None
    endpoint_url = os.environ.get("S3_ENDPOINT")
    if endpoint_url:
        # Ensure protocol prefix for s3fs/dlt
        if not endpoint_url.startswith(("http://", "https://")):
            endpoint_url = f"http://{endpoint_url}"
        credentials = {
            "aws_access_key_id": os.environ.get("S3_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.environ.get("S3_SECRET_ACCESS_KEY"),
            "endpoint_url": endpoint_url,
        }

    # Layout with zero-padded month/day to match Airbyte and lib.py expectations
    # dlt placeholders: {YYYY}, {MM}, {DD} are zero-padded
    # e.g., year=2026/month=01/day=09/{load_id}.{file_id}.parquet
    layout = "{table_name}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"

    return dlt.pipeline(
        pipeline_name=source_name,
        destination=dlt.destinations.filesystem(
            bucket_url=bucket_url,
            layout=layout,
            credentials=credentials,
        ),
        dataset_name=source_name,
    )


def run_stream(source_name: str, stream_name: str, dry_run: bool = False) -> dict:
    """Run a single stream with optional field filtering based on stream config."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Running {source_name}/{stream_name}")

    # Load stream config
    config = load_stream_config(source_name, stream_name)
    if config:
        print(f"  Loaded config from dag/streams/{source_name}/{stream_name}.json")
        if not config.get("selected", True):
            print(f"  Stream is not selected, skipping")
            return {"status": "skipped", "reason": "not selected"}
    else:
        print(f"  No stream config found, using defaults")

    # Get selected fields from config
    selected_fields = None
    if config and "fields" in config:
        selected_fields = list(config["fields"].keys())
        print(f"  Filtering to {len(selected_fields)} fields")

    # Get initial_value from config (required for incremental loading)
    if not config or "initial_value" not in config:
        raise ValueError(f"Missing 'initial_value' in stream config for {source_name}/{stream_name}")
    initial_value = config["initial_value"]

    if dry_run:
        print(f"  Would run with fields: {selected_fields or 'all'}")
        print(f"  Initial value: {initial_value}")
        return {"status": "dry_run", "fields": selected_fields, "initial_value": initial_value}

    # Get the resource - dlt manages incremental state automatically
    resource_func = get_resource(source_name, stream_name)
    resource = resource_func()

    # Apply field filter if configured
    if selected_fields:
        resource = resource | field_filter(selected_fields)

    # Keep nested arrays inline (don't create child tables like orders__tags)
    resource.max_table_nesting = 0

    # Create and run pipeline
    pipeline = create_pipeline(source_name)
    load_info = pipeline.run(resource, table_name=stream_name, loader_file_format="parquet")

    print(f"  {load_info}")
    return {"status": "success", "load_info": str(load_info)}


def run_streams(
    source_name: str, stream_names: list[str], dry_run: bool = False
) -> dict[str, dict]:
    """Run multiple streams for a source."""
    results = {}
    for stream_name in stream_names:
        try:
            results[stream_name] = run_stream(source_name, stream_name, dry_run)
        except Exception as e:
            print(f"  Error: {e}")
            results[stream_name] = {"status": "error", "error": str(e)}
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run dlt pipeline with DAG directory integration"
    )
    parser.add_argument("source", help="Source name (e.g., 'odoo')")
    parser.add_argument("streams", nargs="*", help="Stream names to run")
    parser.add_argument(
        "--all",
        action="store_true",
        dest="run_all",
        help="Run all configured streams from dag/streams/{source}/",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be run without executing",
    )

    args = parser.parse_args()

    if args.run_all:
        streams = get_configured_streams(args.source)
        if not streams:
            print(f"No configured streams found in dag/streams/{args.source}/")
            sys.exit(1)
        print(f"Running all {len(streams)} configured streams: {', '.join(streams)}")
    elif args.streams:
        streams = args.streams
    else:
        parser.error("Specify stream names or use --all")

    results = run_streams(args.source, streams, args.dry_run)

    # Summary
    print("\n" + "=" * 50)
    print("Summary:")
    for stream, result in results.items():
        status = result.get("status", "unknown")
        print(f"  {stream}: {status}")

    # Exit with error if any failed
    if any(r.get("status") == "error" for r in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()

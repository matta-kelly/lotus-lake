#!/usr/bin/env python3
"""Discover dlt source schemas and write to dag/sources/{source}/{stream}.json.

Usage:
    python discover-source.py odoo
    python discover-source.py odoo orders  # Single stream only
"""
import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

env_file = Path(__file__).parent.parent.parent / ".env"
if env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(env_file)

import dlt


def get_dag_sources_path(source_name: str) -> Path:
    """Get the dag/sources/{source}/ directory path."""
    return Path(__file__).parent.parent / "dag" / "sources" / source_name


def get_resources(source_module) -> dict[str, Any]:
    """Extract all dlt resources from a source module."""
    resources = {}
    for name in dir(source_module):
        obj = getattr(source_module, name)
        # Check if it's a dlt resource (callable with dlt metadata)
        if callable(obj) and hasattr(obj, "_dlt_resource"):
            resources[name] = obj
        # Also check for resources exported in __all__
        elif name in getattr(source_module, "__all__", []):
            if callable(obj):
                resources[name] = obj
    return resources


def extract_cursor_field(resource_func) -> list[str] | None:
    """Extract cursor field from incremental parameter defaults."""
    import inspect

    sig = inspect.signature(resource_func)
    for param in sig.parameters.values():
        if param.default is not inspect.Parameter.empty:
            # Check if it's an Incremental object
            if hasattr(param.default, "cursor_path"):
                return [param.default.cursor_path]
    return None


def infer_schema_from_sample(records: list[dict]) -> dict[str, dict]:
    """Infer JSON schema fields from sample records."""
    if not records:
        return {}

    fields = {}
    # Aggregate field types across all records
    for record in records:
        for key, value in record.items():
            if key not in fields:
                fields[key] = infer_field_type(value)
    return fields


def infer_field_type(value: Any) -> dict:
    """Infer JSON schema type from a Python value."""
    if value is None:
        return {"type": "string"}  # Default to string for null
    elif isinstance(value, bool):
        return {"type": "boolean"}
    elif isinstance(value, int):
        return {"type": "integer"}
    elif isinstance(value, float):
        return {"type": "number"}
    elif isinstance(value, str):
        # Check for date-time format
        if len(value) >= 19 and "T" in value or " " in value:
            try:
                from datetime import datetime

                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return {"type": "string", "format": "date-time"}
            except ValueError:
                pass
        return {"type": "string"}
    elif isinstance(value, list):
        if value:
            item_type = infer_field_type(value[0])
            return {"type": "array", "items": item_type}
        return {"type": "array", "items": {"type": "string"}}
    elif isinstance(value, dict):
        properties = {k: infer_field_type(v) for k, v in value.items()}
        return {"type": "object", "properties": properties}
    else:
        return {"type": "string"}


def discover_resource(
    source_name: str, stream_name: str, resource_func, sample_limit: int = 10
) -> dict:
    """Discover schema for a single dlt resource."""
    print(f"  Discovering {stream_name}...")

    # Get resource metadata
    resource = resource_func()

    # Extract primary key
    primary_key = None
    if hasattr(resource, "_pipe"):
        pk = getattr(resource._pipe, "primary_key", None)
        if pk:
            primary_key = [[pk]] if isinstance(pk, str) else [[k] for k in pk]

    # Extract cursor field from function signature
    cursor_field = extract_cursor_field(resource_func)

    # Determine sync modes based on cursor field presence
    if cursor_field:
        sync_modes = ["incremental"]
        destination_sync_modes = ["append", "merge"]
    else:
        sync_modes = ["full_refresh"]
        destination_sync_modes = ["overwrite", "append"]

    # Sample records to infer schema
    print(f"    Sampling {sample_limit} records...")
    sample_records = []
    try:
        # Create a new resource with limit for sampling
        sample_resource = resource_func(limit=sample_limit)
        for batch in sample_resource:
            if isinstance(batch, list):
                sample_records.extend(batch)
            else:
                sample_records.append(batch)
            if len(sample_records) >= sample_limit:
                break
        sample_records = sample_records[:sample_limit]
    except TypeError:
        # Resource doesn't accept limit parameter, try without
        try:
            sample_resource = resource_func()
            count = 0
            for batch in sample_resource:
                if isinstance(batch, list):
                    sample_records.extend(batch)
                else:
                    sample_records.append(batch)
                count += 1
                if len(sample_records) >= sample_limit or count >= 3:
                    break
            sample_records = sample_records[:sample_limit]
        except Exception as e:
            print(f"    Warning: Could not sample records: {e}")
    except Exception as e:
        print(f"    Warning: Could not sample records: {e}")

    # Infer schema from samples
    fields = infer_schema_from_sample(sample_records)
    print(f"    Inferred {len(fields)} fields from {len(sample_records)} records")

    schema = {
        "stream": stream_name,
        "options": {
            "sync_modes": sync_modes,
            "destination_sync_modes": destination_sync_modes,
        },
        "defaults": {},
        "fields": fields,
    }

    if primary_key:
        schema["defaults"]["primary_key"] = primary_key
    if cursor_field:
        schema["defaults"]["cursor_field"] = cursor_field

    return schema


def discover_source(source_name: str, stream_filter: str | None = None, sample_limit: int = 10):
    """Discover all streams for a dlt source and write schemas."""
    print(f"Discovering source: {source_name}")

    # Import the source module
    try:
        source_module = importlib.import_module(f"sources.{source_name}")
    except ImportError as e:
        print(f"Error: Could not import source 'sources.{source_name}': {e}")
        sys.exit(1)

    # Get all resources
    resources = get_resources(source_module)
    if not resources:
        print(f"Error: No dlt resources found in sources.{source_name}")
        sys.exit(1)

    # Filter to specific stream if requested
    if stream_filter:
        if stream_filter not in resources:
            print(f"Error: Stream '{stream_filter}' not found in source '{source_name}'")
            print(f"Available streams: {', '.join(resources.keys())}")
            sys.exit(1)
        resources = {stream_filter: resources[stream_filter]}

    print(f"Found {len(resources)} stream(s): {', '.join(resources.keys())}")

    # Create output directory
    output_dir = get_dag_sources_path(source_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Discover each resource
    for stream_name, resource_func in resources.items():
        try:
            schema = discover_resource(source_name, stream_name, resource_func, sample_limit)

            # Write schema to file
            output_path = output_dir / f"{stream_name}.json"
            with open(output_path, "w") as f:
                json.dump(schema, f, indent=2)
            print(f"    Wrote: {output_path}")

        except Exception as e:
            print(f"    Error discovering {stream_name}: {e}")
            continue

    print(f"\nDiscovery complete. Schemas written to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Discover dlt source schemas and write to dag/sources/"
    )
    parser.add_argument("source", help="Source name (e.g., 'odoo')")
    parser.add_argument("stream", nargs="?", help="Optional: specific stream to discover")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="Number of records to sample for schema inference (default: 10)",
    )

    args = parser.parse_args()
    discover_source(args.source, args.stream, args.sample_limit)


if __name__ == "__main__":
    main()

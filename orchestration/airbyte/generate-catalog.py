#!/usr/bin/env python3
"""
Generate Airbyte-compatible catalogs from stream definitions.

Outputs _catalog.json alongside stream configs in assets/streams/<source>/.

Usage:
  python generate-catalog.py
"""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent  # lotus-lake/
STREAMS_DIR = REPO_ROOT / "orchestration" / "assets" / "streams"
SOURCES_DIR = REPO_ROOT / "orchestration" / "assets" / "sources"


def load_json(path):
    with open(path) as f:
        return json.load(f)


def get_configured_sources():
    """Get sources that have stream definitions."""
    if not STREAMS_DIR.exists():
        return []
    return sorted([d.name for d in STREAMS_DIR.iterdir() if d.is_dir() and not d.name.startswith('.')])


def fields_to_json_schema(fields):
    """Convert clean fields format back to JSON schema properties."""
    properties = {}
    for name, field in fields.items():
        prop = {}
        field_type = field.get("type", "string")
        prop["type"] = ["null", field_type]

        if "description" in field:
            prop["description"] = field["description"]
        if "format" in field:
            prop["format"] = field["format"]

        if field_type == "object" and "properties" in field:
            prop["properties"] = fields_to_json_schema(field["properties"])

        if field_type == "array" and "items" in field:
            items = field["items"]
            if isinstance(items, dict) and all(isinstance(v, dict) for v in items.values()):
                prop["items"] = {
                    "type": ["null", "object"],
                    "properties": fields_to_json_schema(items)
                }
            else:
                prop["items"] = items

        properties[name] = prop

    return properties


def build_airbyte_stream(stream_config, source_schema):
    """Build Airbyte stream object from clean config and source schema."""
    stream_name = stream_config["stream"]

    if source_schema:
        full_fields = source_schema.get("fields", {})
        options = source_schema.get("options", {})
        defaults = source_schema.get("defaults", {})
        supported_sync_modes = options.get("sync_modes", [])
        default_cursor_field = defaults.get("cursor_field", [])
        source_defined_primary_key = defaults.get("primary_key", [])
        source_defined_cursor = bool(default_cursor_field)
    else:
        full_fields = stream_config.get("fields", {})
        supported_sync_modes = []
        source_defined_cursor = bool(stream_config.get("cursor_field"))
        default_cursor_field = stream_config.get("cursor_field", [])
        source_defined_primary_key = stream_config.get("primary_key", [])

    properties = fields_to_json_schema(full_fields)
    json_schema = {
        "type": json.dumps(["null", "object"]),
        "properties": json.dumps(properties),
        "additionalProperties": "true"
    }

    # Use camelCase for selectedFields to match Airbyte's API response format
    selected_fields = [
        {"fieldPath": [name]}
        for name in stream_config.get("fields", {}).keys()
    ]

    return {
        "stream": {
            "name": stream_name,
            "supportedSyncModes": supported_sync_modes,
            "json_schema": json_schema,
            "default_cursor_field": default_cursor_field,
            "source_defined_cursor": source_defined_cursor,
            "source_defined_primary_key": source_defined_primary_key
        },
        # Use camelCase keys to match Airbyte's API response format
        # This prevents drift detection from seeing snake_case vs camelCase differences
        "config": {
            "syncMode": stream_config.get("sync_mode", "full_refresh"),
            "cursorField": stream_config.get("cursor_field", []),
            "destinationSyncMode": stream_config.get("destination_sync_mode", "overwrite"),
            "primaryKey": stream_config.get("primary_key", []),
            "aliasName": stream_name,
            "selected": stream_config.get("selected", True),
            "suggested": False,
            "includeFiles": False,
            "fieldSelectionEnabled": True,
            "selectedFields": selected_fields,
            "hashedFields": [],
            "mappers": []
        }
    }


def generate_catalog(source_name):
    """Generate Airbyte catalog for a single source."""
    streams_path = STREAMS_DIR / source_name
    sources_path = SOURCES_DIR / source_name

    if not streams_path.exists():
        print(f"No streams defined at {streams_path}")
        return

    # Get stream configs (exclude _catalog.json)
    stream_files = [f for f in streams_path.glob("*.json") if not f.name.startswith("_")]
    if not stream_files:
        print(f"No JSON files in {streams_path}")
        return

    print(f"Generating catalog for {source_name}")

    catalog = {"streams": []}

    for stream_file in sorted(stream_files):
        stream_config = load_json(stream_file)
        stream_name = stream_config.get("stream", stream_file.stem)

        source_schema = None
        source_file = sources_path / f"{stream_name}.json"
        if source_file.exists():
            source_schema = load_json(source_file)

        airbyte_stream = build_airbyte_stream(stream_config, source_schema)
        catalog["streams"].append(airbyte_stream)

        field_count = len(stream_config.get("fields", {}))
        print(f"  + {stream_name} ({field_count} selected fields)")

    # Output to _catalog.json in the same directory as stream configs
    output_file = streams_path / "_catalog.json"
    with open(output_file, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"  -> {output_file.relative_to(REPO_ROOT)} ({len(catalog['streams'])} streams)")


def main():
    sources = get_configured_sources()

    if not sources:
        print(f"No stream definitions found in {STREAMS_DIR}")
        sys.exit(1)

    print(f"Generating catalogs for {len(sources)} sources...\n")

    for source in sources:
        generate_catalog(source)
        print()


if __name__ == "__main__":
    main()

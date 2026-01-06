#!/usr/bin/env python3
"""
Discover available streams from an Airbyte source.

Usage:
  python discover-source.py           # Interactive: list sources, pick one
  python discover-source.py shopify   # Direct: discover specific source
"""
import json
import os
import sys
import urllib.request
import base64

from pathlib import Path

AIRBYTE_URL = os.environ.get("AIRBYTE_URL", "http://localhost:8080/api/v1")
WORKSPACE_ID = os.environ["TF_VAR_workspace_id"]
USERNAME = os.environ["TF_VAR_airbyte_username"]
PASSWORD = os.environ["TF_VAR_airbyte_password"]

REPO_ROOT = Path(__file__).parent.parent.parent.parent  # lotus-lake/
OUTPUT_DIR = REPO_ROOT / "orchestration" / "assets" / "sources"


def api_call(endpoint, payload):
    credentials = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/{endpoint}",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {credentials}"
        }
    )
    with urllib.request.urlopen(req) as response:
        return json.load(response)


def get_all_sources():
    """Get all configured sources from Airbyte."""
    sources = api_call("sources/list", {"workspaceId": WORKSPACE_ID})["sources"]
    return sources


def simplify_type(type_def):
    if isinstance(type_def, list):
        non_null = [t for t in type_def if t != "null"]
        return non_null[0] if non_null else "null"
    return type_def


def parse_schema_field(field_def):
    if not isinstance(field_def, dict):
        return {"type": str(field_def)}

    result = {"type": simplify_type(field_def.get("type", "unknown"))}

    if "description" in field_def:
        result["description"] = field_def["description"]
    if "format" in field_def:
        result["format"] = field_def["format"]

    if result["type"] == "array" and "items" in field_def:
        items = field_def["items"]
        if isinstance(items, dict):
            if "properties" in items:
                result["items"] = parse_schema_properties(items["properties"])
            else:
                result["items"] = parse_schema_field(items)

    if result["type"] == "object" and "properties" in field_def:
        result["properties"] = parse_schema_properties(field_def["properties"])

    return result


def parse_schema_properties(properties):
    if isinstance(properties, str):
        properties = json.loads(properties)
    return {name: parse_schema_field(field) for name, field in properties.items()}


def discover_source(source):
    """Run discovery for a source and save schemas."""
    source_name = source["name"].lower().split()[0]
    print(f"\nDiscovering {source['name']}...")

    result = api_call("sources/discover_schema", {"sourceId": source["sourceId"]})
    catalog = result["catalog"]

    output_path = OUTPUT_DIR / source_name
    output_path.mkdir(parents=True, exist_ok=True)

    for stream_data in catalog["streams"]:
        stream = stream_data["stream"]
        stream_name = stream["name"]
        json_schema = stream.get("jsonSchema", {})
        properties = json_schema.get("properties", {})

        supported_sync_modes = stream.get("supportedSyncModes", [])

        schema = {
            "stream": stream_name,
            "options": {
                "sync_modes": supported_sync_modes,
                "destination_sync_modes": ["overwrite", "append", "append_dedup"]
            },
            "defaults": {
                "primary_key": stream.get("sourceDefinedPrimaryKey", []),
                "cursor_field": stream.get("defaultCursorField", [])
            },
            "fields": parse_schema_properties(properties)
        }

        file_path = output_path / f"{stream_name}.json"
        with open(file_path, "w") as f:
            json.dump(schema, f, indent=2)

        field_count = len(schema["fields"])
        print(f"  {stream_name} ({field_count} fields)")

    print(f"\nSaved {len(catalog['streams'])} streams to {output_path}/")


def interactive_select():
    """Show list of sources, let user pick one."""
    sources = get_all_sources()

    print("Available sources:\n")
    for i, source in enumerate(sources, 1):
        print(f"  {i}. {source['name']}")

    print()
    choice = input("Select source (number or name): ").strip()

    # Try as number
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(sources):
            return sources[idx]
    except ValueError:
        pass

    # Try as name
    choice_lower = choice.lower()
    for source in sources:
        if choice_lower in source["name"].lower():
            return source

    print(f"Source '{choice}' not found")
    sys.exit(1)


def main():
    # No args = interactive
    if len(sys.argv) == 1:
        source = interactive_select()
        discover_source(source)
        return

    # With arg = direct
    source_name = sys.argv[1]
    sources = get_all_sources()

    for source in sources:
        if source_name.lower() in source["name"].lower():
            discover_source(source)
            return

    print(f"Source '{source_name}' not found")
    sys.exit(1)


if __name__ == "__main__":
    main()

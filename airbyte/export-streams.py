#!/usr/bin/env python3
"""
Export all Airbyte connection streams to Terraform-compatible JSON.

Usage: python export-streams.py
Outputs to streams/<source_name>.json for each connection.
"""
import json
import os
import re
import urllib.request
import base64

AIRBYTE_URL = os.environ.get("AIRBYTE_URL", "http://localhost:8080/api/v1")
WORKSPACE_ID = os.environ["TF_VAR_workspace_id"]
USERNAME = os.environ["TF_VAR_airbyte_username"]
PASSWORD = os.environ["TF_VAR_airbyte_password"]
OUTPUT_DIR = "streams"

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

def snake_case(name):
    return re.sub(r'[^a-z0-9]', '', name.split()[0].lower())

def transform_catalog(catalog):
    for s in catalog["streams"]:
        st = s["stream"]
        
        # Convert json_schema: all values must be JSON strings
        if "jsonSchema" in st:
            schema = st.pop("jsonSchema")
            st["json_schema"] = {k: json.dumps(v) for k, v in schema.items()}
        
        # Stream key renames
        for old, new in [
            ("defaultCursorField", "default_cursor_field"),
            ("sourceDefinedCursor", "source_defined_cursor"),
            ("sourceDefinedPrimaryKey", "source_defined_primary_key")
        ]:
            if old in st:
                st[new] = st.pop(old)
        
        # Config key renames
        c = s["config"]
        for old, new in [
            ("syncMode", "sync_mode"),
            ("destinationSyncMode", "destination_sync_mode"),
            ("cursorField", "cursor_field"),
            ("primaryKey", "primary_key"),
            ("aliasName", "alias_name"),
            ("fieldSelectionEnabled", "field_selection_enabled"),
            ("selectedFields", "selected_fields")
        ]:
            if old in c:
                c[new] = c.pop(old)
        
        # Transform selectedFields items: fieldPath -> field_path
        if "selected_fields" in c:
            c["selected_fields"] = [
                {"field_path": f["fieldPath"]} if "fieldPath" in f else f
                for f in c["selected_fields"]
            ]
    
    return catalog

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    connections = api_call("connections/list", {"workspaceId": WORKSPACE_ID})["connections"]
    print(f"Found {len(connections)} connections")
    
    for conn in connections:
        name = snake_case(conn["name"])
        output_file = f"{OUTPUT_DIR}/{name}.json"
        
        details = api_call("connections/get", {"connectionId": conn["connectionId"]})
        catalog = transform_catalog(details["syncCatalog"])
        
        with open(output_file, "w") as f:
            json.dump(catalog, f, indent=2)
        
        selected = sum(1 for s in catalog["streams"] if s["config"].get("selected"))
        print(f"  {conn['name']} → {output_file} ({selected} selected streams)")

if __name__ == "__main__":
    main()
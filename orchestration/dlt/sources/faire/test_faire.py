#!/usr/bin/env python3
"""Quick test for Faire API connection and order_lines stream."""
import sys
import os
from pathlib import Path

# Load .env from lotus-lake root (5 levels up: faire -> sources -> dlt -> orchestration -> lotus-lake)
env_file = Path(__file__).resolve().parent.parent.parent.parent.parent / ".env"
print(f"Loading env from: {env_file}")
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())
else:
    print(f"WARNING: .env not found at {env_file}")

from client import FaireAPI

def test_connection():
    """Test basic API connection."""
    print("Testing Faire API connection...")
    api = FaireAPI()

    try:
        profile = api.test_connection()
        print(f"SUCCESS! Connected to brand: {profile.get('name', profile)}")
        return True
    except Exception as e:
        print(f"FAILED: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return False

def test_orders(updated_at_min: str = "2026-01-29T00:00:00.000Z", limit: int = 10):
    """Fetch a small sample of orders."""
    print(f"\nFetching orders since {updated_at_min} (limit={limit})...")
    api = FaireAPI()

    try:
        response = api.get_orders(updated_at_min=updated_at_min, limit=limit)
    except Exception as e:
        print(f"FAILED: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response body: {e.response.text}")
        return None

    orders = response.get("orders", [])
    print(f"Got {len(orders)} orders")

    # Show order structure
    for i, order in enumerate(orders[:3]):  # Show max 3
        print(f"\n--- Order {i+1}: {order.get('id')} ---")
        print(f"  state: {order.get('state')}")
        print(f"  created_at: {order.get('created_at')}")
        print(f"  updated_at: {order.get('updated_at')}")

        items = order.get("items", [])
        print(f"  items: {len(items)}")
        for j, item in enumerate(items[:2]):  # Show max 2 items per order
            print(f"    [{j}] {item.get('product_name')} - qty: {item.get('quantity')} - sku: {item.get('sku')}")

    return orders

if __name__ == "__main__":
    if not test_connection():
        sys.exit(1)

    test_orders()

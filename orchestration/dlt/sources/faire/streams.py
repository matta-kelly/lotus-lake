"""Faire dlt resources - each function is a stream.

Order items are embedded within orders in the Faire API, so we fetch orders
and extract/flatten the items array, attaching order-level context to each item.

API Docs: https://faire.github.io/external-api-v2-docs/
"""
import dlt
from .client import FaireAPI


@dlt.resource(write_disposition="merge", primary_key="id")
def order_lines(
    updated_at=dlt.sources.incremental(
        "order_updated_at",
        initial_value="2020-01-01T00:00:00.000Z",
    ),
    limit: int = 50,
    log=None,
):
    """Order line items stream with incremental loading.

    Fetches orders from Faire API and extracts/flattens the embedded items array.
    Each item is enriched with order-level context (order_id, order dates, customer info).

    Args:
        updated_at: Incremental cursor tracking order_updated_at (ISO 8601)
        limit: Orders per page (default 50, range 10-50)
        log: Optional Dagster logger

    Yields:
        Batches of order line item dicts
    """
    def _log(msg):
        if log:
            log.info(msg)
        else:
            print(msg)

    api = FaireAPI()
    page = 1
    total_orders = 0
    total_items = 0

    # Capture cursor ONCE before pagination loop - it changes as we yield records
    cursor = updated_at.last_value
    _log(f"[order_lines] cursor={cursor}")

    while True:
        _log(f"[order_lines] fetching page={page}")
        response = api.get_orders(
            updated_at_min=cursor,
            page=page,
            limit=limit,
        )

        orders = response.get("orders", [])
        if not orders:
            _log(f"[order_lines] no more orders. Total: {total_orders} orders, {total_items} items")
            break

        batch_orders = len(orders)
        total_orders += batch_orders

        # Extract and flatten order items
        items_batch = []
        for order in orders:
            order_id = order.get("id")
            order_created_at = order.get("created_at")
            order_updated_at = order.get("updated_at")
            order_state = order.get("state")

            # Extract address info for customer context
            address = order.get("address", {})
            retailer_name = address.get("name")
            retailer_city = address.get("city")
            retailer_state = address.get("state")
            retailer_country = address.get("country")

            # Extract each item and enrich with order context
            for item in order.get("items", []):
                enriched_item = {
                    # Item-level fields
                    "id": item.get("id"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "product_id": item.get("product_id"),
                    "variant_id": item.get("variant_id"),
                    "quantity": item.get("quantity"),
                    "sku": item.get("sku"),
                    "product_name": item.get("product_name"),
                    "variant_name": item.get("variant_name"),
                    "includes_tester": item.get("includes_tester"),
                    "state": item.get("state"),

                    # Price fields (v2 structure)
                    "price_cents": item.get("price_cents"),
                    "price_amount_minor": (
                        item.get("price", {}).get("amount_minor")
                        if item.get("price") else None
                    ),
                    "price_currency": (
                        item.get("price", {}).get("currency")
                        if item.get("price") else None
                    ),

                    # Order-level context (for warehousing without separate orders table)
                    "order_id": order_id,
                    "order_created_at": order_created_at,
                    "order_updated_at": order_updated_at,
                    "order_state": order_state,

                    # Retailer/customer context
                    "retailer_name": retailer_name,
                    "retailer_city": retailer_city,
                    "retailer_state": retailer_state,
                    "retailer_country": retailer_country,
                }
                items_batch.append(enriched_item)

        batch_items = len(items_batch)
        total_items += batch_items
        _log(f"[order_lines] page={page} got {batch_orders} orders, {batch_items} items (total: {total_items})")

        if items_batch:
            yield items_batch

        # Check if we've reached the last page
        if batch_orders < limit:
            _log(f"[order_lines] last page. Total: {total_orders} orders, {total_items} items")
            break

        page += 1

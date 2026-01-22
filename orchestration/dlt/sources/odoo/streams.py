"""Odoo dlt resources - each function is a stream."""
import dlt
from .client import OdooAPI


@dlt.resource(write_disposition="merge", primary_key="id")
def orders(
    updated_at=dlt.sources.incremental("write_date", initial_value="2020-01-01 00:00:00"),
    limit: int = 100,
):
    """Sales orders stream with incremental loading."""
    api = OdooAPI()
    offset = 0
    total_records = 0

    print(f"[orders] cursor={updated_at.last_value}")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "last_sync_date": updated_at.last_value,
        }
        print(f"[orders] fetching offset={offset} limit={limit}")
        batch = api.get("api/sales", params=params)

        if not batch:
            print(f"[orders] empty batch, done")
            break

        batch_size = len(batch)
        total_records += batch_size
        print(f"[orders] got {batch_size} records (total: {total_records})")

        yield batch

        if batch_size < limit:
            break
        offset += limit

    print(f"[orders] DONE: {total_records} records extracted")


@dlt.resource(write_disposition="merge", primary_key="id")
def order_lines(
    updated_at=dlt.sources.incremental("write_date", initial_value="2020-01-01 00:00:00"),
    limit: int = 1000,
):
    """Sale order lines stream with incremental loading."""
    api = OdooAPI()
    offset = 0
    total_records = 0

    print(f"[order_lines] cursor={updated_at.last_value}")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "start_date": updated_at.last_value,
        }
        print(f"[order_lines] fetching offset={offset} limit={limit}")
        batch = api.get("api/sale_order_lines", params=params)

        if not batch:
            print(f"[order_lines] empty batch, done")
            break

        batch_size = len(batch)
        total_records += batch_size
        print(f"[order_lines] got {batch_size} records (total: {total_records})")

        yield batch

        if batch_size < limit:
            break
        offset += limit

    print(f"[order_lines] DONE: {total_records} records extracted")

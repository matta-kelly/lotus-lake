"""Odoo dlt resources - each function is a stream."""
import dlt
from .client import OdooAPI


@dlt.resource(write_disposition="merge", primary_key="id")
def orders(
    initial_value: str,
    limit: int = 100,
):
    """Sales orders stream with incremental loading."""
    updated_at = dlt.sources.incremental("write_date", initial_value=initial_value)
    api = OdooAPI()
    offset = 0

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "last_sync_date": updated_at.last_value,
        }
        batch = api.get("api/sales", params=params)

        if not batch:
            break

        yield batch

        if len(batch) < limit:
            break
        offset += limit


@dlt.resource(write_disposition="merge", primary_key="id")
def order_lines(
    initial_value: str,
    limit: int = 1000,
):
    """Sale order lines stream with incremental loading."""
    updated_at = dlt.sources.incremental("write_date", initial_value=initial_value)
    api = OdooAPI()
    offset = 0

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "start_date": updated_at.last_value,
        }
        batch = api.get("api/sale_order_lines", params=params)

        if not batch:
            break

        yield batch

        if len(batch) < limit:
            break
        offset += limit

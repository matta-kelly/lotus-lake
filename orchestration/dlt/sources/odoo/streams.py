"""Odoo dlt resources - each function is a stream."""
import dlt
from .client import OdooAPI


@dlt.resource(write_disposition="merge", primary_key="id")
def orders(
    updated_at=dlt.sources.incremental("write_date", initial_value="2020-01-01 00:00:00"),
    limit: int = 100,
    log=None,
):
    """Sales orders stream with incremental loading."""
    def _log(msg):
        if log:
            log.info(msg)
        else:
            print(msg)

    api = OdooAPI()
    offset = 0
    total_records = 0

    # Capture cursor ONCE - it changes as we yield records
    cursor = updated_at.last_value
    _log(f"[orders] cursor={cursor}")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "last_sync_date": cursor,
        }
        batch = api.get("api/sales", params=params)

        if not batch:
            _log(f"[orders] empty batch, done. Total: {total_records}")
            break

        batch_size = len(batch)
        total_records += batch_size
        _log(f"[orders] offset={offset} got {batch_size} (total: {total_records})")

        yield batch

        if batch_size < limit:
            _log(f"[orders] last batch. Total: {total_records}")
            break
        offset += limit


@dlt.resource(write_disposition="merge", primary_key="id")
def order_lines(
    updated_at=dlt.sources.incremental("write_date", initial_value="2020-01-01 00:00:00"),
    limit: int = 1000,
    log=None,
):
    """Sale order lines stream with incremental loading."""
    def _log(msg):
        if log:
            log.info(msg)
        else:
            print(msg)

    api = OdooAPI()
    offset = 0
    total_records = 0

    # Capture cursor ONCE - it changes as we yield records
    cursor = updated_at.last_value
    _log(f"[order_lines] cursor={cursor}")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "start_date": cursor,
        }
        batch = api.get("api/sale_order_lines", params=params)

        if not batch:
            _log(f"[order_lines] empty batch, done. Total: {total_records}")
            break

        batch_size = len(batch)
        total_records += batch_size
        _log(f"[order_lines] offset={offset} got {batch_size} (total: {total_records})")

        yield batch

        if batch_size < limit:
            _log(f"[order_lines] last batch. Total: {total_records}")
            break
        offset += limit

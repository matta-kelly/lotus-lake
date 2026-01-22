"""Odoo dlt resources - each function is a stream."""
import logging
import dlt
from .client import OdooAPI

logger = logging.getLogger(__name__)


@dlt.resource(write_disposition="merge", primary_key="id")
def orders(
    updated_at=dlt.sources.incremental("write_date", initial_value="2020-01-01 00:00:00"),
    limit: int = 100,
):
    """Sales orders stream with incremental loading."""
    api = OdooAPI()
    offset = 0
    total_records = 0

    logger.info(f"[orders] Starting extraction, cursor: {updated_at.last_value}")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "last_sync_date": updated_at.last_value,
        }
        logger.info(f"[orders] Fetching batch: offset={offset}, limit={limit}")
        batch = api.get("api/sales", params=params)

        if not batch:
            logger.info(f"[orders] Empty batch received, ending extraction")
            break

        batch_size = len(batch)
        total_records += batch_size
        logger.info(f"[orders] Received {batch_size} records (total: {total_records})")

        yield batch

        if batch_size < limit:
            logger.info(f"[orders] Last batch (size {batch_size} < limit {limit})")
            break
        offset += limit

    logger.info(f"[orders] Extraction complete: {total_records} total records")


@dlt.resource(write_disposition="merge", primary_key="id")
def order_lines(
    updated_at=dlt.sources.incremental("write_date", initial_value="2020-01-01 00:00:00"),
    limit: int = 1000,
):
    """Sale order lines stream with incremental loading."""
    api = OdooAPI()
    offset = 0
    total_records = 0

    logger.info(f"[order_lines] Starting extraction, cursor: {updated_at.last_value}")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "start_date": updated_at.last_value,
        }
        logger.info(f"[order_lines] Fetching batch: offset={offset}, limit={limit}")
        batch = api.get("api/sale_order_lines", params=params)

        if not batch:
            logger.info(f"[order_lines] Empty batch received, ending extraction")
            break

        batch_size = len(batch)
        total_records += batch_size
        logger.info(f"[order_lines] Received {batch_size} records (total: {total_records})")

        yield batch

        if batch_size < limit:
            logger.info(f"[order_lines] Last batch (size {batch_size} < limit {limit})")
            break
        offset += limit

    logger.info(f"[order_lines] Extraction complete: {total_records} total records")

from flask import Blueprint, render_template
from services.query_utils import query_trino
from trino.exceptions import TrinoUserError

last_sync_bp = Blueprint("last_sync", __name__)

@last_sync_bp.route("/last-sync")
def last_sync():
    """
    Checks the maximum date/timestamp for all tables in the mart schema.
    """
    query = """
        SELECT 'Campaign Performance' AS table_name, MAX(date) AS last_sync_date
        FROM iceberg.mart.campaign_performance

        UNION ALL

        SELECT 'Product Performance' AS table_name, MAX(date) AS last_sync_date
        FROM iceberg.mart.shopify_product_performance

        UNION ALL

        SELECT 'Tag Performance' AS table_name, MAX(date) AS last_sync_date
        FROM iceberg.mart.tag_performance

        UNION ALL

        SELECT 'Sales Breakdown' AS table_name, MAX(day) AS last_sync_date
        FROM iceberg.mart.shopify_sales_breakdown
    """
    try:
        sync_data = query_trino(query)
        return render_template("last_sync.html", sync_data=sync_data, error=None)

    except TrinoUserError:
        error_message = (
            "Could not retrieve sync status. One or more mart tables may not exist yet. "
            "Please ensure the dbt models have run successfully in Dagster."
        )
        return render_template("last_sync.html", sync_data=[], error=error_message)
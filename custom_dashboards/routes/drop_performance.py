from flask import Blueprint, render_template, request, jsonify
from services.query_utils import query_trino
from trino.exceptions import TrinoUserError

drop_perf_bp = Blueprint("drop_performance", __name__)

@drop_perf_bp.route("/drop-performance")
def drop_performance():
    try:
        # Query to load all tags (for the dropdown)
        tags = query_trino("""
            SELECT DISTINCT tag_name
            FROM iceberg.mart.tag_performance
            WHERE lower(tag_name) LIKE '%drop%'
            ORDER BY tag_name
        """)
        return render_template("drop_performance.html", tags=[t[0] for t in tags], error=None)

    except TrinoUserError as e:
        error_message = (
            "The underlying data mart has not been created yet. "
            "Please ensure the dbt models have run successfully in Dagster."
        )
        return render_template("drop_performance.html", tags=[], error=error_message)


@drop_perf_bp.route("/api/drop-performance")
def drop_performance_data():
    tag = request.args.get("tag")
    if not tag:
        return jsonify({"error": "Missing tag"}), 400

    # Query to load products for a selected tag
    query = f"""
        SELECT
            p.product_title,
            SUM(p.units_sold) AS total_units_sold,
            SUM(p.revenue) AS total_revenue
        FROM iceberg.mart.shopify_product_performance p
        JOIN iceberg.core.shopify_product_tags t ON p.product_id = t.product_id
        WHERE t.tag_name = '{tag}'
        GROUP BY p.product_title
        ORDER BY total_revenue DESC
    """
    try:
        rows = query_trino(query)
        data = [{"product_name": r[0], "units_sold": r[1], "revenue": r[2]} for r in rows]
        return jsonify(data)
    except TrinoUserError as e:
        return jsonify({"error": f"Failed to query Trino: {e}"}), 500
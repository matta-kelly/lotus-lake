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
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    if not tag:
        return jsonify({"error": "Missing tag"}), 400

    # Dynamically build the query based on provided filters
    query = f"""
        SELECT
            p.product_title,
            SUM(p.units_sold) AS total_units_sold,
            SUM(p.revenue) AS total_revenue
        FROM iceberg.mart.shopify_product_performance p
        JOIN iceberg.core.shopify_product_tags t ON p.product_id = t.product_id
        WHERE t.tag_name = '{tag}'
    """

    if start_date and end_date:
        query += f" AND p.date BETWEEN DATE '{start_date}' AND DATE '{end_date}'"

    query += """
        GROUP BY p.product_title
        ORDER BY total_revenue DESC
    """

    try:
        rows = query_trino(query)
        data = [{"product_name": r[0], "units_sold": r[1], "revenue": r[2]} for r in rows]
        return jsonify(data)
    except TrinoUserError as e:
        return jsonify({"error": f"Failed to query Trino: {e}"}), 500


@drop_perf_bp.route("/api/get-release-date")
def get_release_date():
    """
    Finds the earliest release date for all products associated with a given tag.
    """
    tag = request.args.get("tag")
    if not tag:
        return jsonify({"error": "Missing tag"}), 400

    query = f"""
        SELECT MIN(p.release_date)
        FROM iceberg.mart.shopify_product_performance p
        JOIN iceberg.core.shopify_product_tags t ON p.product_id = t.product_id
        WHERE t.tag_name = '{tag}'
    """
    try:
        result = query_trino(query)
        release_date = result[0][0] if result and result[0] and result[0][0] else None

        if release_date:
            return jsonify({"release_date": release_date.strftime('%Y-%m-%d')})
        else:
            return jsonify({"release_date": None})

    except TrinoUserError as e:
        return jsonify({"error": f"Failed to query Trino for release date: {e}"}), 500
from flask import Blueprint, render_template, request, jsonify
from services.query_utils import query_trino

drop_perf_bp = Blueprint("drop_performance", __name__)

@drop_perf_bp.route("/drop-performance")
def drop_performance():
    # ① Query to load all tags (for the dropdown)
    tags = query_trino("""
        SELECT DISTINCT tag_name
        FROM iceberg.mart.shopify_tag_performance
        ORDER BY tag_name
    """)
    return render_template("drop_performance.html", tags=[t[0] for t in tags])

@drop_perf_bp.route("/api/drop-performance")
def drop_performance_data():
    tag = request.args.get("tag")
    if not tag:
        return jsonify({"error": "Missing tag"}), 400

    # ② Query to load products for a selected tag
    query = f"""
        SELECT p.product_id, p.product_name, p.units_sold, p.revenue
        FROM iceberg.mart.shopify_product_performance p
        JOIN iceberg.core.shopify_product_tags t ON p.product_id = t.product_id
        WHERE t.tag_name = '{tag}'
        ORDER BY p.revenue DESC
    """
    rows = query_trino(query)
    data = [{"product_id": r[0], "product_name": r[1], "units_sold": r[2], "revenue": r[3]} for r in rows]
    return jsonify(data)

from flask import Blueprint, render_template, jsonify, request
from services.query_utils import query_trino
from trino.exceptions import TrinoUserError
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

home_bp = Blueprint("home", __name__)

@home_bp.route("/")
def home():
    return render_template("home.html", title="Home")


@home_bp.route("/api/sales-overview")
def sales_overview():
    """
    Fetch sales breakdown data for a given date range
    """
    # Get date parameters (default to last 30 days)
    end_date = request.args.get("end_date")
    start_date = request.args.get("start_date")
    
    # If no dates provided, default to last 30 days
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Validate date range
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if start_dt > end_dt:
            logger.warning(f"Invalid date range: {start_date} > {end_date}, swapping dates")
            start_date, end_date = end_date, start_date
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    
    logger.info(f"Fetching sales overview for date range: {start_date} to {end_date}")
    
    try:
        query = f"""
            SELECT
                day,
                gross_sales,
                discounts,
                returns,
                net_sales,
                shipping_charges,
                taxes,
                total_sales,
                order_count
            FROM iceberg.mart.shopify_sales_breakdown
            WHERE day BETWEEN DATE '{start_date}' AND DATE '{end_date}'
            ORDER BY day DESC
        """
        
        logger.info("Executing sales overview query")
        rows = query_trino(query)
        
        sales_data = [
            {
                "day": r[0].strftime('%Y-%m-%d') if r[0] else None,
                "gross_sales": float(r[1]) if r[1] is not None else 0.0,
                "discounts": float(r[2]) if r[2] is not None else 0.0,
                "returns": float(r[3]) if r[3] is not None else 0.0,
                "net_sales": float(r[4]) if r[4] is not None else 0.0,
                "shipping_charges": float(r[5]) if r[5] is not None else 0.0,
                "taxes": float(r[6]) if r[6] is not None else 0.0,
                "total_sales": float(r[7]) if r[7] is not None else 0.0,
                "order_count": int(r[8]) if r[8] is not None else 0
            } for r in rows
        ]
        
        logger.info(f"Sales overview query returned {len(sales_data)} rows")
        
        # Calculate summary stats
        total_sales = sum(d["total_sales"] for d in sales_data)
        total_orders = sum(d["order_count"] for d in sales_data)
        total_discounts = sum(d["discounts"] for d in sales_data)
        total_returns = sum(d["returns"] for d in sales_data)
        avg_order_value = total_sales / total_orders if total_orders > 0 else 0.0
        
        return jsonify({
            "sales_data": sales_data,
            "summary": {
                "total_sales": total_sales,
                "total_orders": total_orders,
                "total_discounts": total_discounts,
                "total_returns": total_returns,
                "avg_order_value": avg_order_value
            }
        })
        
    except TrinoUserError as e:
        logger.error(f"Trino error: {e}")
        return jsonify({"error": f"Failed to query Trino: {e}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
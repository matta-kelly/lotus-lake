from flask import Blueprint, render_template, request, jsonify
from services.query_utils import query_trino
from trino.exceptions import TrinoUserError
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.error(f"Failed to load tags: {e}")
        return render_template("drop_performance.html", tags=[], error=error_message)


@drop_perf_bp.route("/api/drop-performance")
def drop_performance_data():
    tag = request.args.get("tag")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    if not tag:
        return jsonify({"error": "Missing tag"}), 400

    # Validate date range
    if start_date and end_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            if start_dt > end_dt:
                logger.warning(f"Invalid date range: {start_date} > {end_date}, swapping dates")
                start_date, end_date = end_date, start_date
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    logger.info(f"Fetching drop performance for tag: {tag}, date range: {start_date} to {end_date}")

    try:
        # --- Query 1: Product Performance ---
        product_query = f"""
            SELECT
                p.product_title,
                SUM(p.units_sold) AS total_units_sold,
                SUM(p.revenue) AS total_revenue
            FROM iceberg.mart.shopify_product_performance p
            JOIN iceberg.core.shopify_product_tags t ON p.product_id = t.product_id
            WHERE t.tag_name = '{tag}'
        """
        if start_date and end_date:
            product_query += f" AND p.date BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        product_query += """
            GROUP BY p.product_title
            ORDER BY total_revenue DESC
        """
        
        logger.info(f"Executing product query")
        product_rows = query_trino(product_query)
        product_data = [{"product_name": r[0], "units_sold": r[1], "revenue": r[2]} for r in product_rows]
        logger.info(f"Product query returned {len(product_data)} rows")

        # Calculate product summary stats
        total_units = sum(p["units_sold"] for p in product_data)
        total_revenue = sum(p["revenue"] for p in product_data)

        # --- Query 2: Campaign Performance (campaigns SENT in date range) ---
        campaign_data = []
        campaign_summary = {
            "avg_open_rate": 0.0,
            "avg_click_rate": 0.0,
            "avg_ctor": 0.0
        }
        
        if start_date and end_date:
            campaign_query = f"""
                WITH campaigns_in_period AS (
                    -- Get campaigns sent during the date range
                    SELECT DISTINCT campaign_id, campaign_name, send_time
                    FROM iceberg.mart.campaign_performance
                    WHERE send_time BETWEEN DATE '{start_date}' AND DATE '{end_date}'
                ),
                campaign_totals AS (
                    -- Get total performance for those campaigns (across all dates)
                    SELECT
                        cp.campaign_id,
                        cp.campaign_name,
                        MIN(cp.send_time) as send_time,
                        SUM(cp.unique_received) AS total_received,
                        SUM(cp.unique_opens) AS total_opens,
                        SUM(cp.unique_clicks) AS total_clicks,
                        CASE 
                            WHEN SUM(cp.unique_received) > 0 
                            THEN ROUND(SUM(cp.unique_opens) * 100.0 / SUM(cp.unique_received), 2)
                            ELSE 0 
                        END AS open_rate,
                        CASE 
                            WHEN SUM(cp.unique_received) > 0 
                            THEN ROUND(SUM(cp.unique_clicks) * 100.0 / SUM(cp.unique_received), 2)
                            ELSE 0 
                        END AS click_rate,
                        CASE 
                            WHEN SUM(cp.unique_opens) > 0 
                            THEN ROUND(SUM(cp.unique_clicks) * 100.0 / SUM(cp.unique_opens), 2)
                            ELSE 0 
                        END AS click_to_open_rate
                    FROM iceberg.mart.campaign_performance cp
                    INNER JOIN campaigns_in_period cip 
                        ON cp.campaign_id = cip.campaign_id
                    GROUP BY cp.campaign_id, cp.campaign_name
                )
                SELECT 
                    campaign_name,
                    send_time,
                    total_received,
                    total_opens,
                    total_clicks,
                    open_rate,
                    click_rate,
                    click_to_open_rate
                FROM campaign_totals
                ORDER BY total_opens DESC
                LIMIT 100
            """
            
            logger.info(f"Executing campaign query (filtering by send_time)")
            campaign_rows = query_trino(campaign_query)
            logger.info(f"Campaign query returned {len(campaign_rows)} rows")
            
            campaign_data = [
                {
                    "campaign_name": r[0] if r[0] else "Unknown Campaign",
                    "send_time": r[1].strftime('%Y-%m-%d') if r[1] else None,
                    "unique_received": int(r[2]) if r[2] is not None else 0,
                    "unique_opens": int(r[3]) if r[3] is not None else 0,
                    "unique_clicks": int(r[4]) if r[4] is not None else 0,
                    "open_rate": float(r[5]) if r[5] is not None else 0.0,
                    "click_rate": float(r[6]) if r[6] is not None else 0.0,
                    "click_to_open_rate": float(r[7]) if r[7] is not None else 0.0
                } for r in campaign_rows
            ]

            # Calculate campaign summary stats (averages across all campaigns)
            if campaign_data:
                campaign_summary = {
                    "avg_open_rate": round(sum(c["open_rate"] for c in campaign_data) / len(campaign_data), 2),
                    "avg_click_rate": round(sum(c["click_rate"] for c in campaign_data) / len(campaign_data), 2),
                    "avg_ctor": round(sum(c["click_to_open_rate"] for c in campaign_data) / len(campaign_data), 2)
                }

        # --- Return both datasets in a single response ---
        response = {
            "products": product_data,
            "campaigns": campaign_data,
            "summary": {
                "total_units": total_units,
                "total_revenue": total_revenue,
                "avg_open_rate": campaign_summary["avg_open_rate"],
                "avg_click_rate": campaign_summary["avg_click_rate"],
                "avg_ctor": campaign_summary["avg_ctor"]
            },
            "debug": {
                "tag": tag,
                "start_date": start_date,
                "end_date": end_date,
                "product_count": len(product_data),
                "campaign_count": len(campaign_data)
            }
        }
        
        logger.info(f"Returning response with {len(product_data)} products and {len(campaign_data)} campaigns")
        return jsonify(response)

    except TrinoUserError as e:
        logger.error(f"Trino error: {e}")
        return jsonify({"error": f"Failed to query Trino: {e}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


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
        logger.info(f"Fetching release date for tag: {tag}")
        result = query_trino(query)
        release_date = result[0][0] if result and result[0] and result[0][0] else None

        if release_date:
            logger.info(f"Release date found: {release_date}")
            return jsonify({"release_date": release_date.strftime('%Y-%m-%d')})
        else:
            logger.warning(f"No release date found for tag: {tag}")
            return jsonify({"release_date": None})

    except TrinoUserError as e:
        logger.error(f"Failed to query Trino for release date: {e}")
        return jsonify({"error": f"Failed to query Trino for release date: {e}"}), 500


@drop_perf_bp.route("/api/campaign-date-range")
def get_campaign_date_range():
    """
    Returns the available date range for campaign data to help with debugging.
    """
    try:
        query = """
            SELECT 
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT campaign_name) as campaign_count,
                COUNT(*) as total_records
            FROM iceberg.mart.campaign_performance
        """
        result = query_trino(query)
        
        if result and result[0]:
            return jsonify({
                "earliest_date": result[0][0].strftime('%Y-%m-%d') if result[0][0] else None,
                "latest_date": result[0][1].strftime('%Y-%m-%d') if result[0][1] else None,
                "campaign_count": result[0][2],
                "total_records": result[0][3]
            })
        else:
            return jsonify({
                "error": "No campaign data available"
            })
    except TrinoUserError as e:
        logger.error(f"Failed to query campaign date range: {e}")
        return jsonify({"error": f"Failed to query campaign date range: {e}"}), 500
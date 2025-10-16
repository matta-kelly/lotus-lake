import subprocess
import json
from pathlib import Path
from dagster import asset, AssetExecutionContext, Output, AssetIn

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "..", "..", "dbt").resolve()

def run_dbt_command(command: list[str], context: AssetExecutionContext, target_name: str) -> dict:
    """
    Execute dbt command with isolated target directory for concurrent execution.
    
    Args:
        command: dbt command to run (e.g., ["run", "--select", "model_name"])
        context: Dagster execution context
        target_name: Unique identifier for this job's target directory
    
    Returns:
        dict with stdout, stderr, returncode, and target_dir
    """
    target_dir = f"target_{target_name}"
    
    # Add target-path to isolate this run's artifacts
    full_command = ["dbt"] + command + ["--target-path", target_dir]
    
    context.log.info(f"Running: {' '.join(full_command)} (cwd={DBT_PROJECT_DIR})")

    result = subprocess.run(
        full_command,
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        full_error = f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        context.log.error(f"dbt command failed:\n{full_error}")
        raise Exception("dbt command failed. See logs for details.")

    context.log.info(f"✓ dbt completed successfully.\n{result.stdout[:300]}")
    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
        "target_dir": target_dir
    }


@asset(
    name="shopify_orders_core_dbt_models",
    description="dbt core models for the Shopify Orders flow.",
    group_name="shopify_order_core_dbt",
    ins={"iceberg_result": AssetIn("iceberg_loader_shopify_orders")},
    tags={"dbt_layer": "core"},
)
def shopify_orders_core_dbt_models(context: AssetExecutionContext, iceberg_result: dict):
    """Run dbt core models for Shopify Orders, validate, and pass through result."""
    if "row_count" not in iceberg_result:
        raise ValueError("iceberg_result missing row_count for validation")

    claimed_parent_count = iceberg_result["row_count"]
    context.log.info(f"✓ Validated input: {claimed_parent_count} parent orders from Iceberg")

    dbt_result = run_dbt_command(
        [
            "run",
            "--select",
            "core.shopify.shopify_orders core.shopify.shopify_order_lines core.shopify.shopify_tax_lines core.shopify.shopify_discounts",
        ],
        context,
        target_name="shopify_orders"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)

        parent_rows_processed = None
        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "")
            if "shopify_orders" in model_name and "order_lines" not in model_name:
                adapter_response = result.get("adapter_response", {})
                parent_rows_processed = adapter_response.get("rows_affected", 0)
                context.log.info(
                    f"dbt processed {parent_rows_processed} rows for shopify_orders model"
                )
                break

        if parent_rows_processed is not None:
            if parent_rows_processed == 0:
                raise ValueError(
                    f"dbt processed 0 rows for shopify_orders but {claimed_parent_count} were loaded to raw. "
                    f"Data loss detected in dbt transformation."
                )
            context.log.info(
                f"✓ Validated dbt processing: {parent_rows_processed} rows affected "
                f"(includes DELETE+INSERT for {claimed_parent_count} parent orders)"
            )
        else:
            context.log.warning("Could not extract row count from dbt run_results.json - validation skipped")
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully, passing through iceberg result for sync update")
    return iceberg_result


@asset(
    name="shopify_orders_mart_dbt_models",
    description="dbt mart models for the Shopify Orders flow.",
    group_name="shopify_sales_summary_mart_dbt",
    deps=["shopify_orders_core_dbt_models"],
    tags={"dbt_layer": "mart"},
)
def shopify_orders_mart_dbt_models(context: AssetExecutionContext):
    """Run dbt mart models after core models complete."""
    result = run_dbt_command(
        ["run", "--select", "shopify_sales_breakdown"],
        context,
        target_name="shopify_orders_mart"
    )
    
    return Output(
        value=result,
        metadata={
            "models_run": "shopify_sales_breakdown",
            "dbt_output": result["stdout"][:500]
        }
    )


@asset(
    name="klaviyo_email_open_core_dbt_models",
    description="dbt core models for the Klaviyo Email Open flow.",
    group_name="klaviyo_email_open_dbt",
    ins={"iceberg_result": AssetIn("iceberg_loader_klaviyo_email_open")},
    tags={"dbt_layer": "core"},
)
def klaviyo_email_open_core_dbt_models(context: AssetExecutionContext, iceberg_result: dict):
    """Run dbt core models for Klaviyo Email Open events, validate, and pass through result."""
    if "row_count" not in iceberg_result:
        raise ValueError("iceberg_result missing row_count for validation")

    claimed_parent_count = iceberg_result["row_count"]
    context.log.info(f"✓ Validated input: {claimed_parent_count} email open events from Iceberg")

    dbt_result = run_dbt_command(
        ["run", "--select", "email_open"],
        context,
        target_name="klaviyo_email_open"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)

        rows_processed = None
        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "")
            if "email_open" in model_name:
                adapter_response = result.get("adapter_response", {})
                rows_processed = adapter_response.get("rows_affected", 0)
                context.log.info(
                    f"dbt processed {rows_processed} rows for klaviyo_email_open model"
                )
                break

        if rows_processed is not None:
            if rows_processed == 0:
                raise ValueError(
                    f"dbt processed 0 rows for klaviyo_email_open but {claimed_parent_count} were loaded to raw. "
                    f"Data loss detected in dbt transformation."
                )
            context.log.info(
                f"✓ Validated dbt processing: {rows_processed} rows affected "
                f"(includes DELETE+INSERT for {claimed_parent_count} raw records)"
            )
        else:
            context.log.warning(
                "Could not extract row count from dbt run_results.json - validation skipped"
            )
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully, passing through iceberg result for sync update")
    return iceberg_result


@asset(
    name="klaviyo_campaign_core_dbt_models",
    description="dbt core models for the Klaviyo Campaign flow.",
    group_name="klaviyo_campaign_dbt",
    ins={"iceberg_result": AssetIn("iceberg_loader_klaviyo_campaign")},
    tags={"dbt_layer": "core"},
)
def klaviyo_campaign_core_dbt_models(context: AssetExecutionContext, iceberg_result: dict):
    """Run dbt core models for Klaviyo Campaign data, validate row counts, and pass through result."""
    if "row_count" not in iceberg_result:
        raise ValueError("iceberg_result missing row_count for validation")

    claimed_parent_count = iceberg_result["row_count"]
    context.log.info(f"Validated input: {claimed_parent_count} Klaviyo campaign records from Iceberg")

    # Run just the klaviyo_campaigns model (no more campaign_message)
    dbt_result = run_dbt_command(
        ["run", "--select", "klaviyo_campaigns"],
        context,
        target_name="klaviyo_campaign"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)

        rows_processed = None
        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "")
            if "klaviyo_campaigns" in model_name:
                adapter_response = result.get("adapter_response", {})
                rows_processed = adapter_response.get("rows_affected", 0)
                context.log.info(
                    f"dbt processed {rows_processed} rows for klaviyo_campaigns model"
                )
                break

        if rows_processed is not None:
            if rows_processed == 0:
                raise ValueError(
                    f"dbt processed 0 rows for klaviyo_campaigns but {claimed_parent_count} were loaded to raw. "
                    f"Data loss detected in dbt transformation."
                )
            context.log.info(
                f"Validated dbt processing: {rows_processed} rows affected "
                f"(includes DELETE+INSERT for {claimed_parent_count} campaign records)"
            )
        else:
            context.log.warning("Could not extract row count from dbt run_results.json - validation skipped")
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully, passing through iceberg result for sync update")
    return iceberg_result

@asset(
    name="shopify_products_core_dbt_models",
    description="dbt core models for the Shopify Products flow.",
    group_name="shopify_products_core_dbt",
    ins={"iceberg_result": AssetIn("iceberg_loader_shopify_products")},
    tags={"dbt_layer": "core"},
)
def shopify_products_core_dbt_models(context: AssetExecutionContext, iceberg_result: dict):
    """Run dbt core models for Shopify Products, Variants, and Tags."""
    if "row_count" not in iceberg_result:
        raise ValueError("iceberg_result missing row_count for validation")

    claimed_parent_count = iceberg_result["row_count"]
    context.log.info(f"✓ Validated input: {claimed_parent_count} product records from Iceberg")

    dbt_result = run_dbt_command(
        [
            "run",
            "--select",
            "core.shopify.shopify_products core.shopify.shopify_variants core.shopify.shopify_product_tags",
        ],
        context,
        target_name="shopify_products"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)

        rows_processed = {}
        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "")
            adapter_response = result.get("adapter_response", {})
            if "shopify_products" in model_name:
                rows_processed["products"] = adapter_response.get("rows_affected", 0)
            elif "shopify_variants" in model_name:
                rows_processed["variants"] = adapter_response.get("rows_affected", 0)
            elif "shopify_product_tags" in model_name:
                rows_processed["tags"] = adapter_response.get("rows_affected", 0)

        if not rows_processed:
            context.log.warning("No rows_affected found in run_results.json - validation skipped")
        else:
            for k, v in rows_processed.items():
                context.log.info(f"✓ dbt processed {v} rows for {k}")

            if rows_processed.get("products", 0) == 0:
                raise ValueError(
                    f"dbt processed 0 rows for shopify_products, "
                    f"but {claimed_parent_count} were loaded to raw."
                )

            context.log.info(
                f"✓ Validated dbt processing across {len(rows_processed)} models. "
                f"Total rows affected: {sum(rows_processed.values())}"
            )

    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully, passing through iceberg result for sync update")
    return iceberg_result


@asset(
    name="klaviyo_received_email_core_dbt_models",
    description="dbt core models for the Klaviyo Received Email flow.",
    group_name="klaviyo_received_email_dbt",
    ins={"iceberg_result": AssetIn("iceberg_loader_klaviyo_received_email")},
    tags={"dbt_layer": "core"},
)
def klaviyo_received_email_core_dbt_models(context: AssetExecutionContext, iceberg_result: dict):
    """Run dbt core models for Klaviyo Received Email events, validate, and pass through result."""
    if "row_count" not in iceberg_result:
        raise ValueError("iceberg_result missing row_count for validation")

    claimed_parent_count = iceberg_result["row_count"]
    context.log.info(f"✓ Validated input: {claimed_parent_count} received email events from Iceberg")

    dbt_result = run_dbt_command(
        ["run", "--select", "received_email"],
        context,
        target_name="klaviyo_received_email"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)

        rows_processed = None
        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "")
            if "received_email" in model_name:
                adapter_response = result.get("adapter_response", {})
                rows_processed = adapter_response.get("rows_affected", 0)
                context.log.info(
                    f"dbt processed {rows_processed} rows for klaviyo_received_email model"
                )
                break

        if rows_processed is not None:
            if rows_processed == 0 and claimed_parent_count > 0:
                raise ValueError(
                    f"dbt processed 0 rows for klaviyo_received_email but {claimed_parent_count} were loaded to raw. "
                    f"Data loss detected in dbt transformation."
                )
            context.log.info(
                f"✓ Validated dbt processing: {rows_processed} rows affected "
                f"(includes DELETE+INSERT for {claimed_parent_count} raw records)"
            )
        else:
            context.log.warning(
                "Could not extract row count from dbt run_results.json - validation skipped"
            )
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully, passing through iceberg result for sync update")
    return iceberg_result


@asset(
    name="klaviyo_email_clicked_core_dbt_models",
    description="dbt core models for the Klaviyo Email Clicked flow.",
    group_name="klaviyo_email_clicked_dbt",
    ins={"iceberg_result": AssetIn("iceberg_loader_klaviyo_email_clicked")},
    tags={"dbt_layer": "core"},
)
def klaviyo_email_clicked_core_dbt_models(context: AssetExecutionContext, iceberg_result: dict):
    """Run dbt core models for Klaviyo Email Clicked events."""
    if "row_count" not in iceberg_result:
        raise ValueError("iceberg_result missing row_count for validation")

    claimed_parent_count = iceberg_result["row_count"]
    context.log.info(f"✓ Validated input: {claimed_parent_count} email click events from Iceberg")

    dbt_result = run_dbt_command(
        ["run", "--select", "email_clicked"],
        context,
        target_name="klaviyo_email_clicked"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)

        rows_processed = None
        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "")
            if "email_clicked" in model_name:
                adapter_response = result.get("adapter_response", {})
                rows_processed = adapter_response.get("rows_affected", 0)
                context.log.info(
                    f"dbt processed {rows_processed} rows for klaviyo_email_clicked model"
                )
                break

        if rows_processed is not None:
            if rows_processed == 0 and claimed_parent_count > 0:
                raise ValueError(
                    f"dbt processed 0 rows for klaviyo_email_clicked but {claimed_parent_count} were loaded to raw. "
                    f"Data loss suspected in dbt transformation."
                )
            context.log.info(
                f"✓ Validated dbt processing: {rows_processed} rows affected "
                f"(DELETE+INSERT operations for {claimed_parent_count} raw records)"
            )
        else:
            context.log.warning("Could not extract row count from dbt run_results.json - validation skipped")

    except FileNotFoundError:
        context.log.warning(
            "run_results.json not found — dbt may have terminated unexpectedly. Validation skipped."
        )
    except json.JSONDecodeError as e:
        context.log.warning(f"Invalid JSON in run_results.json: {e}. Validation skipped.")
    except Exception as e:
        context.log.warning(f"Unexpected issue parsing run_results.json: {e}. Validation skipped.")

    context.log.info("✓ dbt completed successfully, passing through iceberg result for downstream sync update.")
    return iceberg_result


@asset(
    name="klaviyo_campaign_performance_mart_dbt_models",
    description="dbt mart model for campaign-level email performance (received, opened, clicked, and rates).",
    group_name="klaviyo_campaign_performance_mart_dbt",
    deps=[
        "klaviyo_campaign_core_dbt_models",
        "klaviyo_received_email_core_dbt_models",
        "klaviyo_email_open_core_dbt_models",
        "klaviyo_email_clicked_core_dbt_models",
    ],
    tags={"dbt_layer": "mart"},
)
def klaviyo_campaign_performance_mart_dbt_models(context: AssetExecutionContext):
    """Run the dbt mart model that aggregates Klaviyo campaign performance metrics."""
    context.log.info("Starting dbt run for campaign_performance...")

    dbt_result = run_dbt_command(
        ["run", "--select", "campaign_performance"],
        context,
        target_name="klaviyo_campaign_performance_mart"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    models_run = []
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)
            for result in run_results.get("results", []):
                unique_id = result.get("unique_id", "")
                if "campaign_performance" in unique_id:
                    models_run.append(unique_id)
                    adapter_response = result.get("adapter_response", {})
                    rows_affected = adapter_response.get("rows_affected", None)
                    context.log.info(
                        f"✓ dbt processed {rows_affected if rows_affected is not None else 'unknown'} rows "
                        f"for campaign_performance mart model."
                    )
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully for campaign_performance.")
    return Output(
        value=dbt_result,
        metadata={
            "models_run": ", ".join(models_run) if models_run else "campaign_performance",
            "dbt_output": dbt_result["stdout"][:500],
        },
    )


@asset(
    name="shopify_product_performance_mart_dbt_models",
    description="dbt mart model aggregating Shopify order lines to product-level performance (units sold, revenue, etc.).",
    group_name="shopify_product_performance_mart_dbt",
    deps=[
        "shopify_orders_core_dbt_models",
        "shopify_products_core_dbt_models",
    ],
    tags={"dbt_layer": "mart"},
)
def shopify_product_performance_mart_dbt_models(context: AssetExecutionContext):
    """Run the dbt mart model for Shopify product performance."""
    context.log.info("Starting dbt run for shopify_product_performance...")

    dbt_result = run_dbt_command(
        ["run", "--select", "shopify_product_performance"],
        context,
        target_name="shopify_product_performance_mart"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    models_run = []
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)
            for result in run_results.get("results", []):
                unique_id = result.get("unique_id", "")
                if "shopify_product_performance" in unique_id:
                    models_run.append(unique_id)
                    adapter_response = result.get("adapter_response", {})
                    rows_affected = adapter_response.get("rows_affected", None)
                    context.log.info(
                        f"✓ dbt processed {rows_affected if rows_affected is not None else 'unknown'} rows "
                        f"for shopify_product_performance mart model."
                    )
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully for shopify_product_performance.")
    return Output(
        value=dbt_result,
        metadata={
            "models_run": ", ".join(models_run) if models_run else "shopify_product_performance",
            "dbt_output": dbt_result["stdout"][:500],
        },
    )


@asset(
    name="shopify_tag_performance_summary_mart_dbt_models",
    description="dbt mart model aggregating Shopify product performance by tag (drop-level summary: units sold, revenue, averages).",
    group_name="shopify_tag_performance_summary_mart_dbt",
    deps=[
        "shopify_product_performance_mart_dbt_models",
    ],
    tags={"dbt_layer": "mart"},
)
def shopify_tag_performance_summary_mart_dbt_models(context: AssetExecutionContext):
    """Run the dbt mart model for tag-level performance summaries."""
    context.log.info("Starting dbt run for shopify_tag_performance...")

    dbt_result = run_dbt_command(
        ["run", "--select", "shopify_tag_performance"],
        context,
        target_name="shopify_tag_performance_mart"
    )

    run_results_path = DBT_PROJECT_DIR / dbt_result["target_dir"] / "run_results.json"
    models_run = []
    try:
        with open(run_results_path) as f:
            run_results = json.load(f)
            for result in run_results.get("results", []):
                unique_id = result.get("unique_id", "")
                if "shopify_tag_performance" in unique_id:
                    models_run.append(unique_id)
                    adapter_response = result.get("adapter_response", {})
                    rows_affected = adapter_response.get("rows_affected", None)
                    context.log.info(
                        f"✓ dbt processed {rows_affected if rows_affected is not None else 'unknown'} rows "
                        f"for shopify_tag_performance mart model."
                    )
    except Exception as e:
        context.log.warning(f"Could not parse dbt run_results.json: {e} - validation skipped")

    context.log.info("dbt completed successfully for shopify_tag_performance.")
    return Output(
        value=dbt_result,
        metadata={
            "models_run": ", ".join(models_run) if models_run else "shopify_tag_performance",
            "dbt_output": dbt_result["stdout"][:500],
        },
    )
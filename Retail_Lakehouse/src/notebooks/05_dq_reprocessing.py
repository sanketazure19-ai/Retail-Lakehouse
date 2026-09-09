# Databricks notebook source

from retail_lakehouse.config.settings import (
    get_datasets,
    get_environment,
)
from retail_lakehouse.utils.dq_reprocessing import (
    reprocess_quarantine_batch,
)
from retail_lakehouse.utils.job_logging import log_cell
from retail_lakehouse.utils.logging import get_logger


logger = get_logger("dq_reprocessing")


# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("domain", "erp")
dbutils.widgets.text("dataset", "customers")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")

environment = dbutils.widgets.get("environment")
domain = dbutils.widgets.get("domain")
dataset = dbutils.widgets.get("dataset")
batch_id = dbutils.widgets.get("batch_id")

job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")


if not batch_id:
    raise ValueError(
        "batch_id parameter is required for DQ reprocessing."
    )


# COMMAND ----------

env_config = get_environment(environment)
datasets = get_datasets()

dataset_config = (
    datasets
    .get(domain, {})
    .get(dataset)
)

if dataset_config is None:
    raise ValueError(
        f"Unknown dataset configuration: "
        f"{domain}.{dataset}"
    )


catalog = env_config["catalog"]

source_table = (
    f"{catalog}.bronze_quarantine.{dataset}"
)

target_table = (
    f"{catalog}.bronze.{dataset}"
)

required_columns = {
    "customers": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "country",
        "signup_date",
        "customer_segment",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_price",
        "currency",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "discount",
        "payment_method",
        "order_status",
    ],
    "clickstream": [
        "event_id",
        "customer_id",
        "event_timestamp",
        "event_type",
        "page_url",
        "product_id",
        "device_type",
        "session_id",
    ],
    "returns": [
        "return_id",
        "order_id",
        "customer_id",
        "product_id",
        "return_date",
        "return_quantity",
        "return_reason",
        "refund_amount",
        "return_status",
    ],
    "promotions": [
        "promotion_id",
        "promotion_name",
        "product_id",
        "start_date",
        "end_date",
        "discount_percent",
        "promotion_type",
        "status",
    ],
}

if dataset not in required_columns:
    raise ValueError(
        f"No required-column configuration for dataset "
        f"'{dataset}'."
    )


# COMMAND ----------

validation_rules = dataset_config.get(
    "validation_rules",
    [],
)

job_context = {
    "job_id": job_id,
    "run_id": job_run_id,
    "task_run_id": task_run_id,
}


# COMMAND ----------

log_cell(
    spark=spark,
    catalog=catalog,
    environment=environment,
    job_context=job_context,
    task_key="dq_reprocessing",
    notebook_name="05_dq_reprocessing",
    cell_name="reprocess_quarantine_batch",
    domain=domain,
    dataset=dataset,
    status="STARTED",
)


try:

    result = reprocess_quarantine_batch(
        spark=spark,
        quarantine_table=source_table,
        target_table=target_table,
        dataset_name=dataset,
        batch_id=batch_id,
        required_columns=required_columns[dataset],
        validation_rules=validation_rules,
    )

    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        job_context=job_context,
        task_key="dq_reprocessing",
        notebook_name="05_dq_reprocessing",
        cell_name="reprocess_quarantine_batch",
        domain=domain,
        dataset=dataset,
        status="SUCCESS",
        rows_processed=result["new_bronze_records"],
    )

    print(result)


except Exception as exc:

    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        job_context=job_context,
        task_key="dq_reprocessing",
        notebook_name="05_dq_reprocessing",
        cell_name="reprocess_quarantine_batch",
        domain=domain,
        dataset=dataset,
        status="FAILED",
        error_type=type(exc).__name__,
        error_message=str(exc),
    )

    raise
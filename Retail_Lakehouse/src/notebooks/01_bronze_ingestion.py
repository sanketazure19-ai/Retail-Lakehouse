# Databricks notebook source
import inspect
import retail_lakehouse
import retail_lakehouse.utils.quality as quality_module

print("=== PACKAGE DIAGNOSTIC ===")
print(f"retail_lakehouse package: {retail_lakehouse.__file__}")
print(f"quality module: {quality_module.__file__}")
print(inspect.getsource(quality_module.add_quality_columns))
print("=== END PACKAGE DIAGNOSTIC ===")

from datetime import datetime, timezone

from retail_lakehouse.config.settings import (
    get_datasets,
    get_environment,
)
from retail_lakehouse.utils.bronze import ingest_to_bronze
from retail_lakehouse.utils.helpers import (
    build_raw_path,
    generate_batch_id,
)
from retail_lakehouse.utils.job_logging import (
    get_job_context,
    log_cell,
)
from retail_lakehouse.utils.schemas import (
    CLICKSTREAM_SCHEMA,
    CUSTOMER_SCHEMA,
    ORDER_SCHEMA,
    PRODUCT_SCHEMA,
    RETURN_SCHEMA,
)

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("domain", "erp")
dbutils.widgets.text("dataset", "customers")

dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")

environment = dbutils.widgets.get("environment")
domain = dbutils.widgets.get("domain")
dataset_name = dbutils.widgets.get("dataset")

job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

job_context = {
    "job_id": job_id,
    "run_id": job_run_id,
    "task_run_id": task_run_id,
}

task_key = f"bronze_{dataset_name}"
notebook_name = "01_bronze_ingestion"

# COMMAND ----------

env_config = get_environment(environment)
datasets = get_datasets()

catalog = env_config["catalog"]
raw_root = env_config["raw_root"]
checkpoint_root = env_config["checkpoint_root"]
schema_root = env_config["schema_root"]

dataset_config = datasets[domain][dataset_name]
dq_failure_threshold_percent = float(
    dataset_config.get(
        "dq_failure_threshold_percent",
        10,
    )
)

raw_path = build_raw_path(
    raw_root,
    dataset_config["path"],
)

schema_path = f"{schema_root}/{domain}/{dataset_name}"
checkpoint_path = f"{checkpoint_root}/{domain}/{dataset_name}"

target_table = f"{catalog}.bronze.{dataset_name}"
quarantine_table = f"{catalog}.bronze_quarantine.{dataset_name}"

batch_id = generate_batch_id(dataset_name)

# COMMAND ----------

schemas = {
    "customers": CUSTOMER_SCHEMA,
    "products": PRODUCT_SCHEMA,
    "orders": ORDER_SCHEMA,
    "clickstream": CLICKSTREAM_SCHEMA,
    "returns": RETURN_SCHEMA,
}

required_columns = {
    "customers": [
        "customer_id",
        "email",
        "signup_date",
        "customer_segment",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
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
        "payment_method",
        "order_status",
    ],
    "clickstream": [
        "event_id",
        "customer_id",
        "event_timestamp",
        "event_type",
        "session_id",
    ],
    "returns": [
        "return_id",
        "order_id",
        "customer_id",
        "product_id",
        "return_date",
        "return_quantity",
        "refund_amount",
        "return_status",
    ],
}

# COMMAND ----------

validation_rules = {
    "customers": [
        {
            "column": "email",
            "rule": "regex",
            "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            "reason": "Invalid email format",
        },
    ],
    "products": [
        {
            "column": "unit_price",
            "rule": "greater_than_or_equal",
            "value": 0,
            "reason": "unit_price must be >= 0",
        },
    ],
    "orders": [
        {
            "column": "quantity",
            "rule": "greater_than",
            "value": 0,
            "reason": "quantity must be > 0",
        },
        {
            "column": "unit_price",
            "rule": "greater_than_or_equal",
            "value": 0,
            "reason": "unit_price must be >= 0",
        },
        {
            "column": "discount_amount",
            "rule": "greater_than_or_equal",
            "value": 0,
            "reason": "discount_amount must be >= 0",
        },
    ],
    "clickstream": [
        {
            "column": "event_type",
            "rule": "not_empty",
            "reason": "event_type cannot be empty",
        },
    ],
    "returns": [
        {
            "column": "return_quantity",
            "rule": "greater_than",
            "value": 0,
            "reason": "return_quantity must be > 0",
        },
        {
            "column": "refund_amount",
            "rule": "greater_than_or_equal",
            "value": 0,
            "reason": "refund_amount must be >= 0",
        },
    ],
}

# COMMAND ----------

if dataset_name not in schemas:
    raise ValueError(
        f"Unsupported Auto Loader dataset: {dataset_name}"
    )

# COMMAND ----------

print(f"Environment: {environment}")
print(f"Domain: {domain}")
print(f"Dataset: {dataset_name}")
print(f"RAW path: {raw_path}")
print(f"Target table: {target_table}")
print(f"Quarantine table: {quarantine_table}")
print(
    f"DQ failure threshold: "
    f"{dq_failure_threshold_percent:.2f}%"
)

# COMMAND ----------

cell_start = datetime.now(timezone.utc)

try:
    ingest_to_bronze(
        spark=spark,
        source_path=raw_path,
        schema_path=schema_path,
        checkpoint_path=checkpoint_path,
        target_table=target_table,
        quarantine_table=quarantine_table,
        source_schema=schemas[dataset_name],
        required_columns=required_columns[dataset_name],
        environment=environment,
        batch_id=batch_id,
        validation_rules=validation_rules.get(dataset_name),
        catalog=catalog,
        dataset_name=dataset_name,
        dq_failure_threshold_percent=dq_failure_threshold_percent,
    )

    cell_end = datetime.now(timezone.utc)

    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        context=job_context,
        task_key=task_key,
        notebook_name=notebook_name,
        cell_name="bronze_ingestion",
        status="SUCCESS",
        start_time=cell_start,
        end_time=cell_end,
        domain=domain,
        dataset=dataset_name,
    )

    print(f"Bronze ingestion completed: {target_table}")

except Exception as exc:
    cell_end = datetime.now(timezone.utc)

    try:
        log_cell(
            spark=spark,
            catalog=catalog,
            environment=environment,
            context=job_context,
            task_key=task_key,
            notebook_name=notebook_name,
            cell_name="bronze_ingestion",
            status="FAILED",
            start_time=cell_start,
            end_time=cell_end,
            domain=domain,
            dataset=dataset_name,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
    except Exception as log_exc:
        print(f"Failed to write job log: {log_exc}")

    raise
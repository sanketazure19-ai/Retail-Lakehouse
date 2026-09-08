# Databricks notebook source

from datetime import datetime, timezone

from retail_lakehouse.config.settings import (
    get_datasets,
    get_environment,
)
from retail_lakehouse.utils.helpers import build_raw_path
from retail_lakehouse.utils.job_logging import (
    get_job_context,
    log_cell,
)
from retail_lakehouse.utils.promotions import ingest_promotions
from retail_lakehouse.utils.schemas import PROMOTION_SCHEMA


# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")

environment = dbutils.widgets.get("environment")
job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

job_context = {
    "job_id": dbutils.widgets.get("job_id"),
    "run_id": dbutils.widgets.get("job_run_id"),
    "task_run_id": dbutils.widgets.get("task_run_id"),
}

task_key = "promotions_ingestion"
notebook_name = "04_promotions_ingestion"


# COMMAND ----------

env_config = get_environment(environment)
datasets = get_datasets()

catalog = env_config["catalog"]
raw_root = env_config["raw_root"]

dataset_config = datasets["marketing"]["promotions"]

raw_path = build_raw_path(
    raw_root,
    dataset_config["path"],
)

target_table = f"{catalog}.bronze.promotions"


# COMMAND ----------

print(f"Environment: {environment}")
print(f"RAW path: {raw_path}")
print(f"Target table: {target_table}")


# COMMAND ----------

cell_start = datetime.now(timezone.utc)

try:

    ingest_promotions(
        spark=spark,
        source_path=raw_path,
        target_table=target_table,
        source_schema=PROMOTION_SCHEMA,
        environment=environment,
    )

    cell_end = datetime.now(timezone.utc)

    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        context=job_context,
        task_key=task_key,
        notebook_name=notebook_name,
        cell_name="promotions_ingestion",
        status="SUCCESS",
        start_time=cell_start,
        end_time=cell_end,
        domain="marketing",
        dataset="promotions",
    )

    print(
        f"Promotions ingestion completed: "
        f"{target_table}"
    )

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
            cell_name="promotions_ingestion",
            status="FAILED",
            start_time=cell_start,
            end_time=cell_end,
            domain="marketing",
            dataset="promotions",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
    except Exception as log_exc:
        print(
            f"Failed to write job log: {log_exc}"
        )

    raise
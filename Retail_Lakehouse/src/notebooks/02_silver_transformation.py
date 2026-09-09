# Databricks notebook source

from pyspark.sql import functions as F

from retail_lakehouse.utils.config import load_config
from retail_lakehouse.utils.dq_metrics import (
    calculate_dq_metrics,
    write_dq_metrics,
)
from retail_lakehouse.utils.job_logging import (
    get_job_context,
    log_cell,
)
from retail_lakehouse.utils.quality import add_quality_columns
from retail_lakehouse.utils.silver import (
    deduplicate,
    get_unprocessed_batches,
    mark_batches_processed,
    merge_to_silver,
    standardize_columns,
    write_quarantine,
)
from retail_lakehouse.utils.validation import validate_not_empty

# COMMAND ----------

dbutils.widgets.text("environment", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("dataset", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")

# COMMAND ----------

environment = dbutils.widgets.get("environment")
domain = dbutils.widgets.get("domain")
dataset_name = dbutils.widgets.get("dataset")

# COMMAND ----------

catalog = load_config("environments.yml")["environments"][environment]["catalog"]

datasets_config = load_config("datasets.yml")["datasets"]

dataset_config = datasets_config[domain][dataset_name]

silver_config = dataset_config["silver"]

required_columns = silver_config.get(
    "required_columns",
    [],
)

validation_rules = silver_config.get(
    "validation_rules",
    [],
)

dq_failure_threshold_percent = silver_config.get(
    "dq_alert_threshold_percent",
    1.0,
)

# COMMAND ----------

source_table = (
    f"{catalog}.bronze.{dataset_name}"
)

target_table = (
    f"{catalog}.silver.{dataset_name}"
)

quarantine_table = (
    f"{catalog}.silver_quarantine.{dataset_name}"
)

control_table = (
    f"{catalog}.silver.processed_batches"
)

# COMMAND ----------

key_columns = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "clickstream": ["event_id"],
    "returns": ["return_id"],
    "promotions": ["promotion_id"],
}

if dataset_name not in key_columns:
    raise ValueError(
        f"No Silver business key configured for '{dataset_name}'"
    )

business_keys = key_columns[dataset_name]

# COMMAND ----------

job_context = get_job_context(
    spark,
    dbutils,
)

task_key = dataset_name

notebook_name = "02_silver_transformation"

# COMMAND ----------

print(f"Environment: {environment}")
print(f"Domain: {domain}")
print(f"Dataset: {dataset_name}")
print(f"Source: {source_table}")
print(f"Target: {target_table}")
print(f"Quarantine: {quarantine_table}")
print(f"Business keys: {business_keys}")
print(f"Required columns: {required_columns}")
print(f"Validation rules: {validation_rules}")

print(
    "DQ alert threshold: "
    f"{dq_failure_threshold_percent}%"
)

# COMMAND ----------

unprocessed_batches = get_unprocessed_batches(
    spark=spark,
    source_table=source_table,
    control_table=control_table,
    dataset_name=dataset_name,
)

print(
    f"Unprocessed batches: {unprocessed_batches}"
)

# COMMAND ----------

if not unprocessed_batches:
    print(
        f"No unprocessed batches for {dataset_name}"
    )
else:

    for batch_id in unprocessed_batches:

        cell_name = (
            f"silver_dq_{dataset_name}_{batch_id}"
        )

        try:

            source_df = (
                spark.table(source_table)
                .filter(
                    F.col("_batch_id") == batch_id
                )
            )

            validate_not_empty(source_df)

            source_df = standardize_columns(
                source_df
            )

            dq_df = add_quality_columns(
                source_df,
                required_columns=required_columns,
                validation_rules=validation_rules,
            )

            metrics = calculate_dq_metrics(
                batch_df=dq_df,
                environment=environment,
                dataset=dataset_name,
                batch_id=batch_id,
                micro_batch_id=0,
                threshold_percent=dq_failure_threshold_percent,
            )

            write_dq_metrics(
                spark=spark,
                catalog=catalog,
                metrics=metrics,
            )

            print(
                "Silver DQ metrics: "
                f"total={metrics['total_records']}, "
                f"valid={metrics['valid_records']}, "
                f"quarantined="
                f"{metrics['quarantined_records']}, "
                f"failure_rate="
                f"{metrics['dq_failure_rate_percent']:.2f}%, "
                f"status={metrics['dq_status']}"
            )

            quarantine_df = (
                dq_df
                .filter(
                    F.col("_quality_status")
                    == "QUARANTINE"
                )
                .withColumn(
                    "_quarantine_timestamp",
                    F.current_timestamp(),
                )
            )

            write_quarantine(
                spark=spark,
                df=quarantine_df,
                quarantine_table=quarantine_table,
            )

            valid_df = (
                dq_df
                .filter(
                    F.col("_quality_status")
                    == "VALID"
                )
                .drop(
                    "_quality_status",
                    "_quality_reason",
                )
            )

            valid_df = deduplicate(
                valid_df,
                key_columns=business_keys,
            )

            valid_df = (
                valid_df
                .withColumn(
                    "_silver_timestamp",
                    F.current_timestamp(),
                )
                .withColumn(
                    "_silver_environment",
                    F.lit(environment),
                )
            )

            valid_count = valid_df.count()

            merge_to_silver(
                spark=spark,
                df=valid_df,
                target_table=target_table,
                key_columns=business_keys,
            )

            mark_batches_processed(
                spark=spark,
                control_table=control_table,
                dataset_name=dataset_name,
                batch_ids=[batch_id],
            )

            log_cell(
                spark=spark,
                catalog=catalog,
                environment=environment,
                job_context=job_context,
                task_key=task_key,
                notebook_name=notebook_name,
                cell_name=cell_name,
                domain=domain,
                dataset=dataset_name,
                status="SUCCESS",
                rows_processed=valid_count,
            )

            print(
                f"Successfully processed "
                f"batch {batch_id}: "
                f"{valid_count} valid records"
            )

        except Exception as exc:

            log_cell(
                spark=spark,
                catalog=catalog,
                environment=environment,
                job_context=job_context,
                task_key=task_key,
                notebook_name=notebook_name,
                cell_name=cell_name,
                domain=domain,
                dataset=dataset_name,
                status="FAILED",
                rows_processed=0,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

            raise
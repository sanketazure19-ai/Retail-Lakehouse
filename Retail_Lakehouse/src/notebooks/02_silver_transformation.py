# Databricks notebook source

from retail_lakehouse.config.settings import (
    get_environment,
    get_datasets,
)

from retail_lakehouse.utils.quality import add_quality_columns

from retail_lakehouse.utils.silver import (
    get_unprocessed_batches,
    get_latest_snapshot,
    transform_silver,
    merge_to_silver,
    write_quarantine,
    mark_batches_processed,
)

from retail_lakehouse.utils.dq_metrics import (
    calculate_dq_metrics,
    write_dq_metrics,
)

from retail_lakehouse.utils.job_logging import log_cell

from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Widgets
# ---------------------------------------------------------------------------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("dataset", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")


# ---------------------------------------------------------------------------
# Runtime parameters
# ---------------------------------------------------------------------------

environment = dbutils.widgets.get("environment")
domain = dbutils.widgets.get("domain")
dataset_name = dbutils.widgets.get("dataset")

job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")


# ---------------------------------------------------------------------------
# Job context
# ---------------------------------------------------------------------------

job_context = {
    "job_id": job_id,
    "run_id": job_run_id,
    "task_run_id": task_run_id,
}

task_key = f"silver_{dataset_name}"
notebook_name = "02_silver_transformation"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

env_config = get_environment(environment)
catalog = env_config["catalog"]

datasets = get_datasets()

dataset_config = datasets[domain][dataset_name]
silver_config = dataset_config["silver"]

processing_mode = silver_config.get(
    "processing_mode",
    "incremental",
)

dq_alert_threshold_percent = silver_config.get(
    "dq_alert_threshold_percent",
    1.0,
)

required_columns = silver_config.get(
    "required_columns",
    [],
)

validation_rules = silver_config.get(
    "validation_rules",
    [],
)


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

source_table = f"{catalog}.bronze.{dataset_name}"
target_table = f"{catalog}.silver.{dataset_name}"
quarantine_table = f"{catalog}.silver_quarantine.{dataset_name}"
control_table = f"{catalog}.silver.processed_batches"


# ---------------------------------------------------------------------------
# Business keys
# ---------------------------------------------------------------------------

key_columns = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "clickstream": ["event_id"],
    "returns": ["return_id"],
    "promotions": ["promotion_id"],
}[dataset_name]


# ---------------------------------------------------------------------------
# Runtime information
# ---------------------------------------------------------------------------

print(f"Environment: {environment}")
print(f"Domain: {domain}")
print(f"Dataset: {dataset_name}")
print(f"Processing mode: {processing_mode}")
print(f"Source table: {source_table}")
print(f"Target table: {target_table}")
print(f"Quarantine table: {quarantine_table}")
print(f"Control table: {control_table}")
print(f"Key columns: {key_columns}")


try:

    # -----------------------------------------------------------------------
    # Read Bronze
    # -----------------------------------------------------------------------

    source_df = spark.table(source_table)


    # =======================================================================
    # SNAPSHOT PROCESSING
    # =======================================================================

    if processing_mode == "snapshot":

        print(
            f"Processing {dataset_name} using snapshot semantics."
        )

        # -------------------------------------------------------------------
        # Business DQ
        # -------------------------------------------------------------------

        dq_df = add_quality_columns(
            source_df,
            required_columns=required_columns,
            validation_rules=validation_rules,
        )

        quarantined_df = dq_df.filter(
            F.col("_quality_status") == "QUARANTINE"
        )

        valid_df = dq_df.filter(
            F.col("_quality_status") == "VALID"
        )

        # -------------------------------------------------------------------
        # Determine snapshot batch
        # -------------------------------------------------------------------

        batch_ids = [
            row["_batch_id"]
            for row in (
                dq_df
                .select("_batch_id")
                .where(F.col("_batch_id").isNotNull())
                .distinct()
                .collect()
            )
        ]

        batch_id = (
            batch_ids[-1]
            if batch_ids
            else f"snapshot_{dataset_name}"
        )

        # -------------------------------------------------------------------
        # DQ metrics
        # -------------------------------------------------------------------

        metrics = calculate_dq_metrics(
            batch_df=dq_df,
            environment=environment,
            dataset=dataset_name,
            batch_id=batch_id,
            micro_batch_id=-1,
            threshold_percent=dq_alert_threshold_percent,
        )

        print(
            "Silver DQ metrics: "
            f"total={metrics['total_records']}, "
            f"valid={metrics['valid_records']}, "
            f"quarantined={metrics['quarantined_records']}, "
            f"dq_failure_rate="
            f"{metrics['dq_failure_rate_percent']:.2f}%, "
            f"status={metrics['dq_status']}"
        )

        write_dq_metrics(
            spark=spark,
            catalog=catalog,
            metrics=metrics,
        )

        # -------------------------------------------------------------------
        # Business quarantine
        # -------------------------------------------------------------------

        write_quarantine(
            spark,
            quarantined_df,
            quarantine_table,
        )

        # -------------------------------------------------------------------
        # Snapshot deduplication
        # -------------------------------------------------------------------

        latest_valid_df = get_latest_snapshot(
            valid_df,
            key_columns=key_columns,
        )

        # -------------------------------------------------------------------
        # Silver transformation
        # -------------------------------------------------------------------

        transformed_df = transform_silver(
            latest_valid_df,
            key_columns=key_columns,
            environment=environment,
        )

        # -------------------------------------------------------------------
        # Silver MERGE
        # -------------------------------------------------------------------

        merge_to_silver(
            spark,
            transformed_df,
            target_table,
            key_columns=key_columns,
        )

        print(
            f"Snapshot processing completed for "
            f"{dataset_name}."
        )


    # =======================================================================
    # INCREMENTAL PROCESSING
    # =======================================================================

    elif processing_mode == "incremental":

        print(
            f"Processing {dataset_name} using "
            "incremental semantics."
        )

        # -------------------------------------------------------------------
        # Find unprocessed Bronze batches
        # -------------------------------------------------------------------

        unprocessed_batches = get_unprocessed_batches(
            spark=spark,
            source_table=source_table,
            control_table=control_table,
            dataset_name=dataset_name,
        )

        print(
            f"Unprocessed batches: {unprocessed_batches}"
        )

        # -------------------------------------------------------------------
        # Process each batch
        # -------------------------------------------------------------------

        for batch_id in unprocessed_batches:

            print(
                f"Processing batch {batch_id} "
                f"for {dataset_name}."
            )

            batch_df = source_df.filter(
                F.col("_batch_id") == batch_id
            )

            # ---------------------------------------------------------------
            # Business DQ
            # ---------------------------------------------------------------

            dq_df = add_quality_columns(
                batch_df,
                required_columns=required_columns,
                validation_rules=validation_rules,
            )

            quarantined_df = dq_df.filter(
                F.col("_quality_status") == "QUARANTINE"
            )

            valid_df = dq_df.filter(
                F.col("_quality_status") == "VALID"
            )

            # ---------------------------------------------------------------
            # DQ metrics
            # ---------------------------------------------------------------

            metrics = calculate_dq_metrics(
                batch_df=dq_df,
                environment=environment,
                dataset=dataset_name,
                batch_id=batch_id,
                micro_batch_id=-1,
                threshold_percent=dq_alert_threshold_percent,
            )

            print(
                "Silver DQ metrics: "
                f"total={metrics['total_records']}, "
                f"valid={metrics['valid_records']}, "
                f"quarantined={metrics['quarantined_records']}, "
                f"dq_failure_rate="
                f"{metrics['dq_failure_rate_percent']:.2f}%, "
                f"status={metrics['dq_status']}"
            )

            write_dq_metrics(
                spark=spark,
                catalog=catalog,
                metrics=metrics,
            )

            # ---------------------------------------------------------------
            # Business quarantine
            # ---------------------------------------------------------------

            write_quarantine(
                spark,
                quarantined_df,
                quarantine_table,
            )

            # ---------------------------------------------------------------
            # Silver transformation
            # ---------------------------------------------------------------

            transformed_df = transform_silver(
                valid_df,
                key_columns=key_columns,
                environment=environment,
            )

            # ---------------------------------------------------------------
            # Silver MERGE
            # ---------------------------------------------------------------

            merge_to_silver(
                spark,
                transformed_df,
                target_table,
                key_columns=key_columns,
            )

            # ---------------------------------------------------------------
            # Mark batch as processed
            # ---------------------------------------------------------------

            mark_batches_processed(
                spark=spark,
                control_table=control_table,
                dataset_name=dataset_name,
                batch_ids=[batch_id],
            )

            print(
                f"Processed batch {batch_id} "
                f"for {dataset_name}."
            )


    # =======================================================================
    # INVALID PROCESSING MODE
    # =======================================================================

    else:

        raise ValueError(
            f"Unsupported processing_mode "
            f"'{processing_mode}' for "
            f"{dataset_name}."
        )


    # -----------------------------------------------------------------------
    # Success logging
    # -----------------------------------------------------------------------

    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        job_context=job_context,
        task_key=task_key,
        notebook_name=notebook_name,
        cell_name="silver_transformation",
        status="SUCCESS",
        domain=domain,
        dataset=dataset_name,
    )


except Exception as exc:

    # -----------------------------------------------------------------------
    # Failure logging
    # -----------------------------------------------------------------------

    log_cell(
            spark=spark,
            catalog=catalog,
            environment=environment,
            job_context=job_context,
            task_key=task_key,
            notebook_name=notebook_name,
            cell_name="silver_transformation",
            status="FAILED",
            domain=domain,
            dataset=dataset_name,
            error_type=type(exc).__name__,
            error_message=str(exc),
    )

    raise
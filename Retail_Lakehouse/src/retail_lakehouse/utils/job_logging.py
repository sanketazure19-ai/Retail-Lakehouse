import uuid
from datetime import datetime

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_job_context(dbutils) -> dict:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    def get_context_value(name: str) -> str | None:
        try:
            value = getattr(context, name)()
            return value.get() if value.isDefined() else None
        except Exception:
            return None

    def get_tag_value(name: str) -> str | None:
        try:
            tags = context.tags()
            value = tags.get(name)
            return value.get() if value.isDefined() else None
        except Exception:
            return None

    job_id = (
        get_tag_value("jobId")
        or get_context_value("jobId")
    )

    run_id = (
        get_tag_value("jobRunId")
        or get_context_value("currentRunId")
    )

    task_run_id = (
        get_tag_value("taskRunId")
        or get_context_value("currentRunId")
    )

    return {
        "job_id": job_id,
        "run_id": run_id,
        "task_run_id": task_run_id,
    }

def log_cell(
    spark,
    catalog: str,
    environment: str,
    context: dict,
    task_key: str,
    notebook_name: str,
    cell_name: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    domain: str | None = None,
    dataset: str | None = None,
    rows_processed: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:

    duration_ms = int(
        (end_time - start_time).total_seconds() * 1000
    )

    row = [(
        str(uuid.uuid4()),
        environment,
        context.get("job_id"),
        context.get("run_id"),
        context.get("task_run_id"),
        task_key,
        notebook_name,
        cell_name,
        domain,
        dataset,
        status,
        start_time,
        end_time,
        duration_ms,
        rows_processed,
        error_type,
        error_message,
        datetime.now(start_time.tzinfo),
    )]

    schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("environment", StringType(), True),

        StructField("job_id", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("task_run_id", StringType(), True),
        StructField("task_key", StringType(), True),

        StructField("notebook_name", StringType(), True),
        StructField("cell_name", StringType(), True),

        StructField("domain", StringType(), True),
        StructField("dataset", StringType(), True),

        StructField("status", StringType(), True),

        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_ms", LongType(), True),

        StructField("rows_processed", LongType(), True),

        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),

        StructField("created_at", TimestampType(), True),
    ])

    log_df = spark.createDataFrame(
        row,
        schema=schema,
    )

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            f"{catalog}.monitoring.databricks_job_logs"
        )
    )
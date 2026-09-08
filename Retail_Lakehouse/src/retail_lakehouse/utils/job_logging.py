import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F


def get_job_context(dbutils) -> dict:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    def get_context_value(name: str) -> str | None:
        try:
            value = getattr(context, name)()
            return value.get() if value.isDefined() else None
        except Exception:
            return None

    return {
        "job_id": get_context_value("jobId"),
        "run_id": get_context_value("currentRunId"),
        "task_run_id": get_context_value("currentRunId"),
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
        datetime.now(timezone.utc),
    )]

    columns = [
        "log_id",
        "environment",
        "job_id",
        "run_id",
        "task_run_id",
        "task_key",
        "notebook_name",
        "cell_name",
        "domain",
        "dataset",
        "status",
        "start_time",
        "end_time",
        "duration_ms",
        "rows_processed",
        "error_type",
        "error_message",
        "created_at",
    ]

    log_df = spark.createDataFrame(row, columns)

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            f"{catalog}.monitoring.databricks_job_logs"
        )
    )
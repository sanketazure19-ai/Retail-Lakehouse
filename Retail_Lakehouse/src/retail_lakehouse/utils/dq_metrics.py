from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DQ_METRICS_SCHEMA = [
    "environment",
    "dataset",
    "batch_id",
    "micro_batch_id",
    "total_records",
    "valid_records",
    "quarantined_records",
    "dq_failure_rate_percent",
    "dq_failure_threshold_percent",
    "dq_status",
    "recorded_at",
]


def calculate_dq_metrics(
    batch_df: DataFrame,
    environment: str,
    dataset: str,
    batch_id: str,
    micro_batch_id: int,
    threshold_percent: float,
) -> dict:
    total_records = batch_df.count()

    if total_records == 0:
        return {
            "environment": environment,
            "dataset": dataset,
            "batch_id": batch_id,
            "micro_batch_id": micro_batch_id,
            "total_records": 0,
            "valid_records": 0,
            "quarantined_records": 0,
            "dq_failure_rate_percent": 0.0,
            "dq_failure_threshold_percent": threshold_percent,
            "dq_status": "PASS",
            "recorded_at": datetime.now(timezone.utc),
        }

    quarantined_records = (
        batch_df
        .filter(F.col("_quality_status") == "QUARANTINE")
        .count()
    )

    valid_records = (
        batch_df
        .filter(F.col("_quality_status") == "VALID")
        .count()
    )

    failure_rate_percent = (
        quarantined_records / total_records
    ) * 100.0

    dq_status = (
        "FAIL"
        if failure_rate_percent > threshold_percent
        else "PASS"
    )

    return {
        "environment": environment,
        "dataset": dataset,
        "batch_id": batch_id,
        "micro_batch_id": micro_batch_id,
        "total_records": total_records,
        "valid_records": valid_records,
        "quarantined_records": quarantined_records,
        "dq_failure_rate_percent": failure_rate_percent,
        "dq_failure_threshold_percent": threshold_percent,
        "dq_status": dq_status,
        "recorded_at": datetime.now(timezone.utc),
    }


def write_dq_metrics(spark, catalog: str, metrics: dict) -> None:
    target_table = f"{catalog}.monitoring.bronze_dq_metrics"

    metrics_df = spark.createDataFrame(
        [(
            metrics["environment"],
            metrics["dataset"],
            metrics["batch_id"],
            int(metrics["micro_batch_id"]),
            int(metrics["total_records"]),
            int(metrics["valid_records"]),
            int(metrics["quarantined_records"]),
            float(metrics["dq_failure_rate_percent"]),
            float(metrics["dq_failure_threshold_percent"]),
            str(metrics["dq_status"]),
            metrics["recorded_at"],
        )],
        schema="""
            environment STRING,
            dataset STRING,
            batch_id STRING,
            micro_batch_id BIGINT,
            total_records BIGINT,
            valid_records BIGINT,
            quarantined_records BIGINT,
            dq_failure_rate_percent DOUBLE,
            dq_failure_threshold_percent DOUBLE,
            dq_status STRING,
            recorded_at TIMESTAMP
        """,
    )

    metrics_df.createOrReplaceTempView("bronze_dq_metrics_source")

    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING bronze_dq_metrics_source AS source
        ON  target.environment = source.environment
        AND target.dataset = source.dataset
        AND target.batch_id = source.batch_id
        AND target.micro_batch_id = source.micro_batch_id

        WHEN MATCHED THEN UPDATE SET
            target.total_records = source.total_records,
            target.valid_records = source.valid_records,
            target.quarantined_records = source.quarantined_records,
            target.dq_failure_rate_percent =
                source.dq_failure_rate_percent,
            target.dq_failure_threshold_percent =
                source.dq_failure_threshold_percent,
            target.dq_status = source.dq_status,
            target.recorded_at = source.recorded_at

        WHEN NOT MATCHED THEN INSERT (
            environment,
            dataset,
            batch_id,
            micro_batch_id,
            total_records,
            valid_records,
            quarantined_records,
            dq_failure_rate_percent,
            dq_failure_threshold_percent,
            dq_status,
            recorded_at
        )
        VALUES (
            source.environment,
            source.dataset,
            source.batch_id,
            source.micro_batch_id,
            source.total_records,
            source.valid_records,
            source.quarantined_records,
            source.dq_failure_rate_percent,
            source.dq_failure_threshold_percent,
            source.dq_status,
            source.recorded_at
        )
        """
    )
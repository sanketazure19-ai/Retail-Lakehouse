from pyspark.sql import DataFrame


def ingest_to_bronze(
    spark,
    source_path: str,
    schema_path: str,
    checkpoint_path: str,
    target_table: str,
    quarantine_table: str,
    source_schema,
    required_columns: list[str],
    environment: str,
    batch_id: str,
    validation_rules: list | None = None,
    catalog: str | None = None,
    dataset_name: str | None = None,
    dq_failure_threshold_percent: float = 10.0,
) -> None:

    from retail_lakehouse.utils.dq_metrics import (
        calculate_dq_metrics,
        write_dq_metrics,
    )
    from retail_lakehouse.utils.metadata import add_ingestion_metadata
    from retail_lakehouse.utils.quality import add_quality_columns

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .schema(source_schema)
        .load(source_path)
    )

    actual_columns = set(df.columns)
    missing_columns = set(required_columns) - actual_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    df = add_ingestion_metadata(
        df,
        environment=environment,
        batch_id=batch_id,
    )

    df = add_quality_columns(
        df,
        required_columns=required_columns,
        validation_rules=validation_rules,
    )

    def process_batch(
        batch_df: DataFrame,
        micro_batch_id: int,
    ) -> None:

        batch_df.cache()

        try:
            metrics = calculate_dq_metrics(
                        batch_df=batch_df,
                        environment=environment,
                        dataset=dataset_name or "unknown",
                        batch_id=batch_id,
                        micro_batch_id=micro_batch_id,
                        threshold_percent=dq_failure_threshold_percent,
            )

            print(
                "DQ metrics: "
                f"dataset={metrics['dataset']}, "
                f"micro_batch_id={metrics['micro_batch_id']}, "
                f"total={metrics['total_records']}, "
                f"valid={metrics['valid_records']}, "
                f"quarantined={metrics['quarantined_records']}, "
                f"failure_rate={metrics['dq_failure_rate_percent']:.2f}%, "
                f"threshold={metrics['dq_failure_threshold_percent']:.2f}%, "
                f"status={metrics['dq_status']}"
            )

            if catalog:
                write_dq_metrics(
                    spark=spark,
                    catalog=catalog,
                    metrics=metrics,
                )

            if metrics["dq_status"] == "FAIL":
                raise ValueError(
                    "DQ circuit breaker triggered for "
                    f"dataset '{dataset_name}'. "
                    f"Failure rate "
                    f"{metrics['dq_failure_rate_percent']:.2f}% "
                    f"exceeded threshold "
                    f"{metrics['dq_failure_threshold_percent']:.2f}%."
                )

            valid_df = (
                batch_df
                .filter("_quality_status = 'VALID'")
                .drop("_quality_status", "_quality_reason")
            )

            quarantine_df = batch_df.filter(
                "_quality_status = 'QUARANTINE'"
            )

            (
                valid_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )

            (
                quarantine_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(quarantine_table)
            )

        finally:
            batch_df.unpersist()

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
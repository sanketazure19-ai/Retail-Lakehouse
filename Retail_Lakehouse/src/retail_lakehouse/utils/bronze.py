from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
    validation_rules: list[dict] | None = None,
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
        dataset = dataset_name or target_table

        metrics = calculate_dq_metrics(
            batch_df=batch_df,
            environment=environment,
            dataset=dataset,
            batch_id=batch_id,
            micro_batch_id=micro_batch_id,
            threshold_percent=dq_failure_threshold_percent,
        )

        print(
            "DQ metrics: "
            f"total={metrics['total_records']}, "
            f"valid={metrics['valid_records']}, "
            f"quarantined={metrics['quarantined_records']}, "
            f"failure_rate="
            f"{metrics['dq_failure_rate_percent']:.2f}%, "
            f"threshold="
            f"{metrics['dq_failure_threshold_percent']:.2f}%, "
            f"status={metrics['dq_status']}"
        )

        # Always persist DQ metrics before deciding whether
        # the batch can enter Bronze.
        if catalog:
            write_dq_metrics(
                spark=spark,
                catalog=catalog,
                metrics=metrics,
            )

        # ------------------------------------------------------------------
        # DQ FAIL
        #
        # Preserve the complete failed micro-batch in quarantine.
        # Bronze remains untouched.
        #
        # We intentionally return instead of raising an exception so that
        # Auto Loader can commit the checkpoint and continue processing
        # subsequent files.
        # ------------------------------------------------------------------
        if metrics["dq_status"] == "FAIL":
            failed_batch_df = (
                batch_df
                .withColumn(
                    "_quarantine_reason",
                    F.lit(
                        "DQ circuit breaker failure: "
                        f"{metrics['dq_failure_rate_percent']:.2f}% "
                        "failure rate exceeded "
                        f"{dq_failure_threshold_percent:.2f}% threshold"
                    ),
                )
                .withColumn(
                    "_quarantine_timestamp",
                    F.current_timestamp(),
                )
                .withColumn(
                    "_quarantine_batch_id",
                    F.lit(batch_id),
                )
            )

            failed_batch_df.write.format("delta").mode("append").saveAsTable(
                quarantine_table
            )

            print(
                f"DQ circuit breaker triggered for dataset '{dataset}'. "
                f"Batch quarantined and Bronze write blocked. "
                f"Failure rate="
                f"{metrics['dq_failure_rate_percent']:.2f}%, "
                f"threshold={dq_failure_threshold_percent:.2f}%."
            )

            return

        # ------------------------------------------------------------------
        # DQ PASS
        #
        # Valid records enter Bronze.
        # Invalid records are retained in the normal quarantine table.
        # ------------------------------------------------------------------
        valid_df = (
            batch_df
            .filter("_quality_status = 'VALID'")
            .drop("_quality_status", "_quality_reason")
        )

        quarantine_df = batch_df.filter(
            "_quality_status = 'QUARANTINE'"
        )

        if valid_df.limit(1).count() > 0:
            valid_df.write.format("delta").mode("append").saveAsTable(
                target_table
            )

        if quarantine_df.limit(1).count() > 0:
            quarantine_df.write.format("delta").mode("append").saveAsTable(
                quarantine_table
            )

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
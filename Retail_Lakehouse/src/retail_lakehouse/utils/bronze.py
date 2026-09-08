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
) -> None:

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

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
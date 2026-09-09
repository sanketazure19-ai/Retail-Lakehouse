from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def reprocess_quarantine_batch(
    spark,
    quarantine_table: str,
    target_table: str,
    dataset_name: str,
    batch_id: str,
    required_columns: list[str],
    validation_rules: list[dict] | None = None,
) -> dict:
    from retail_lakehouse.utils.quality import add_quality_columns

    quarantine_df = (
        spark.table(quarantine_table)
        .filter(F.col("_environment").isNotNull())
        .filter(F.col("_batch_id") == batch_id)
    )

    if quarantine_df.limit(1).count() == 0:
        raise ValueError(
            f"No quarantined records found for dataset "
            f"'{dataset_name}' and batch '{batch_id}'."
        )

    # Remove Phase-2-only quarantine metadata before revalidation.
    reprocess_df = quarantine_df.drop(
        "_quarantine_reason",
        "_quarantine_timestamp",
        "_quarantine_batch_id",
    )

    # Reapply the same DQ rules.
    reprocess_df = add_quality_columns(
        reprocess_df,
        required_columns=required_columns,
        validation_rules=validation_rules,
    )

    valid_df = (
        reprocess_df
        .filter(F.col("_quality_status") == "VALID")
        .drop("_quality_status", "_quality_reason")
    )

    invalid_df = reprocess_df.filter(
        F.col("_quality_status") == "QUARANTINE"
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    # ------------------------------------------------------------------
    # Idempotency
    #
    # Use source file + original batch as the identity of a reprocessed
    # record. Records already present in Bronze are not inserted again.
    # ------------------------------------------------------------------
    if valid_count > 0:
        if spark.catalog.tableExists(target_table):
            bronze_df = spark.table(target_table)

            if "_source_file" in bronze_df.columns:
                existing_keys = bronze_df.select(
                    "_source_file",
                    "_batch_id",
                ).distinct()

                valid_df = valid_df.join(
                    existing_keys,
                    on=["_source_file", "_batch_id"],
                    how="left_anti",
                )

        new_valid_count = valid_df.count()

        if new_valid_count > 0:
            valid_df.write.format("delta").mode("append").saveAsTable(
                target_table
            )
    else:
        new_valid_count = 0

    # Preserve records that still fail validation.
    if invalid_count > 0:
        (
            invalid_df
            .withColumn(
                "_quarantine_reason",
                F.coalesce(
                    F.col("_quality_reason"),
                    F.lit("DQ validation failed during reprocessing"),
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
            .drop("_quality_status", "_quality_reason")
            .write.format("delta")
            .mode("append")
            .saveAsTable(quarantine_table)
        )

    result = {
        "dataset": dataset_name,
        "batch_id": batch_id,
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "new_bronze_records": new_valid_count,
        "status": "PASS" if invalid_count == 0 else "PARTIAL",
    }

    print(
        f"DQ reprocessing result: "
        f"dataset={dataset_name}, "
        f"batch_id={batch_id}, "
        f"valid={valid_count}, "
        f"invalid={invalid_count}, "
        f"new_bronze={new_valid_count}, "
        f"status={result['status']}"
    )

    return result
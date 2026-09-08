from pyspark.sql import functions as F


def ingest_promotions(
    spark,
    source_path: str,
    target_table: str,
    source_schema,
    environment: str,
) -> None:

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(source_schema)
        .load(source_path)
    )

    print(f"Promotions source row count: {df.count()}")

    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_date", F.current_date())
        .withColumn("_batch_id", F.lit("promotions_batch"))
        .withColumn("_environment", F.lit(environment))
    )

    required_columns = [
        "promotion_id",
        "promotion_name",
        "product_id",
        "start_date",
        "end_date",
        "discount_percent",
        "promotion_type",
        "status",
    ]

    invalid_condition = None

    for column in required_columns:
        condition = (
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == "")
        )

        if invalid_condition is None:
            invalid_condition = condition
        else:
            invalid_condition = (
                invalid_condition | condition
            )

    valid_df = df.filter(~invalid_condition)

    print(
        f"Promotions valid row count: "
        f"{valid_df.count()}"
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(
        f"Promotions written to: {target_table}"
    )
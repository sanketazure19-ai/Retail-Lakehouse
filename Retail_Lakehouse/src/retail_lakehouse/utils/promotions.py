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
        .load(source_path)
    )

    print(
        f"Promotions source row count: "
        f"{df.count()}"
    )

    df = (
        df
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path"),
        )
        .withColumn(
            "promotion_id",
            F.col("promotion_id").cast("string"),
        )
        .withColumn(
            "promotion_name",
            F.col("promotion_name").cast("string"),
        )
        .withColumn(
            "product_id",
            F.col("product_id").cast("string"),
        )
        .withColumn(
            "start_date",
            F.to_date(
                F.col("start_date"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "end_date",
            F.to_date(
                F.col("end_date"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "discount_percent",
            F.col("discount_percent").cast("int"),
        )
        .withColumn(
            "promotion_type",
            F.col("promotion_type").cast("string"),
        )
        .withColumn(
            "status",
            F.col("status").cast("string"),
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_load_date",
            F.current_date(),
        )
        .withColumn(
            "_batch_id",
            F.lit("promotions_batch"),
        )
        .withColumn(
            "_environment",
            F.lit(environment),
        )
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
            | (
                F.trim(
                    F.col(column).cast("string")
                ) == ""
            )
        )

        if invalid_condition is None:
            invalid_condition = condition
        else:
            invalid_condition = (
                invalid_condition | condition
            )

    valid_df = df.filter(
        ~invalid_condition
    )

    print(
        f"Promotions valid row count: "
        f"{valid_df.count()}"
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .saveAsTable(target_table)
    )

    print(
        f"Promotions written to: "
        f"{target_table}"
    )
from pyspark.sql import DataFrame


def ingest_promotions(
    spark,
    source_path: str,
    target_table: str,
    source_schema,
    environment: str,
) -> None:

    from retail_lakehouse.utils.metadata import add_ingestion_metadata
    from retail_lakehouse.utils.quality import add_quality_columns

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(source_schema)
        .load(source_path)
    )

    df = add_ingestion_metadata(
        df,
        environment=environment,
        batch_id="promotions_batch",
    )

    df = add_quality_columns(
        df,
        required_columns=[
            "promotion_id",
            "promotion_name",
            "product_id",
            "start_date",
            "end_date",
            "discount_percent",
            "promotion_type",
            "status",
        ],
    )

    valid_df = (
        df
        .filter("_quality_status = 'VALID'")
        .drop("_quality_status", "_quality_reason")
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
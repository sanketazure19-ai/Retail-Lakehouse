from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def standardize_columns(df: DataFrame) -> DataFrame:
    return df.select(
        *[
            F.col(column).alias(column.strip().lower())
            for column in df.columns
        ]
    )


def deduplicate(
    df: DataFrame,
    key_columns: list[str],
) -> DataFrame:

    window = Window.partitionBy(*key_columns).orderBy(
        F.col("_ingestion_timestamp").desc(),
        F.col("_batch_id").desc(),
    )

    return (
        df
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def add_silver_metadata(
    df: DataFrame,
    environment: str,
) -> DataFrame:

    return (
        df
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_environment", F.lit(environment))
    )


def get_unprocessed_batches(
    spark,
    source_table: str,
    control_table: str,
    dataset_name: str,
) -> list[str]:

    source_batches = (
        spark.table(source_table)
        .select("_batch_id")
        .where(F.col("_batch_id").isNotNull())
        .distinct()
    )

    if not spark.catalog.tableExists(control_table):
        return [
            row["_batch_id"]
            for row in source_batches.collect()
        ]

    processed_batches = (
        spark.table(control_table)
        .filter(F.col("dataset") == dataset_name)
        .select("_batch_id")
        .distinct()
    )

    return [
        row["_batch_id"]
        for row in (
            source_batches
            .join(
                processed_batches,
                on="_batch_id",
                how="left_anti",
            )
            .collect()
        )
    ]


def merge_to_silver(
    spark,
    df: DataFrame,
    target_table: str,
    key_columns: list[str],
) -> None:

    if not spark.catalog.tableExists(target_table):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        return

    target = DeltaTable.forName(
        spark,
        target_table,
    )

    merge_condition = " AND ".join(
        [
            f"target.{column} = source.{column}"
            for column in key_columns
        ]
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def mark_batches_processed(
    spark,
    control_table: str,
    dataset_name: str,
    batch_ids: list[str],
) -> None:

    rows = [
        (
            dataset_name,
            batch_id,
        )
        for batch_id in batch_ids
    ]

    control_df = spark.createDataFrame(
        rows,
        [
            "dataset",
            "_batch_id",
        ],
    ).withColumn(
        "_processed_timestamp",
        F.current_timestamp(),
    )

    (
        control_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(control_table)
    )


def transform_silver(
    df: DataFrame,
    key_columns: list[str],
    environment: str,
) -> DataFrame:

    df = standardize_columns(df)

    df = deduplicate(
        df,
        key_columns=key_columns,
    )

    return add_silver_metadata(
        df,
        environment=environment,
    )
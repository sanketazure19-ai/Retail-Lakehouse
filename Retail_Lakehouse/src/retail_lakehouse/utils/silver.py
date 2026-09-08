from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
    return df.dropDuplicates(key_columns)


def add_silver_metadata(
    df: DataFrame,
    environment: str,
) -> DataFrame:
    return (
        df
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_environment", F.lit(environment))
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
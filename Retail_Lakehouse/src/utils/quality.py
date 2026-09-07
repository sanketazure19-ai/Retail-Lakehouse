from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def find_null_records(
    df: DataFrame,
    required_columns: list[str],
) -> DataFrame:
    condition = None

    for column in required_columns:
        current_condition = F.col(column).isNull()

        condition = (
            current_condition
            if condition is None
            else condition | current_condition
        )

    return df.filter(condition)


def add_quality_status(
    df: DataFrame,
    required_columns: list[str],
) -> DataFrame:
    condition = None

    for column in required_columns:
        current_condition = F.col(column).isNull()

        condition = (
            current_condition
            if condition is None
            else condition | current_condition
        )

    return df.withColumn(
        "_quality_status",
        F.when(condition, F.lit("QUARANTINE"))
        .otherwise(F.lit("VALID")),
    )
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quality_columns(
    df: DataFrame,
    required_columns: list[str],
) -> DataFrame:

    conditions = []

    for column in required_columns:
        conditions.append(
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == "")
        )

    invalid_condition = conditions[0]

    for condition in conditions[1:]:
        invalid_condition = invalid_condition | condition

    quality_reason = None

    for column in required_columns:
        current_reason = F.when(
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == ""),
            F.lit(f"{column} is null or empty"),
        )

        quality_reason = (
            current_reason
            if quality_reason is None
            else quality_reason.otherwise(current_reason)
        )

    return (
        df
        .withColumn(
            "_quality_status",
            F.when(
                invalid_condition,
                F.lit("QUARANTINE"),
            ).otherwise(F.lit("VALID")),
        )
        .withColumn(
            "_quality_reason",
            quality_reason,
        )
    )
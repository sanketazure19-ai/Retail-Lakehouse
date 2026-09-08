from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quality_columns(
    df: DataFrame,
    required_columns: list[str],
) -> DataFrame:

    invalid_conditions = [
        F.col(column).isNull()
        | (F.trim(F.col(column).cast("string")) == "")
        for column in required_columns
    ]

    if not invalid_conditions:
        return (
            df
            .withColumn("_quality_status", F.lit("VALID"))
            .withColumn("_quality_reason", F.lit(None).cast("string"))
        )

    invalid_condition = invalid_conditions[0]

    for condition in invalid_conditions[1:]:
        invalid_condition = invalid_condition | condition

    quality_reason = F.lit(None).cast("string")

    for column in reversed(required_columns):
        condition = (
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == "")
        )

        quality_reason = F.when(
            condition,
            F.lit(f"{column} is null or empty"),
        ).otherwise(quality_reason)

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
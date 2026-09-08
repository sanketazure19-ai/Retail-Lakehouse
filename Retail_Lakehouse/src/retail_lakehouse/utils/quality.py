from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quality_columns(
    df: DataFrame,
    required_columns: list[str],
    validation_rules: list[dict] | None = None,
) -> DataFrame:

    invalid_conditions = []
    reason_conditions = []

    # Required-column validation
    for column in required_columns:
        condition = (
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == "")
        )

        invalid_conditions.append(condition)
        reason_conditions.append(
            (
                condition,
                f"{column} is null or empty",
            )
        )

    # Dataset-specific validation rules
    for rule in validation_rules or []:

        column = rule["column"]
        rule_type = rule["rule"]
        reason = rule["reason"]

        if rule_type == "regex":
            condition = (
                F.col(column).isNotNull()
                & (F.trim(F.col(column).cast("string")) != "")
                & ~F.col(column).cast("string").rlike(rule["pattern"])
            )

        elif rule_type == "greater_than":
            condition = (
                F.col(column).isNotNull()
                & (F.col(column) <= F.lit(rule["value"]))
            )

        elif rule_type == "greater_than_or_equal":
            condition = (
                F.col(column).isNotNull()
                & (F.col(column) < F.lit(rule["value"]))
            )

        elif rule_type == "not_empty":
            condition = (
                F.col(column).isNull()
                | (F.trim(F.col(column).cast("string")) == "")
            )

        else:
            raise ValueError(
                f"Unsupported validation rule: {rule_type}"
            )

        invalid_conditions.append(condition)
        reason_conditions.append(
            (
                condition,
                reason,
            )
        )

    if not invalid_conditions:
        return (
            df
            .withColumn("_quality_status", F.lit("VALID"))
            .withColumn(
                "_quality_reason",
                F.lit(None).cast("string"),
            )
        )

    invalid_condition = invalid_conditions[0]

    for condition in invalid_conditions[1:]:
        invalid_condition = invalid_condition | condition

    quality_reason = F.lit(None).cast("string")

    for condition, reason in reversed(reason_conditions):
        quality_reason = F.when(
            condition,
            F.lit(reason),
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
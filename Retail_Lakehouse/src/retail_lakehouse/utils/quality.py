from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quality_columns(df: DataFrame) -> DataFrame:
    """
    Apply customer-level data quality rules.

    Rules:
    - customer_id must be non-null and non-empty
    - email must be non-null and non-empty
    - signup_date must be valid
    - customer_segment must be non-null and non-empty
    """

    customer_id_invalid = (
        F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == "")
    )

    email_invalid = (
        F.col("email").isNull()
        | (F.trim(F.col("email")) == "")
    )

    signup_date_invalid = F.col("signup_date").isNull()

    customer_segment_invalid = (
        F.col("customer_segment").isNull()
        | (F.trim(F.col("customer_segment")) == "")
    )

    quality_reason = (
        F.when(customer_id_invalid, F.lit("customer_id is null or empty"))
        .when(email_invalid, F.lit("email is null or empty"))
        .when(signup_date_invalid, F.lit("signup_date is null or invalid"))
        .when(
            customer_segment_invalid,
            F.lit("customer_segment is null or empty"),
        )
    )

    quality_status = F.when(
        customer_id_invalid
        | email_invalid
        | signup_date_invalid
        | customer_segment_invalid,
        F.lit("QUARANTINE"),
    ).otherwise(F.lit("VALID"))

    return (
        df
        .withColumn("_quality_status", quality_status)
        .withColumn("_quality_reason", quality_reason)
    )
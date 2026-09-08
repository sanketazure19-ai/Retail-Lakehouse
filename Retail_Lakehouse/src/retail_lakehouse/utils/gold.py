from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_orders(
    orders_df: DataFrame,
) -> DataFrame:

    return (
        orders_df
        .withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price"),
        )
        .withColumn(
            "net_sales_amount",
            (
                F.col("quantity") * F.col("unit_price")
            ) - F.coalesce(
                F.col("discount_amount"),
                F.lit(0.0),
            ),
        )
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price",
            "discount_amount",
            "gross_amount",
            "net_sales_amount",
            "payment_method",
            "order_status",
            "_ingestion_timestamp",
            "_source_file",
        )
    )


def build_fact_returns(
    returns_df: DataFrame,
) -> DataFrame:

    return (
        returns_df
        .select(
            "return_id",
            "order_id",
            "customer_id",
            "product_id",
            "return_date",
            "return_quantity",
            "refund_amount",
            "return_reason",
            "return_status",
            "_ingestion_timestamp",
            "_source_file",
        )
    )


def build_dim_customer(
    customers_df: DataFrame,
) -> DataFrame:

    return (
        customers_df
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "state",
            "country",
            "signup_date",
            "customer_segment",
        )
    )


def build_dim_product(
    products_df: DataFrame,
) -> DataFrame:

    return (
        products_df
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
            "currency",
        )
    )


def build_dim_date(
    spark,
    start_date: str,
    end_date: str,
) -> DataFrame:

    dates = (
        spark.sql(
            f"""
            SELECT explode(
                sequence(
                    to_date('{start_date}'),
                    to_date('{end_date}'),
                    interval 1 day
                )
            ) AS date
            """
        )
    )

    return (
        dates
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
    )
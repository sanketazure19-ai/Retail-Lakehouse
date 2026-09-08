from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_customer_scd2_source(
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
        .withColumn(
            "effective_start_date",
            F.current_date(),
        )
        .withColumn(
            "effective_end_date",
            F.lit("9999-12-31").cast("date"),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
    )


def merge_customer_scd2(
    spark,
    source_df: DataFrame,
    target_table: str,
) -> None:

    if not spark.catalog.tableExists(target_table):

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        return

    target = DeltaTable.forName(
        spark,
        target_table,
    )

    target_df = spark.table(target_table)

    compare_columns = [
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "country",
        "signup_date",
        "customer_segment",
    ]

    change_condition = " OR ".join(
        [
            f"""
            NOT (target.{column} <=> source.{column})
            """
            for column in compare_columns
        ]
    )

    changed_customers = (
        source_df.alias("source")
        .join(
            target_df.alias("target"),
            (
                F.col("source.customer_id")
                == F.col("target.customer_id")
            )
            & F.col("target.is_current"),
            "inner",
        )
        .where(F.expr(change_condition))
        .select("source.*")
    )

    if changed_customers.limit(1).count() > 0:

        (
            target.alias("target")
            .merge(
                changed_customers.alias("source"),
                """
                target.customer_id = source.customer_id
                AND target.is_current = true
                """,
            )
            .whenMatchedUpdate(
                set={
                    "effective_end_date": "source.effective_start_date",
                    "is_current": "false",
                }
            )
            .execute()
        )

    new_customers = (
        source_df.alias("source")
        .join(
            target_df.alias("target"),
            (
                F.col("source.customer_id")
                == F.col("target.customer_id")
            )
            & F.col("target.is_current"),
            "left_anti",
        )
    )

    if new_customers.limit(1).count() > 0:

        (
            new_customers.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

    changed_customers_to_insert = (
        changed_customers
        .withColumn(
            "effective_start_date",
            F.current_date(),
        )
        .withColumn(
            "effective_end_date",
            F.lit("9999-12-31").cast("date"),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
    )

    if changed_customers_to_insert.limit(1).count() > 0:

        (
            changed_customers_to_insert.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )


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

def build_customer_sales_summary(
    orders_df: DataFrame,
) -> DataFrame:

    return (
        orders_df
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount").alias("gross_sales_amount"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum("net_sales_amount").alias("net_sales_amount"),
        )
    )


def build_product_sales_summary(
    orders_df: DataFrame,
) -> DataFrame:

    return (
        orders_df
        .groupBy("product_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount").alias("gross_sales_amount"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum("net_sales_amount").alias("net_sales_amount"),
        )
    )
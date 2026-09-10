# Databricks notebook source

from retail_lakehouse.config.settings import get_environment
from retail_lakehouse.utils.gold import (
    build_customer_scd2_source,
    build_dim_date,
    build_dim_product,
    build_fact_orders,
    build_fact_returns,
    build_customer_sales_summary,
    build_product_sales_summary,
    build_daily_sales_summary,
    build_monthly_sales_summary,
    build_customer_revenue_summary,
    build_product_revenue_summary,
    build_promotion_product_summary,
    merge_by_key,
    merge_customer_scd2,
)
from retail_lakehouse.utils.job_logging import log_cell


dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")

environment = dbutils.widgets.get("environment")
job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

job_context = {
    "job_id": job_id,
    "run_id": job_run_id,
    "task_run_id": task_run_id,
}

task_key = "gold_transformation"
notebook_name = "03_gold_transformation"

env_config = get_environment(environment)
catalog = env_config["catalog"]

silver_tables = {
    "customers": f"{catalog}.silver.customers",
    "products": f"{catalog}.silver.products",
    "orders": f"{catalog}.silver.orders",
    "returns": f"{catalog}.silver.returns",
    "promotions": f"{catalog}.silver.promotions",
}

gold_tables = {
    "dim_customer": f"{catalog}.gold.dim_customer",
    "dim_product": f"{catalog}.gold.dim_product",
    "fact_orders": f"{catalog}.gold.fact_orders",
    "fact_returns": f"{catalog}.gold.fact_returns",
    "dim_date": f"{catalog}.gold.dim_date",
    "customer_sales_summary": f"{catalog}.gold.customer_sales_summary",
    "product_sales_summary": f"{catalog}.gold.product_sales_summary",
    "daily_sales_summary": f"{catalog}.gold.daily_sales_summary",
    "monthly_sales_summary": f"{catalog}.gold.monthly_sales_summary",
    "customer_revenue_summary": f"{catalog}.gold.customer_revenue_summary",
    "product_revenue_summary": f"{catalog}.gold.product_revenue_summary",
    "promotion_product_summary": f"{catalog}.gold.promotion_product_summary",
}

print(f"Environment: {environment}")
print(f"Catalog: {catalog}")
print(f"Silver customers: {silver_tables['customers']}")
print(f"Silver products: {silver_tables['products']}")
print(f"Silver orders: {silver_tables['orders']}")
print(f"Silver returns: {silver_tables['returns']}")
print(f"Silver promotions: {silver_tables['promotions']}")


try:
    customers_df = spark.table(silver_tables["customers"])
    products_df = spark.table(silver_tables["products"])
    orders_df = spark.table(silver_tables["orders"])
    returns_df = spark.table(silver_tables["returns"])
    promotions_df = spark.table(silver_tables["promotions"])

    print("Loaded Silver datasets.")
    print(f"Customers: {customers_df.count()}")
    print(f"Products: {products_df.count()}")
    print(f"Orders: {orders_df.count()}")
    print(f"Returns: {returns_df.count()}")
    print(f"Promotions: {promotions_df.count()}")

    # ------------------------------------------------------------------
    # Dimensions
    # ------------------------------------------------------------------

    customer_scd2_source = build_customer_scd2_source(customers_df)

    merge_customer_scd2(
        spark=spark,
        source_df=customer_scd2_source,
        target_table=gold_tables["dim_customer"],
    )

    dim_product_df = build_dim_product(products_df)

    merge_by_key(
        spark=spark,
        source_df=dim_product_df,
        target_table=gold_tables["dim_product"],
        key_columns=["product_id"],
    )

    dim_date_df = build_dim_date(
        spark=spark,
        start_date="2020-01-01",
        end_date="2030-12-31",
    )

    dim_date_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema",
        "true",
    ).saveAsTable(gold_tables["dim_date"])

    # ------------------------------------------------------------------
    # Canonical facts
    # ------------------------------------------------------------------

    fact_orders_df = build_fact_orders(orders_df)

    merge_by_key(
        spark=spark,
        source_df=fact_orders_df,
        target_table=gold_tables["fact_orders"],
        key_columns=["order_id"],
    )

    fact_returns_df = build_fact_returns(returns_df)

    merge_by_key(
        spark=spark,
        source_df=fact_returns_df,
        target_table=gold_tables["fact_returns"],
        key_columns=["return_id"],
    )

    # Read canonical Gold facts after merge.
    # Downstream summaries are therefore based on the same canonical facts
    # that consumers query.
    canonical_orders_df = spark.table(gold_tables["fact_orders"])
    canonical_returns_df = spark.table(gold_tables["fact_returns"])

    # ------------------------------------------------------------------
    # Aggregate / reporting tables
    # ------------------------------------------------------------------

    customer_sales_summary_df = build_customer_sales_summary(
        canonical_orders_df
    )

    product_sales_summary_df = build_product_sales_summary(
        canonical_orders_df
    )

    daily_sales_summary_df = build_daily_sales_summary(
        canonical_orders_df,
        canonical_returns_df,
    )

    monthly_sales_summary_df = build_monthly_sales_summary(
        canonical_orders_df,
        canonical_returns_df,
    )

    customer_revenue_summary_df = build_customer_revenue_summary(
        canonical_orders_df,
        canonical_returns_df,
    )

    product_revenue_summary_df = build_product_revenue_summary(
        canonical_orders_df,
        canonical_returns_df,
    )

    promotion_product_summary_df = build_promotion_product_summary(
        promotions_df,
        products_df,
    )

    summary_tables = {
        "customer_sales_summary": customer_sales_summary_df,
        "product_sales_summary": product_sales_summary_df,
        "daily_sales_summary": daily_sales_summary_df,
        "monthly_sales_summary": monthly_sales_summary_df,
        "customer_revenue_summary": customer_revenue_summary_df,
        "product_revenue_summary": product_revenue_summary_df,
        "promotion_product_summary": promotion_product_summary_df,
    }

    for table_name, dataframe in summary_tables.items():
        print(f"Writing {table_name}: {gold_tables[table_name]}")
        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(gold_tables[table_name])
        )

    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        job_context=job_context,
        task_key=task_key,
        notebook_name=notebook_name,
        cell_name="gold_transformation",
        status="SUCCESS",
        domain="gold",
        dataset="gold",
    )

    print("Gold transformation completed successfully.")

except Exception as exc:
    log_cell(
        spark=spark,
        catalog=catalog,
        environment=environment,
        job_context=job_context,
        task_key=task_key,
        notebook_name=notebook_name,
        cell_name="gold_transformation",
        status="FAILED",
        domain="gold",
        dataset="gold",
        error_type=type(exc).__name__,
        error_message=str(exc),
    )
    raise

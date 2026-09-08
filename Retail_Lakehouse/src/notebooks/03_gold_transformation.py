# Databricks notebook source

from retail_lakehouse.config.settings import get_environment
from retail_lakehouse.utils.gold import (
    build_customer_revenue_summary,
    build_customer_sales_summary,
    build_customer_scd2_source,
    build_dim_date,
    build_dim_product,
    build_fact_orders,
    build_fact_returns,
    build_monthly_sales_summary,
    build_daily_sales_summary,
    build_product_revenue_summary,
    build_product_sales_summary,
    build_promotion_product_summary,
    merge_customer_scd2,
)


# COMMAND ----------

dbutils.widgets.text("environment", "dev")

environment = dbutils.widgets.get("environment")

env_config = get_environment(environment)
catalog = env_config["catalog"]


# COMMAND ----------

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
    "promotion_product_summary": (f"{catalog}.gold.promotion_product_summary"),
}


# COMMAND ----------

customers_df = spark.table(
    silver_tables["customers"]
)

products_df = spark.table(
    silver_tables["products"]
)

orders_df = spark.table(
    silver_tables["orders"]
)

returns_df = spark.table(
    silver_tables["returns"]
)

promotions_df = spark.table(
    silver_tables["promotions"]
)

# COMMAND ----------

customer_scd2_df = build_customer_scd2_source(
    customers_df
)

dim_product_df = build_dim_product(
    products_df
)

fact_orders_df = build_fact_orders(
    orders_df
)

fact_returns_df = build_fact_returns(
    returns_df
)

customer_sales_summary_df = build_customer_sales_summary(
    fact_orders_df
)

product_sales_summary_df = build_product_sales_summary(
    fact_orders_df
)

daily_sales_summary_df = build_daily_sales_summary(
    fact_orders_df,
    fact_returns_df,
)

monthly_sales_summary_df = build_monthly_sales_summary(
    fact_orders_df,
    fact_returns_df,
)

customer_revenue_summary_df = build_customer_revenue_summary(
    fact_orders_df,
    fact_returns_df,
)

product_revenue_summary_df = build_product_revenue_summary(
    fact_orders_df,
    fact_returns_df,
)

promotion_product_summary_df = (
    build_promotion_product_summary(
        promotions_df,
        products_df,
    )
)

# COMMAND ----------

merge_customer_scd2(
    spark=spark,
    source_df=customer_scd2_df,
    target_table=gold_tables["dim_customer"],
)


# COMMAND ----------

(
    dim_product_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["dim_product"])
)

(
    fact_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["fact_orders"])
)

(
    fact_returns_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["fact_returns"])
)

(
    customer_sales_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["customer_sales_summary"])
)

(
    product_sales_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["product_sales_summary"])
)

(
    daily_sales_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["daily_sales_summary"])
)

(
    monthly_sales_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["monthly_sales_summary"])
)

(
    customer_revenue_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["customer_revenue_summary"])
)

(
    product_revenue_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["product_revenue_summary"])
)

(
    promotion_product_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        gold_tables["promotion_product_summary"]
    )
)


# COMMAND ----------

date_df = build_dim_date(
    spark=spark,
    start_date="2020-01-01",
    end_date="2030-12-31",
)

(
    date_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["dim_date"])
)


# COMMAND ----------

print("Gold transformation completed.")
print(f"Customer dimension: {gold_tables['dim_customer']}")
print(f"Product dimension: {gold_tables['dim_product']}")
print(f"Orders fact: {gold_tables['fact_orders']}")
print(f"Returns fact: {gold_tables['fact_returns']}")
print(f"Date dimension: {gold_tables['dim_date']}")
print(
    f"Customer sales summary: "
    f"{gold_tables['customer_sales_summary']}"
)
print(
    f"Product sales summary: "
    f"{gold_tables['product_sales_summary']}"
)
print(
    f"Daily sales summary: "
    f"{gold_tables['daily_sales_summary']}"
)

print(
    f"Monthly sales summary: "
    f"{gold_tables['monthly_sales_summary']}"
)

print(
    f"Customer revenue summary: "
    f"{gold_tables['customer_revenue_summary']}"
)

print(
    f"Product revenue summary: "
    f"{gold_tables['product_revenue_summary']}"
)
print(
    f"Promotion product summary: "
    f"{gold_tables['promotion_product_summary']}"
)
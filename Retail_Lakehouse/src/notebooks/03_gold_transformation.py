# Databricks notebook source

from pyspark.sql import functions as F

from retail_lakehouse.config.settings import get_environment
from retail_lakehouse.utils.gold import (
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_fact_orders,
    build_fact_returns,
)


# COMMAND ----------

dbutils.widgets.text("environment", "dev")

environment = dbutils.widgets.get("environment")

env_config = get_environment(environment)
catalog = env_config["catalog"]


# COMMAND ----------

bronze_silver_tables = {
    "customers": f"{catalog}.silver.customers",
    "products": f"{catalog}.silver.products",
    "orders": f"{catalog}.silver.orders",
    "returns": f"{catalog}.silver.returns",
}

gold_tables = {
    "dim_customer": f"{catalog}.gold.dim_customer",
    "dim_product": f"{catalog}.gold.dim_product",
    "fact_orders": f"{catalog}.gold.fact_orders",
    "fact_returns": f"{catalog}.gold.fact_returns",
    "dim_date": f"{catalog}.gold.dim_date",
}


# COMMAND ----------

customers_df = spark.table(
    bronze_silver_tables["customers"]
)

products_df = spark.table(
    bronze_silver_tables["products"]
)

orders_df = spark.table(
    bronze_silver_tables["orders"]
)

returns_df = spark.table(
    bronze_silver_tables["returns"]
)


# COMMAND ----------

dim_customer_df = build_dim_customer(
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


# COMMAND ----------

date_df = build_dim_date(
    spark=spark,
    start_date="2020-01-01",
    end_date="2030-12-31",
)


# COMMAND ----------

(
    dim_customer_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tables["dim_customer"])
)

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
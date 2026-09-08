# Databricks notebook source

from retail_lakehouse.config.settings import (
    get_datasets,
    get_environment,
)
from retail_lakehouse.utils.silver import transform_silver


# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("domain", "erp")
dbutils.widgets.text("dataset", "customers")

environment = dbutils.widgets.get("environment")
domain = dbutils.widgets.get("domain")
dataset_name = dbutils.widgets.get("dataset")

env_config = get_environment(environment)
datasets = get_datasets()

catalog = env_config["catalog"]

dataset_config = datasets[domain][dataset_name]

if dataset_config["ingestion"] != "autoloader":
    raise ValueError(
        f"Silver transformation for {dataset_name} is not configured "
        "for Auto Loader Bronze ingestion"
    )

source_table = f"{catalog}.bronze.{dataset_name}"
target_table = f"{catalog}.silver.{dataset_name}"


# COMMAND ----------

key_columns = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "clickstream": ["event_id"],
    "returns": ["return_id"],
}

if dataset_name not in key_columns:
    raise ValueError(
        f"Unsupported Silver dataset: {dataset_name}"
    )


# COMMAND ----------

print(f"Environment: {environment}")
print(f"Domain: {domain}")
print(f"Dataset: {dataset_name}")
print(f"Source table: {source_table}")
print(f"Target table: {target_table}")


# COMMAND ----------

bronze_df = spark.read.table(source_table)

silver_df = transform_silver(
    df=bronze_df,
    key_columns=key_columns[dataset_name],
    environment=environment,
)


# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Silver transformation completed: {target_table}")
# Databricks notebook source
from pyspark.sql import functions as F

from retail_lakehouse.config.settings import (
    get_datasets,
    get_environment,
)
from retail_lakehouse.utils.silver import (
    get_unprocessed_batches,
    mark_batches_processed,
    merge_to_silver,
    transform_silver,
)
from tests.conftest import spark


# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("domain", "erp")
dbutils.widgets.text("dataset", "customers")

environment = dbutils.widgets.get("environment")
domain = dbutils.widgets.get("domain")
dataset_name = dbutils.widgets.get("dataset")


# COMMAND ----------

env_config = get_environment(environment)
datasets = get_datasets()

catalog = env_config["catalog"]

dataset_config = datasets[domain][dataset_name]

if dataset_config["ingestion"] != "autoloader":
    raise ValueError(
        f"Unsupported Silver dataset: {dataset_name}"
    )

source_table = f"{catalog}.bronze.{dataset_name}"
target_table = f"{catalog}.silver.{dataset_name}"
control_table = f"{catalog}.silver.processed_batches"


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
print(f"Control table: {control_table}")


# COMMAND ----------

unprocessed_batches = get_unprocessed_batches(
    spark=spark,
    source_table=source_table,
    control_table=control_table,
    dataset_name=dataset_name,
)

print(f"Unprocessed batches: {len(unprocessed_batches)}")

if not unprocessed_batches:
    print("No new Bronze batches to process.")


# COMMAND ----------

if unprocessed_batches:

    bronze_df =  (
    spark.table(source_table)
    .filter(
        F.col("_batch_id").isin(unprocessed_batches)
    )
)

    silver_df = transform_silver(
        df=bronze_df,
        key_columns=key_columns[dataset_name],
        environment=environment,
    )

    merge_to_silver(
        spark=spark,
        df=silver_df,
        target_table=target_table,
        key_columns=key_columns[dataset_name],
    )

    mark_batches_processed(
        spark=spark,
        control_table=control_table,
        dataset_name=dataset_name,
        batch_ids=unprocessed_batches,
    )

    print(
        f"Processed {len(unprocessed_batches)} batch(es) "
        f"into {target_table}"
    )
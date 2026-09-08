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
    "promotions": ["promotion_id"],
}

if dataset_name not in key_columns:
    raise ValueError(
        f"Unsupported Silver dataset: {dataset_name}"
    )

# COMMAND ----------

print(f"Environment: {environment}")
print(f"Domain: {domain}")
print(f"Dataset: {dataset_name}")
print(f"Ingestion type: {dataset_config['ingestion']}")
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

print(
    f"Unprocessed batches: "
    f"{len(unprocessed_batches)}"
)

# COMMAND ----------

if not unprocessed_batches:

    print(
        f"No new Bronze batches to process "
        f"for {dataset_name}."
    )

else:

    bronze_df = (
        spark.table(source_table)
        .filter(
            F.col("_batch_id").isin(
                unprocessed_batches
            )
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
        f"Processed {len(unprocessed_batches)} "
        f"batch(es) into {target_table}"
    )
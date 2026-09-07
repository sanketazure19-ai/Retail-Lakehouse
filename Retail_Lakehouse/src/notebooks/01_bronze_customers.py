# Databricks notebook source

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

from pyspark.sql import functions as F
from retail_lakehouse.utils.quality import add_quality_columns
from retail_lakehouse.config.settings import get_environment, get_datasets
from retail_lakehouse.utils.helpers import build_raw_path, generate_batch_id
from retail_lakehouse.utils.metadata import add_ingestion_metadata
from retail_lakehouse.utils.validation import validate_required_columns


# COMMAND ----------

# Environment

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

env_config = get_environment(environment)
datasets = get_datasets()

catalog = env_config["catalog"]
raw_root = env_config["raw_root"]
checkpoint_root = env_config["checkpoint_root"]
schema_root = env_config["schema_root"]

dataset_path = datasets["erp"]["customers"]["path"]

raw_path = build_raw_path(raw_root, dataset_path)

schema_path = f"{schema_root}/erp/customers"
checkpoint_path = f"{checkpoint_root}/erp/customers"

target_table = f"{catalog}.bronze.customers"

print(f"Environment: {environment}")
print(f"RAW path: {raw_path}")
print(f"Schema path: {schema_path}")
print(f"Checkpoint path: {checkpoint_path}")
print(f"Target table: {target_table}")


# COMMAND ----------

# Explicit source schema

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("customer_segment", StringType(), True),
])


# COMMAND ----------

# Auto Loader

batch_id = generate_batch_id("customers")

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .schema(customer_schema)
    .load(raw_path)
)


# COMMAND ----------

# Structural validation

validate_required_columns(
    df,
    [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "country",
        "signup_date",
        "customer_segment",
    ],
)


# COMMAND ----------

# Add ingestion metadata

df = add_ingestion_metadata(
    df,
    environment=environment,
    batch_id=batch_id,
)

# COMMAND ----------

# Record-level data quality

df = add_quality_columns(df)


# COMMAND ----------


# COMMAND ----------

# Write valid and quarantined records

quarantine_table = f"{catalog}.bronze_quarantine.customers"


def process_batch(batch_df, batch_id):

    valid_df = batch_df.filter(
        F.col("_quality_status") == "VALID"
    )

    quarantine_df = batch_df.filter(
        F.col("_quality_status") == "QUARANTINE"
    )

    (
        valid_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(quarantine_table)
    )

    print(
        f"Processed micro-batch {batch_id}: "
        f"valid → {target_table}, "
        f"quarantine → {quarantine_table}"
    )


query = (
    df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print(f"Customers ingestion completed: {target_table}")
print(f"Customers quarantine completed: {quarantine_table}")
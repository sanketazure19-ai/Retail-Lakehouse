# Databricks notebook source

from retail_lakehouse.config.schemas import (
    CLICKSTREAM_SCHEMA,
    CUSTOMER_SCHEMA,
    ORDER_SCHEMA,
    PRODUCT_SCHEMA,
    RETURN_SCHEMA,
)
from retail_lakehouse.config.settings import get_datasets, get_environment
from retail_lakehouse.utils.bronze import ingest_to_bronze
from retail_lakehouse.utils.helpers import build_raw_path, generate_batch_id


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
raw_root = env_config["raw_root"]
checkpoint_root = env_config["checkpoint_root"]
schema_root = env_config["schema_root"]

dataset_config = datasets[domain][dataset_name]

raw_path = build_raw_path(
    raw_root,
    dataset_config["path"],
)

schema_path = f"{schema_root}/{domain}/{dataset_name}"
checkpoint_path = f"{checkpoint_root}/{domain}/{dataset_name}"

target_table = f"{catalog}.bronze.{dataset_name}"
quarantine_table = f"{catalog}.bronze_quarantine.{dataset_name}"

batch_id = generate_batch_id(dataset_name)


# COMMAND ----------

schemas = {
    "customers": CUSTOMER_SCHEMA,
    "products": PRODUCT_SCHEMA,
    "orders": ORDER_SCHEMA,
    "clickstream": CLICKSTREAM_SCHEMA,
    "returns": RETURN_SCHEMA,
}

required_columns = {
    "customers": [
        "customer_id",
        "email",
        "signup_date",
        "customer_segment",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "currency",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "payment_method",
        "order_status",
    ],
    "clickstream": [
        "event_id",
        "customer_id",
        "event_timestamp",
        "event_type",
        "session_id",
    ],
    "returns": [
        "return_id",
        "order_id",
        "customer_id",
        "product_id",
        "return_date",
        "return_quantity",
        "refund_amount",
        "return_status",
    ],
}

if dataset_name not in schemas:
    raise ValueError(f"Unsupported Auto Loader dataset: {dataset_name}")


# COMMAND ----------

print(f"Environment: {environment}")
print(f"Domain: {domain}")
print(f"Dataset: {dataset_name}")
print(f"RAW path: {raw_path}")
print(f"Target table: {target_table}")
print(f"Quarantine table: {quarantine_table}")


# COMMAND ----------

ingest_to_bronze(
    spark=spark,
    source_path=raw_path,
    schema_path=schema_path,
    checkpoint_path=checkpoint_path,
    target_table=target_table,
    quarantine_table=quarantine_table,
    source_schema=schemas[dataset_name],
    required_columns=required_columns[dataset_name],
    environment=environment,
    batch_id=batch_id,
)

print(f"Bronze ingestion completed: {target_table}")
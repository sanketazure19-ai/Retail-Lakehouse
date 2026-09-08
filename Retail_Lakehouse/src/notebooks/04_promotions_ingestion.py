# Databricks notebook source

from retail_lakehouse.config.settings import (
    get_datasets,
    get_environment,
)
from retail_lakehouse.utils.helpers import build_raw_path
from retail_lakehouse.utils.promotions import ingest_promotions
from retail_lakehouse.utils.schemas import PROMOTION_SCHEMA

# COMMAND ----------

dbutils.widgets.text("environment", "dev")

environment = dbutils.widgets.get("environment")

# COMMAND ----------

env_config = get_environment(environment)
datasets = get_datasets()

catalog = env_config["catalog"]
raw_root = env_config["raw_root"]

dataset_config = datasets["marketing"]["promotions"]

raw_path = build_raw_path(
    raw_root,
    dataset_config["path"],
)

target_table = f"{catalog}.bronze.promotions"

# COMMAND ----------

print(f"Environment: {environment}")
print(f"RAW path: {raw_path}")
print(f"Target table: {target_table}")

# COMMAND ----------

ingest_promotions(
    spark=spark,
    source_path=raw_path,
    target_table=target_table,
    source_schema=PROMOTION_SCHEMA,
    environment=environment,
)

print(
    f"Promotions ingestion completed: {target_table}"
)
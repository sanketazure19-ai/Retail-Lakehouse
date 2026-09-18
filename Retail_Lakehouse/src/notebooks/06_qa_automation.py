# Databricks notebook source
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment").lower()
catalog = f"retail_{environment}"

results = []


def record(test_name, layer, status, actual=None, expected=None, details=None):
    results.append(
        {
            "test_name": test_name,
            "layer": layer,
            "status": status,
            "actual": actual,
            "expected": expected,
            "details": details,
        }
    )


def table_exists(full_name):
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False


def check_table(name, layer):
    full_name = f"{catalog}.{layer}.{name}"
    ok = table_exists(full_name)
    record(
        f"{layer}.{name} exists",
        layer,
        "PASS" if ok else "FAIL",
        actual=ok,
        expected=True,
    )
    return full_name if ok else None


def check_columns(full_name, required, layer):
    actual = set(spark.table(full_name).columns)
    missing = sorted(set(required) - actual)
    ok = not missing
    record(
        f"{full_name} required columns",
        layer,
        "PASS" if ok else "FAIL",
        actual=sorted(actual),
        expected=required,
        details=f"Missing: {missing}" if missing else None,
    )


def check_non_empty(full_name, layer):
    count = spark.table(full_name).count()
    ok = count > 0
    record(
        f"{full_name} non-empty",
        layer,
        "PASS" if ok else "FAIL",
        actual=count,
        expected="> 0",
    )


def check_unique(full_name, keys, layer):
    df = spark.table(full_name)
    duplicate_groups = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    record(
        f"{full_name} business-key uniqueness",
        layer,
        "PASS" if duplicate_groups == 0 else "FAIL",
        actual=duplicate_groups,
        expected=0,
        details=f"Keys: {keys}",
    )


# -------------------------
# Bronze technical QA
# -------------------------
bronze_specs = {
    "customers": ["customer_id", "email", "_source_file", "_ingestion_timestamp",
                  "_load_date", "_batch_id", "_environment", "_rescued_data", "_corrupt_record"],
    "products": ["product_id", "_source_file", "_ingestion_timestamp",
                 "_load_date", "_batch_id", "_environment", "_rescued_data", "_corrupt_record"],
    "orders": ["order_id", "_source_file", "_ingestion_timestamp",
               "_load_date", "_batch_id", "_environment", "_rescued_data", "_corrupt_record"],
    "clickstream": ["event_id", "_source_file", "_ingestion_timestamp",
                    "_load_date", "_batch_id", "_environment", "_rescued_data", "_corrupt_record"],
    "returns": ["return_id", "_source_file", "_ingestion_timestamp",
                "_load_date", "_batch_id", "_environment", "_rescued_data", "_corrupt_record"],
    "promotions": ["promotion_id", "_source_file", "_ingestion_timestamp",
                   "_load_date", "_batch_id", "_environment", "_corrupt_record"],
}

for dataset, columns in bronze_specs.items():
    full_name = check_table(dataset, "bronze")
    if full_name:
        check_columns(full_name, columns, "BRONZE")
        check_non_empty(full_name, "BRONZE")

        df = spark.table(full_name)
        rescued = df.filter(F.col("_rescued_data").isNotNull()).count() if "_rescued_data" in df.columns else 0
        corrupt = (
    df.filter(F.col("_corrupt_record").isNotNull()).count()
    if "_corrupt_record" in df.columns
    else 0
)
        record(
            f"{full_name} technical ingestion quality",
            "BRONZE",
            "PASS" if rescued == 0 and corrupt == 0 else "WARN",
            actual={"rescued": rescued, "corrupt": corrupt},
            expected={"rescued": 0, "corrupt": 0},
        )

# Promotions is a snapshot: current fixture is expected to contain 200 source rows.
promotions = f"{catalog}.bronze.promotions"
if table_exists(promotions):
    promotion_count = spark.table(promotions).count()
    record(
        "Bronze promotions snapshot row count",
        "BRONZE",
        "PASS" if promotion_count == 200 else "WARN",
        actual=promotion_count,
        expected=200,
        details="Warn rather than fail so a legitimate source snapshot change does not block QA.",
    )

# -------------------------
# Silver business QA
# -------------------------
silver_required = {
    "customers": ["customer_id", "email"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "clickstream": ["event_id"],
    "returns": ["return_id", "order_id", "customer_id", "product_id"],
    "promotions": ["promotion_id"],
}

for dataset, columns in silver_required.items():
    full_name = check_table(dataset, "silver")
    if full_name:
        check_columns(full_name, columns, "SILVER")
        check_non_empty(full_name, "SILVER")

for dataset, keys in {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "clickstream": ["event_id"],
    "returns": ["return_id"],
    "promotions": ["promotion_id"],
}.items():
    full_name = f"{catalog}.silver.{dataset}"
    if table_exists(full_name):
        check_unique(full_name, keys, "SILVER")

dq_table = f"{catalog}.monitoring.silver_dq_metrics"
if table_exists(dq_table):
    dq = spark.table(dq_table)
    latest = (
        dq.orderBy(F.col("recorded_at").desc())
        .limit(20)
        .collect()
    )
    for row in latest:
        status = row["dq_status"]
        record(
            f"Silver DQ latest status: {row['dataset']}",
            "SILVER",
            "PASS" if status in {"PASS", "ALERT"} else "FAIL",
            actual=status,
            expected="PASS or ALERT",
            details=f"failure_rate={row['dq_failure_rate_percent']:.2f}%, "
                    f"threshold={row['dq_failure_threshold_percent']:.2f}%",
        )

# -------------------------
# Gold QA
# -------------------------
gold_unique_specs = {
    "dim_customer": ["customer_id", "effective_start_date"],
    "dim_product": ["product_id"],
    "fact_orders": ["order_id"],
    "fact_returns": ["return_id"],
}

for dataset, keys in gold_unique_specs.items():
    full_name = check_table(dataset, "gold")
    if full_name:
        check_non_empty(full_name, "GOLD")
        check_unique(full_name, keys, "GOLD")

dim_customer = f"{catalog}.gold.dim_customer"
if table_exists(dim_customer):
    df = spark.table(dim_customer)
    current_count = df.filter(F.col("is_current") == True).count()
    invalid_current = df.filter(
        (F.col("is_current") == True) &
        (F.col("effective_end_date") != F.lit("9999-12-31").cast("date"))
    ).count()
    record(
        "Gold dim_customer SCD2 current-row integrity",
        "GOLD",
        "PASS" if invalid_current == 0 else "FAIL",
        actual={"current_rows": current_count, "invalid_current_rows": invalid_current},
        expected="Every current row has effective_end_date=9999-12-31",
    )

fact_orders = f"{catalog}.gold.fact_orders"
customer_summary = f"{catalog}.gold.customer_sales_summary"
if table_exists(fact_orders) and table_exists(customer_summary):
    fact_df = spark.table(fact_orders)
    summary_df = spark.table(customer_summary)
    candidate_fact = [c for c in ["net_amount", "net_sales", "revenue", "sales_amount"] if c in fact_df.columns]
    candidate_summary = [c for c in ["net_sales", "net_amount", "revenue", "sales_amount", "total_sales"] if c in summary_df.columns]
    if candidate_fact and candidate_summary:
        fact_total = fact_df.agg(F.sum(candidate_fact[0])).first()[0] or 0.0
        summary_total = summary_df.agg(F.sum(candidate_summary[0])).first()[0] or 0.0
        delta = abs(float(fact_total) - float(summary_total))
        record(
            "Gold customer summary reconciles to fact_orders",
            "GOLD",
            "PASS" if delta < 0.01 else "FAIL",
            actual={"fact_column": candidate_fact[0], "summary_column": candidate_summary[0],
                    "fact_total": float(fact_total), "summary_total": float(summary_total), "delta": delta},
            expected="delta < 0.01",
        )
    else:
        record(
            "Gold customer summary reconciliation",
            "GOLD",
            "WARN",
            actual={"fact_columns": fact_df.columns, "summary_columns": summary_df.columns},
            expected="Recognized sales measure columns",
            details="Skipped because the existing summary schema does not expose a recognized measure column.",
        )

# -------------------------
# Persist results + exit
# -------------------------
failed = sum(1 for r in results if r["status"] == "FAIL")
warnings = sum(1 for r in results if r["status"] == "WARN")
passed = sum(1 for r in results if r["status"] == "PASS")

qa_summary = {
    "environment": environment,
    "catalog": catalog,
    "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "summary": {"passed": passed, "failed": failed, "warnings": warnings},
    "tests": results,
}

result_rows = [
    (
        environment,
        r["layer"],
        r["test_name"],
        r["status"],
        json.dumps(r["actual"], default=str),
        json.dumps(r["expected"], default=str),
        r.get("details"),
        datetime.now(timezone.utc),
    )
    for r in results
]

result_schema = """
environment string,
layer string,
test_name string,
status string,
actual string,
expected string,
details string,
recorded_at timestamp
"""

target = f"{catalog}.monitoring.qa_test_results"
spark.createDataFrame(result_rows, result_schema).write.format("delta").mode("append").saveAsTable(target)

print(json.dumps(qa_summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(qa_summary, default=str))
